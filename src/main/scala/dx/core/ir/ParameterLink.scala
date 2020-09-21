package dx.core.ir

import dx.AppInternalException
import dx.api.{DxApi, DxExecution, DxFile, DxUtils, DxWorkflowStage}
import dx.core.Native
import dx.core.io.{DxFileDescCache, DxFileSource}
import dx.core.ir.Type.TFile
import dx.core.ir.Value._
import spray.json._
import wdlTools.util.{Enum, FileSourceResolver, LocalFileSource, Logger}

object IORef extends Enum {
  type IORef = Value
  val Input, Output = Value
}

/**
  * A union of all the different ways of building a value from JSON passed
  * by the platform. A complex value is a WDL values that does not map to
  * a native dx:type. Such values may also have files embedded in them.
  * For example:
  * - Ragged file array:  Array\[Array\[File\]\]
  * - Object with file elements
  * - Map of files:     Map[String, File]
  * A complex value is implemented as a json structure, and an array of
  * all the files it references.
  */
sealed trait ParameterLink {
  val dxType: Type

  /**
    * Copy this ParameterLink, replacing dxType with its optional equivalent.
    * @return
    */
  def makeOptional: ParameterLink
}
case class ParameterLinkValue(jsn: JsValue, dxType: Type) extends ParameterLink {
  def makeOptional: ParameterLinkValue = {
    copy(dxType = Type.ensureOptional(dxType))
  }
}

case class ParameterLinkStage(dxStage: DxWorkflowStage,
                              ioRef: IORef.Value,
                              varName: String,
                              dxType: Type)
    extends ParameterLink {
  def makeOptional: ParameterLinkStage = {
    copy(dxType = Type.ensureOptional(dxType))
  }
}

case class ParameterLinkWorkflowInput(varName: String, dxType: Type) extends ParameterLink {
  def makeOptional: ParameterLinkWorkflowInput = {
    copy(dxType = Type.ensureOptional(dxType))
  }
}

case class ParameterLinkExec(dxExecution: DxExecution, varName: String, dxType: Type)
    extends ParameterLink {
  def makeOptional: ParameterLinkExec = {
    copy(dxType = Type.ensureOptional(dxType))
  }
}

object ParameterLink {
  // Key used to wrap a complex value in JSON.

  val FlatFilesSuffix = "___dxfiles"
  val WorkflowInputFieldKey = "workflowInputField"
}

case class ParameterLinkSerializer(fileResolver: FileSourceResolver = FileSourceResolver.get,
                                   dxApi: DxApi = DxApi.get) {

  /**
    * Serialize a complex value into a JSON value. The value could potentially point
    * to many files. The assumption is that files are already in the format of dxWDLs,
    * so not requiring upload/download or any special conversion.
    * @param t the type
    * @param v the value
    * @return
    */
  private def serialize(t: Type, v: Value): JsValue = {
    if (Type.isNestedOptional(t)) {
      Logger.error(s"""|jsFromWdlValue
                       |    type=${t}
                       |    val=${v}
                       |""".stripMargin)
      throw new Exception("a nested optional type/value")
    }
    def handler(value: Value): Option[JsValue] = {
      value match {
        case VString(s) if s.length > Native.StringLengthLimit =>
          throw new AppInternalException(
              s"string is longer than ${Native.StringLengthLimit}"
          )
        case VFile(path) =>
          fileResolver.resolve(path) match {
            case dxFile: DxFileSource       => Some(dxFile.dxFile.asJson)
            case localFile: LocalFileSource => Some(JsString(localFile.toString))
            case other =>
              throw new RuntimeException(s"Unsupported file source ${other}")
          }
        case _ => None
      }
    }
    ValueSerde.serialize(v, Some(handler))
  }

  /**
    * Create a link from a WDL value.
    *
    * @param t the WDL type
    * @param v the WDL value
    * @return
    */
  def createLink(t: Type, v: Value): ParameterLink = {
    ParameterLinkValue(serialize(t, v), t)
  }

  def createConstantField(value: Value,
                          bindName: String,
                          encodeDots: Boolean = true): (String, JsValue) = {
    val bindEncName =
      if (encodeDots) {
        Parameter.encodeDots(bindName)
      } else {
        bindName
      }
    (bindEncName, ValueSerde.serialize(value))
  }

  // create input/output fields that bind the variable name [bindName] to
  // this WdlVar
  def createFieldsFromLink(link: ParameterLink,
                           bindName: String,
                           encodeDots: Boolean = true): Vector[(String, JsValue)] = {
    val encodedName =
      if (encodeDots) {
        Parameter.encodeDots(bindName)
      } else {
        bindName
      }
    val wdlType = Type.unwrapOptional(link.dxType)
    if (Type.isDxType(wdlType)) {
      // Types that are supported natively in DX
      val jsv: JsValue = link match {
        case ParameterLinkValue(jsLinkvalue, _) => jsLinkvalue
        case ParameterLinkStage(dxStage, ioRef, varEncName, _) =>
          ioRef match {
            case IORef.Input =>
              dxStage.getInputReference(Parameter.encodeDots(varEncName))
            case IORef.Output =>
              dxStage.getOutputReference(Parameter.encodeDots(varEncName))
          }
        case ParameterLinkWorkflowInput(varEncName, _) =>
          JsObject(
              DxUtils.DxLinkKey -> JsObject(
                  ParameterLink.WorkflowInputFieldKey -> JsString(
                      Parameter.encodeDots(varEncName)
                  )
              )
          )
        case ParameterLinkExec(dxJob, varEncName, _) =>
          DxUtils.dxExecutionToEbor(dxJob, Parameter.encodeDots(varEncName))
      }
      Vector((encodedName, jsv))
    } else {
      // Complex type requiring two fields: a JSON structure, and a flat array of files.
      val fileArrayName = s"${encodedName}${ParameterLink.FlatFilesSuffix}"
      val mapValue = link match {
        case ParameterLinkValue(jsLinkvalue, _) =>
          // files that are embedded in the structure
          val jsFiles = dxApi.findFiles(jsLinkvalue).map(_.asJson)
          // Dx allows hashes as an input/output type. If the JSON value is
          // not a hash (JsObject), we need to add an outer layer to it.
          val jsLink = JsObject(Parameter.ComplexValueKey -> jsLinkvalue)
          Map(encodedName -> jsLink, fileArrayName -> JsArray(jsFiles))
        case ParameterLinkStage(dxStage, ioRef, varName, _) =>
          val varFileArrayName = s"${varName}${ParameterLink.FlatFilesSuffix}"
          ioRef match {
            case IORef.Input =>
              Map(
                  encodedName -> dxStage.getInputReference(varName),
                  fileArrayName -> dxStage.getInputReference(varFileArrayName)
              )
            case IORef.Output =>
              Map(
                  encodedName -> dxStage.getOutputReference(varName),
                  fileArrayName -> dxStage.getOutputReference(varFileArrayName)
              )
          }
        case ParameterLinkWorkflowInput(varName, _) =>
          val varFileArrayName = s"${varName}${ParameterLink.FlatFilesSuffix}"
          Map(
              encodedName ->
                JsObject(
                    DxUtils.DxLinkKey -> JsObject(
                        ParameterLink.WorkflowInputFieldKey -> JsString(varName)
                    )
                ),
              fileArrayName ->
                JsObject(
                    DxUtils.DxLinkKey -> JsObject(
                        ParameterLink.WorkflowInputFieldKey -> JsString(varFileArrayName)
                    )
                )
          )
        case ParameterLinkExec(dxJob, varName, _) =>
          val varFileArrayName = s"${varName}${ParameterLink.FlatFilesSuffix}"
          Map(
              encodedName -> DxUtils
                .dxExecutionToEbor(dxJob, Parameter.encodeDots(varName)),
              fileArrayName -> DxUtils
                .dxExecutionToEbor(dxJob, Parameter.encodeDots(varFileArrayName))
          )
      }
      mapValue.toVector
    }
  }

  def createFields(bindName: String,
                   t: Type,
                   v: Value,
                   encodeDots: Boolean = true): Vector[(String, JsValue)] = {
    createFieldsFromLink(createLink(t, v), bindName, encodeDots)
  }

  def createFieldsFromMap(values: Map[String, (Type, Value)],
                          encodeDots: Boolean = true): Map[String, JsValue] = {
    values.flatMap {
      case (name, (t, v)) => createFields(name, t, v, encodeDots)
    }
  }
}

case class ParameterLinkDeserializer(dxFileDescCache: DxFileDescCache, dxApi: DxApi = DxApi.get) {
  private def unpack(jsv: JsValue): JsValue = {
    jsv match {
      case JsObject(fields) if fields.contains(Parameter.ComplexValueKey) =>
        // unpack the hash with which complex JSON values are wrapped in dnanexus.
        fields(Parameter.ComplexValueKey)
      case _ => jsv
    }
  }

  def deserializeInput(jsv: JsValue): Value = {
    def handler(value: JsValue): Option[Value] = {
      if (DxFile.isLinkJson(value)) {
        // Convert the path in DNAx to a string. We can later decide if we want to download it or not.
        // Use the cache value if there is one to save the API call.
        val dxFile = dxFileDescCache.updateFileFromCache(DxFile.fromJson(dxApi, value))
        Some(VFile(dxFile.asUri))
      } else {
        None
      }
    }
    ValueSerde.deserialize(unpack(jsv), Some(handler))
  }

  def deserializeInputMap(inputs: Map[String, JsValue]): Map[String, Value] = {
    inputs.map {
      case (name, jsv) => name -> deserializeInput(jsv)
    }
  }

  def deserializeInputWithType(
      jsv: JsValue,
      t: Type,
      translator: Option[(JsValue, Type) => JsValue] = None
  ): Value = {
    def parameterLinkTranslator(jsv: JsValue, t: Type): JsValue = {
      val updatedValue = translator.map(_(jsv, t)).getOrElse(jsv)
      (t, updatedValue) match {
        case (TFile, _) if DxFile.isLinkJson(jsv) =>
          // Convert the path in DNAx to a string. We can later decide if we want to download it or not.
          // Use the cache value if there is one to save the API call.
          val dxFile = dxFileDescCache.updateFileFromCache(DxFile.fromJson(dxApi, jsv))
          JsString(dxFile.asUri)
        case _ =>
          updatedValue
      }
    }
    ValueSerde.deserializeWithType(unpack(jsv), t, Some(parameterLinkTranslator))
  }
}
