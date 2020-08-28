package dx.core.ir

import dx.AppInternalException
import dx.api.{DxApi, DxExecution, DxUtils, DxWorkflowStage}
import dx.core.io.DxFileSource
import dx.core.ir.Value._
import dx.core.languages.IORef
import dx.core.languages.wdl.ParameterLinkSerde
import spray.json._
import wdlTools.util.{FileSourceResolver, LocalFileSource, Logger}

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
}
case class ParameterLinkValue(jsn: JsValue, dxType: Type) extends ParameterLink
case class ParameterLinkStage(dxStage: DxWorkflowStage,
                              ioRef: IORef.Value,
                              varName: String,
                              dxType: Type)
    extends ParameterLink
case class ParameterLinkWorkflowInput(varName: String, dxType: Type) extends ParameterLink
case class ParameterLinkExec(dxExecution: DxExecution, varName: String, dxType: Type)
    extends ParameterLink

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
        case VString(s) if s.length > ParameterLinkSerde.MaxStringLength =>
          throw new AppInternalException(
              s"string is longer than ${ParameterLinkSerde.MaxStringLength}"
          )
        case VFile(path) =>
          fileResolver.resolve(path) match {
            case dxFile: DxFileSource       => Some(dxFile.dxFile.getLinkAsJson)
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
  def createFields(link: ParameterLink,
                   bindName: String,
                   encodeDots: Boolean = true): Vector[(String, JsValue)] = {

    val encodedName =
      if (encodeDots) {
        Parameter.encodeDots(bindName)
      } else {
        bindName
      }
    val wdlType = Type.unwrapOptional(link.dxType)
    if (ParameterLinkSerde.isNativeDxType(wdlType)) {
      // Types that are supported natively in DX
      val jsv: JsValue = link match {
        case ParameterLinkValue(jsn, _) => jsn
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
                  ParameterLinkSerde.WorkflowInputFieldKey -> JsString(
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
      val fileArrayName = s"${encodedName}${ParameterLinkSerde.FlatFilesSuffix}"
      val mapValue = link match {
        case ParameterLinkValue(jsn, _) =>
          // files that are embedded in the structure
          val jsFiles = dxApi.findFiles(jsn).map(_.getLinkAsJson)
          // Dx allows hashes as an input/output type. If the JSON value is
          // not a hash (JsObject), we need to add an outer layer to it.
          val jsn1 = JsObject(ParameterLinkSerde.ComplexValueKey -> jsn)
          Map(encodedName -> jsn1, fileArrayName -> JsArray(jsFiles))
        case ParameterLinkStage(dxStage, ioRef, varName, _) =>
          val varFileArrayName = s"${varName}${ParameterLinkSerde.FlatFilesSuffix}"
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
          val varFileArrayName = s"${varName}${ParameterLinkSerde.FlatFilesSuffix}"
          Map(
              encodedName ->
                JsObject(
                    DxUtils.DxLinkKey -> JsObject(
                        ParameterLinkSerde.WorkflowInputFieldKey -> JsString(varName)
                    )
                ),
              fileArrayName ->
                JsObject(
                    DxUtils.DxLinkKey -> JsObject(
                        ParameterLinkSerde.WorkflowInputFieldKey -> JsString(varFileArrayName)
                    )
                )
          )
        case ParameterLinkExec(dxJob, varName, _) =>
          val varFileArrayName = s"${varName}${ParameterLinkSerde.FlatFilesSuffix}"
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
}
