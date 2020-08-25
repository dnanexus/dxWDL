/**
  * Conversions from WDL types and data structures to DNAx JSON
  * representations. There are two difficulties this module needs to deal
  * with: (1) WDL has high order types which DNAx does not, and (2) the
  * file type is very different between WDL and DNAx.
  */
package dx.core.languages.wdl

import dx.AppInternalException
import dx.api.{DxApi, DxFile, DxUtils}
import dx.core.io.{DxFileDescCache, DxFileSource}
import dx.core.ir.Type._
import dx.core.ir.{
  ParameterLink,
  ParameterLinkExec,
  ParameterLinkStage,
  ParameterLinkValue,
  ParameterLinkWorkflowInput,
  Type,
  Value
}
import dx.core.languages.IORef
import spray.json._
import wdlTools.eval.WdlValues.V_Map
import wdlTools.eval.{EvalException, JsonSerde, WdlValues}
import wdlTools.types.WdlTypes
import wdlTools.util.{FileSourceResolver, LocalFileSource, Logger}

case class ParameterLinkSerde(dxApi: DxApi,
                              fileResolver: FileSourceResolver,
                              dxFileDescCache: DxFileDescCache,
                              typeAliases: Map[String, Type]) {

  // Serialize a complex value into a JSON value. The value could potentially point
  // to many files. The assumption is that files are already in the format of dxWDLs,
  // so not requiring upload/download or any special conversion.
  private def serialize(t: Type, v: Value): JsValue = {
    def handler(wdlValue: WdlValues.V): Option[JsValue] = {
      wdlValue match {
        case WdlValues.V_Optional(WdlValues.V_Optional(_)) =>
          Logger.error(s"""|jsFromWdlValue
                           |    type=${t}
                           |    val=${wdlValue}
                           |""".stripMargin)
          throw new Exception("a nested optional type/value")
        case WdlValues.V_String(s) if s.length > ParameterLinkSerde.MaxStringLength =>
          throw new AppInternalException(
              s"string is longer than ${ParameterLinkSerde.MaxStringLength}"
          )
        case WdlValues.V_File(path) =>
          fileResolver.resolve(path) match {
            case dxFile: DxFileSource       => Some(dxFile.dxFile.getLinkAsJson)
            case localFile: LocalFileSource => Some(JsString(localFile.toString))
            case other =>
              throw new RuntimeException(s"Unsupported file source ${other}")
          }
        // Represent a Map in JSON as an array of keys, followed by an array of values.
        case WdlValues.V_Map(m) =>
          val keys = m.keys.map(key => JsonSerde.serialize(key, Some(handler)))
          val values = m.values.map(value => JsonSerde.serialize(value, Some(handler)))
          Some(
              JsObject("keys" -> JsArray(keys.toVector),
                       "values" ->
                         JsArray(values.toVector))
          )
        case _ => None
      }
    }
    JsonSerde.serialize(v, Some(handler))
  }

  /**
    * Create a link from a WDL value.
    * @param t the WDL type
    * @param v the WDL value
    * @return
    */
  def createLink(t: Type, v: Value): ParameterLink = {
    ParameterLinkValue(serialize(t, v), t)
  }

  /**
    * Converts a job input to a WDL value. Converts files to string representation
    * (but does not download them).
    * For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
    * @param name input name
    * @param wdlType input type
    * @param jsValue input value
    * @return
    */
  def deserializeJobInput(name: String, wdlType: WdlTypes.T, jsValue: JsValue): WdlValues.V = {
    def handler(jsValue: JsValue, wdlType: WdlTypes.T, name: String): Option[WdlValues.V] = {
      (wdlType, jsValue) match {
        // special handling for maps serialized as a pair of arrays
        case (WdlTypes.T_File, obj: JsObject) =>
          // Convert the path in DNAx to a string. We can later decide if we want to download it or not.
          // Use the cache value if there is one to save the API call.
          val dxFile = dxFileDescCache.updateFileFromCache(DxFile.fromJsValue(dxApi, obj))
          Some(WdlValues.V_File(dxFile.asUri))
        case (WdlTypes.T_Map(keyType, valueType), JsObject(fields))
            if fields.keySet == Set("keys", "values") =>
          try {
            (fields("keys"), fields("values")) match {
              case (JsArray(keys), JsArray(values)) if keys.length == values.length =>
                val coerced = keys
                  .zip(values)
                  .map {
                    case (k, v) =>
                      val keyCoerced =
                        JsonSerde.deserialize(k, keyType, s"${name}.${k}", Some(handler))
                      val valueCoerced =
                        JsonSerde.deserialize(v, valueType, s"${name}.${k}", Some(handler))
                      keyCoerced -> valueCoerced
                  }
                  .toMap
                Some(V_Map(coerced))
              case _ =>
                // return None rather than throw an exception since this might coincidentally be
                // a regular map with "keys" and "values" keys.
                None
            }
          } catch {
            case _: EvalException =>
              None
          }
      }
    }
    JsonSerde.deserialize(jsValue, wdlType, name, Some(handler))
  }

  /**
    * Deserialize a job input that may be an encoded complex value.
    * @param name input name
    * @param wdlType input type
    * @param jsv input value
    * @return
    */
  def unpackJobInput(name: String, wdlType: WdlTypes.T, jsv: JsValue): WdlValues.V = {
    val unpacked =
      jsv match {
        case JsObject(fields) if fields.contains(ParameterLinkSerde.ComplexValueKey) =>
          // unpack the hash with which complex JSON values are
          // wrapped in dnanexus.
          fields(ParameterLinkSerde.ComplexValueKey)
        case _ => jsv
      }
    deserializeJobInput(name, wdlType, unpacked)
  }
  // create input/output fields that bind the variable name [bindName] to
  // this WdlVar
  def createFields(link: ParameterLink,
                   bindName: String,
                   encodeDots: Boolean = true): Vector[(String, JsValue)] = {

    val encodedName =
      if (encodeDots) {
        ParameterLinkSerde.encodeDots(bindName)
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
              dxStage.getInputReference(ParameterLinkSerde.encodeDots(varEncName))
            case IORef.Output =>
              dxStage.getOutputReference(ParameterLinkSerde.encodeDots(varEncName))
          }
        case ParameterLinkWorkflowInput(varEncName, _) =>
          JsObject(
              DxUtils.DxLinkKey -> JsObject(
                  ParameterLinkSerde.WorkflowInputFieldKey -> JsString(
                      ParameterLinkSerde.encodeDots(varEncName)
                  )
              )
          )
        case ParameterLinkExec(dxJob, varEncName, _) =>
          DxUtils.dxExecutionToEbor(dxJob, ParameterLinkSerde.encodeDots(varEncName))
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
                .dxExecutionToEbor(dxJob, ParameterLinkSerde.encodeDots(varName)),
              fileArrayName -> DxUtils
                .dxExecutionToEbor(dxJob, ParameterLinkSerde.encodeDots(varFileArrayName))
          )
      }
      mapValue.toVector
    }
  }
}

object ParameterLinkSerde {

  /**
    * Key used to wrap a complex value in JSON.
    */
  val ComplexValueKey = "___"

  /*
   */
  val FlatFilesSuffix = "___dxfiles"

  /**
    * Very long strings cause problems with bash and the UI, so we set
    * a max limit of 32k characters
    */
  val MaxStringLength: Int = 32 * 1024
  val WorkflowInputFieldKey = "workflowInputField"

  // dx does not allow dots in variable names, so we
  // convert them to underscores.
  def encodeDots(varName: String): String = {
    varName.replaceAll("\\.", ComplexValueKey)
  }

  def decodeDots(varName: String): String = {
    varName.replaceAll(ComplexValueKey, "\\.")
  }

  def createConstantField(wdlValue: WdlValues.V,
                          bindName: String,
                          encodeDots: Boolean = true): (String, JsValue) = {
    val bindEncName =
      if (encodeDots) {
        ParameterLinkSerde.encodeDots(bindName)
      } else {
        bindName
      }
    (bindEncName, JsonSerde.serialize(wdlValue))
  }

  // Is this a WDL type that maps to a primitive, non-optional native DX type?
  def isNativeDxPrimitiveType(t: Type): Boolean = {
    t match {
      case TBoolean   => true
      case TInt       => true
      case TFloat     => true
      case TString    => true
      case TFile      => true
      case TDirectory => true // ?
      case THash      => true // ?
      case _          => false
    }
  }

  // Is this a WDL type that maps to a native DX type?
  def isNativeDxType(t: Type): Boolean = {
    t match {
      case _ if isNativeDxPrimitiveType(t) => true
      case TArray(inner, _)                => isNativeDxPrimitiveType(inner)
      case TOptional(inner)                => isNativeDxPrimitiveType(inner)
      case TOptional(TArray(inner, _))     => isNativeDxPrimitiveType(inner)
      case _                               => false
    }
  }
}
