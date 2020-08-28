/**
  * Conversions from WDL types and data structures to DNAx JSON
  * representations. There are two difficulties this module needs to deal
  * with: (1) WDL has high order types which DNAx does not, and (2) the
  * file type is very different between WDL and DNAx.
  */
package dx.core.languages.wdl

import dx.api.{DxApi, DxFile}
import dx.core.io.DxFileDescCache
import dx.core.ir.Type._
import dx.core.ir.Type
import spray.json._
import wdlTools.eval.WdlValues.V_Map
import wdlTools.eval.{EvalException, JsonSerde, WdlValues}
import wdlTools.types.WdlTypes
import wdlTools.util.FileSourceResolver

case class ParameterLinkSerde(dxApi: DxApi = DxApi.get,
                              fileResolver: FileSourceResolver = FileSourceResolver.get,
                              dxFileDescCache: DxFileDescCache,
                              typeAliases: Map[String, Type]) {

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
