/**
  * Conversions from WDL types and data structures to DNAx JSON
  * representations. There are two difficulties this module needs to deal
  * with: (1) WDL has high order types which DNAx does not, and (2) the
  * file type is very different between WDL and DNAx.
  */
package dx.core.languages.wdl

import dx.AppInternalException
import dx.api.{DxApi, DxExecution, DxFile, DxUtils, DxWorkflowStage}
import dx.core.io.{DxFileDescCache, DxFileSource}
import dx.core.languages.IORef
import spray.json._
import wdlTools.eval.WdlValues.V_Map
import wdlTools.eval.{Coercion, EvalException, JsonSerde, WdlValues}
import wdlTools.types.WdlTypes
import wdlTools.util.{FileSourceResolver, LocalFileSource, Logger}

// A union of all the different ways of building a value
// from JSON passed by the platform.
//
// A complex values is a WDL values that does not map to a native dx:type. Such
// values may also have files embedded in them. For example:
//  - Ragged file array:  Array[Array[File]]
//  - Object with file elements
//  - Map of files:     Map[String, File]
// A complex value is implemented as a json structure, and an array of
// all the files it references.
sealed trait DxLink
case class DxLinkValue(jsn: JsValue) extends DxLink // This may contain dx-files
case class DxLinkStage(dxStage: DxWorkflowStage, ioRef: IORef.Value, varName: String) extends DxLink
case class DxLinkWorkflowInput(varName: String) extends DxLink
case class DxLinkExec(dxExec: DxExecution, varName: String) extends DxLink

case class WdlVarLinks(wdlType: WdlTypes.T, dxlink: DxLink)

case class WdlVarLinksConverter(dxApi: DxApi,
                                fileResolver: FileSourceResolver,
                                dxFileDescCache: DxFileDescCache,
                                typeAliases: Map[String, WdlTypes.T]) {

  private val MaxStringLength: Int = 32 * 1024 // Long strings cause problems with bash and the UI

  // Serialize a complex WDL value into a JSON value. The value could potentially point
  // to many files. The assumption is that files are already in the format of dxWDLs,
  // requiring not upload/download or any special conversion.
  private def jsFromWdlValue(wdlType: WdlTypes.T, wdlValue: WdlValues.V): JsValue = {
    def handler(wdlValue: WdlValues.V): Option[JsValue] = {
      wdlValue match {
        case WdlValues.V_Optional(WdlValues.V_Optional(_)) =>
          Logger.error(s"""|jsFromWdlValue
                           |    type=${wdlType}
                           |    val=${wdlValue}
                           |""".stripMargin)
          throw new Exception("a nested optional type/value")
        case WdlValues.V_String(s) if s.length > MaxStringLength =>
          throw new AppInternalException(s"string is longer than ${MaxStringLength}")
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
    val coerced = Coercion.coerceTo(wdlType, wdlValue, allowNonstandardCoercions = true)
    JsonSerde.serialize(coerced, Some(handler))
  }

  // import a WDL value
  def importFromWDL(wdlType: WdlTypes.T, wdlValue: WdlValues.V): WdlVarLinks = {
    val jsValue = jsFromWdlValue(wdlType, wdlValue)
    WdlVarLinks(wdlType, DxLinkValue(jsValue))
  }

  // Convert a job input to a WdlValues.V. Do not download any files, convert them
  // to a string representation. For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
  //
  def jobInputToWdlValue(name: String, wdlType: WdlTypes.T, jsValue: JsValue): WdlValues.V = {
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

  def unpackJobInput(name: String, wdlType: WdlTypes.T, jsv: JsValue): WdlValues.V = {
    val jsv1 =
      jsv match {
        case JsObject(fields) if fields contains "___" =>
          // unpack the hash with which complex JSON values are
          // wrapped in dnanexus.
          fields("___")
        case _ => jsv
      }
    jobInputToWdlValue(name, wdlType, jsv1)
  }

  // Is this a WDL type that maps to a native DX type?
  def isNativeDxType(wdlType: WdlTypes.T): Boolean = {
    wdlType match {
      // optional dx:native types
      case WdlTypes.T_Optional(WdlTypes.T_Boolean)     => true
      case WdlTypes.T_Optional(WdlTypes.T_Int)         => true
      case WdlTypes.T_Optional(WdlTypes.T_Float)       => true
      case WdlTypes.T_Optional(WdlTypes.T_String)      => true
      case WdlTypes.T_Optional(WdlTypes.T_File)        => true
      case WdlTypes.T_Array(WdlTypes.T_Boolean, false) => true
      case WdlTypes.T_Array(WdlTypes.T_Int, false)     => true
      case WdlTypes.T_Array(WdlTypes.T_Float, false)   => true
      case WdlTypes.T_Array(WdlTypes.T_String, false)  => true
      case WdlTypes.T_Array(WdlTypes.T_File, false)    => true

      // compulsory dx:native types
      case WdlTypes.T_Boolean                         => true
      case WdlTypes.T_Int                             => true
      case WdlTypes.T_Float                           => true
      case WdlTypes.T_String                          => true
      case WdlTypes.T_File                            => true
      case WdlTypes.T_Array(WdlTypes.T_Boolean, true) => true
      case WdlTypes.T_Array(WdlTypes.T_Int, true)     => true
      case WdlTypes.T_Array(WdlTypes.T_Float, true)   => true
      case WdlTypes.T_Array(WdlTypes.T_String, true)  => true
      case WdlTypes.T_Array(WdlTypes.T_File, true)    => true

      // A tricky, but important case, is `Array[File]+?`. This
      // cannot be converted into a dx file array, unfortunately.
      case _ => false
    }
  }

  // Dots are illegal in applet variable names.
  private def encodeAppletVarName(varName: String): String = {
    if (varName contains ".") {
      throw new Exception(s"Variable ${varName} includes the illegal symbol \\.")
    }
    varName
  }

  // We need to deal with types like:
  //     Int??, Array[File]??
  @scala.annotation.tailrec
  private def stripOptional(t: WdlTypes.T): WdlTypes.T = {
    t match {
      case WdlTypes.T_Optional(x) => stripOptional(x)
      case x                      => x
    }
  }

  private def nodots(s: String): String = {
    encodeAppletVarName(WdlVarLinksConverter.transformVarName(s))
  }

  def genConstantField(wdlValue: WdlValues.V,
                       bindName: String,
                       encodeDots: Boolean = true): (String, JsValue) = {
    val bindEncName =
      if (encodeDots) {
        nodots(bindName)
      } else {
        bindName
      }
    (bindEncName, JsonSerde.serialize(wdlValue))
  }

  // create input/output fields that bind the variable name [bindName] to
  // this WdlVar
  def genFields(wvl: WdlVarLinks,
                bindName: String,
                encodeDots: Boolean = true): Vector[(String, JsValue)] = {

    val bindEncName =
      if (encodeDots) {
        nodots(bindName)
      } else {
        bindName
      }
    def mkSimple(): (String, JsValue) = {
      val jsv: JsValue = wvl.dxlink match {
        case DxLinkValue(jsn) => jsn
        case DxLinkStage(dxStage, ioRef, varEncName) =>
          ioRef match {
            case IORef.Input  => dxStage.getInputReference(nodots(varEncName))
            case IORef.Output => dxStage.getOutputReference(nodots(varEncName))
          }
        case DxLinkWorkflowInput(varEncName) =>
          JsObject(
              "$dnanexus_link" -> JsObject("workflowInputField" -> JsString(nodots(varEncName)))
          )
        case DxLinkExec(dxJob, varEncName) =>
          DxUtils.dxExecutionToEbor(dxJob, nodots(varEncName))
      }
      (bindEncName, jsv)
    }
    def mkComplex: Map[String, JsValue] = {
      val bindEncName_F = bindEncName + WdlVarLinksConverter.FLAT_FILES_SUFFIX
      wvl.dxlink match {
        case DxLinkValue(jsn) =>
          // files that are embedded in the structure
          val dxFiles = dxApi.findFiles(jsn)
          val jsFiles = dxFiles.map(_.getLinkAsJson)
          // Dx allows hashes as an input/output type. If the JSON value is
          // not a hash (js-object), we need to add an outer layer to it.
          val jsn1 = JsObject("___" -> jsn)
          Map(bindEncName -> jsn1, bindEncName_F -> JsArray(jsFiles))
        case DxLinkStage(dxStage, ioRef, varEncName) =>
          val varEncName_F = varEncName + WdlVarLinksConverter.FLAT_FILES_SUFFIX
          ioRef match {
            case IORef.Input =>
              Map(
                  bindEncName -> dxStage.getInputReference(varEncName),
                  bindEncName_F -> dxStage.getInputReference(varEncName_F)
              )
            case IORef.Output =>
              Map(
                  bindEncName -> dxStage.getOutputReference(varEncName),
                  bindEncName_F -> dxStage.getOutputReference(varEncName_F)
              )
          }
        case DxLinkWorkflowInput(varEncName) =>
          val varEncName_F = varEncName + WdlVarLinksConverter.FLAT_FILES_SUFFIX
          Map(
              bindEncName ->
                JsObject(
                    "$dnanexus_link" -> JsObject("workflowInputField" -> JsString(varEncName))
                ),
              bindEncName_F ->
                JsObject(
                    "$dnanexus_link" -> JsObject("workflowInputField" -> JsString(varEncName_F))
                )
          )
        case DxLinkExec(dxJob, varEncName) =>
          val varEncName_F = varEncName + WdlVarLinksConverter.FLAT_FILES_SUFFIX
          Map(bindEncName -> DxUtils.dxExecutionToEbor(dxJob, nodots(varEncName)),
              bindEncName_F -> DxUtils.dxExecutionToEbor(dxJob, nodots(varEncName_F)))
      }
    }

    val wdlType = stripOptional(wvl.wdlType)
    if (isNativeDxType(wdlType)) {
      // Types that are supported natively in DX
      Vector(mkSimple())
    } else {
      // General complex type requiring two fields: a JSON
      // structure, and a flat array of files.
      mkComplex.toVector
    }
  }
}

object WdlVarLinksConverter {
  // dx does not allow dots in variable names, so we
  // convert them to underscores.
  def transformVarName(varName: String): String = {
    varName.replaceAll("\\.", "___")
  }

  val FLAT_FILES_SUFFIX = "___dxfiles"
}
