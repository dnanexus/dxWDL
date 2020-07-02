/**

Conversions from WDL types and data structures to DNAx JSON
representations. There are two difficulties this module needs to deal
with: (1) WDL has high order types which DNAx does not, and (2) the
file type is very different between WDL and DNAx.

  */
package dx.core.languages.wdl

import dx.AppInternalException
import dx.api.{DxApi, DxExecution, DxFile, DxUtils, DxWorkflowStage}
import dx.core.io.DxFileSource
import dx.core.languages.IORef
import spray.json._
import wdlTools.eval.WdlValues
import wdlTools.types.WdlTypes
import wdlTools.util.{FileSourceResolver, LocalFileSource}

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
case class DxlValue(jsn: JsValue) extends DxLink // This may contain dx-files
case class DxlStage(dxStage: DxWorkflowStage, ioRef: IORef.Value, varName: String) extends DxLink
case class DxlWorkflowInput(varName: String) extends DxLink
case class DxlExec(dxExec: DxExecution, varName: String) extends DxLink

case class WdlVarLinks(wdlType: WdlTypes.T, dxlink: DxLink)

case class WdlVarLinksConverter(dxApi: DxApi,
                                fileResolver: FileSourceResolver,
                                dxFileCache: Map[String, DxFile],
                                typeAliases: Map[String, WdlTypes.T]) {

  private val MAX_STRING_LEN: Int = 32 * 1024 // Long strings cause problems with bash and the UI

  private def isNestedOptional(t: WdlTypes.T, v: WdlValues.V): Boolean = {
    t match {
      case WdlTypes.T_Optional(WdlTypes.T_Optional(_)) => return true
      case _                                           => ()
    }
    v match {
      case WdlValues.V_Optional(WdlValues.V_Optional(_)) => return true
      case _                                             => ()
    }
    false
  }

  // Serialize a complex WDL value into a JSON value. The value could potentially point
  // to many files. The assumption is that files are already in the format of dxWDLs,
  // requiring not upload/download or any special conversion.
  private def jsFromWdlValue(wdlType: WdlTypes.T, wdlValue: WdlValues.V): JsValue = {
    if (isNestedOptional(wdlType, wdlValue)) {
      System.err.println(s"""|jsFromWdlValue
                             |    type=${wdlType}
                             |    val=${wdlValue}
                             |""".stripMargin)
      throw new Exception("a nested optional type/value")
    }
    def handleFile(path: String): JsValue = {
      fileResolver.resolve(path) match {
        case dxFile: DxFileSource       => dxFile.dxFile.getLinkAsJson
        case localFile: LocalFileSource => JsString(localFile.toString)
        case other                      => throw new RuntimeException(s"Unsupported file source ${other}")
      }
    }
    (wdlType, wdlValue) match {
      // Base case: primitive types
      case (WdlTypes.T_File, WdlValues.V_String(path)) => handleFile(path)
      case (WdlTypes.T_File, WdlValues.V_File(path))   => handleFile(path)
      case (WdlTypes.T_String, WdlValues.V_File(path)) => JsString(path)
      case (WdlTypes.T_String, WdlValues.V_String(buf)) =>
        if (buf.length > MAX_STRING_LEN)
          throw new AppInternalException(s"string is longer than ${MAX_STRING_LEN}")
        JsString(buf)
      case (WdlTypes.T_Boolean, WdlValues.V_Boolean(b))      => JsBoolean(b)
      case (WdlTypes.T_Boolean, WdlValues.V_String("true"))  => JsBoolean(true)
      case (WdlTypes.T_Boolean, WdlValues.V_String("false")) => JsBoolean(false)

      // Integer conversions
      case (WdlTypes.T_Int, WdlValues.V_Int(n))    => JsNumber(n)
      case (WdlTypes.T_Int, WdlValues.V_String(s)) => JsNumber(s.toInt)
      case (WdlTypes.T_Int, WdlValues.V_Float(x))  => JsNumber(x.toInt)

      // Float conversions
      case (WdlTypes.T_Float, WdlValues.V_Float(x))  => JsNumber(x)
      case (WdlTypes.T_Float, WdlValues.V_Int(n))    => JsNumber(n.toFloat)
      case (WdlTypes.T_Float, WdlValues.V_String(s)) => JsNumber(s.toFloat)

      case (WdlTypes.T_Pair(lType, rType), WdlValues.V_Pair(l, r)) =>
        val lJs = jsFromWdlValue(lType, l)
        val rJs = jsFromWdlValue(rType, r)
        JsObject("left" -> lJs, "right" -> rJs)

      // Maps. These are projections from a key to value, where
      // the key and value types are statically known. We
      // represent them in JSON as an array of keys, followed by
      // an array of values.
      case (WdlTypes.T_Map(keyType, valueType), WdlValues.V_Map(m)) =>
        // general case
        val kJs = m.keys.map(jsFromWdlValue(keyType, _))
        val vJs = m.values.map(jsFromWdlValue(valueType, _))
        JsObject("keys" -> JsArray(kJs.toVector), "values" -> JsArray(vJs.toVector))

      // Arrays: these come after maps, because there is an automatic coercion from
      // a map to an array.
      //
      // Base case: empty array
      case (_, WdlValues.V_Array(ar)) if ar.isEmpty =>
        JsArray(Vector.empty)
      case (WdlTypes.T_Array(_, _), null) =>
        JsArray(Vector.empty)

      // Non empty array
      case (WdlTypes.T_Array(t, _), WdlValues.V_Array(elems)) =>
        val jsVals = elems.map { x =>
          jsFromWdlValue(t, x)
        }
        JsArray(jsVals)

      // Strip optional type
      case (WdlTypes.T_Optional(t), WdlValues.V_Optional(w)) =>
        jsFromWdlValue(t, w)
      case (WdlTypes.T_Optional(_), WdlValues.V_Null) =>
        JsNull
      case (WdlTypes.T_Optional(t), w) =>
        jsFromWdlValue(t, w)
      case (t, WdlValues.V_Optional(w)) =>
        jsFromWdlValue(t, w)

      // structs
      case (WdlTypes.T_Struct(structName, typeMap), WdlValues.V_Struct(_, valueMap)) =>
        // Convert each of the elements
        val mJs = valueMap.map {
          case (key, wdlValue) =>
            val elemType = typeMap.get(key) match {
              case None =>
                throw new Exception(s"""|ERROR
                                        |WdlTypes.T_Struct
                                        |  structName=${structName}
                                        |  typeMap=${typeMap}
                                        |  valueMap=${valueMap}
                                        |typeMap is missing key=${key}
                                        |""".stripMargin)
              case Some(t) => t
            }
            key -> jsFromWdlValue(elemType, wdlValue)
        }
        JsObject(mJs)

      case (_, _) =>
        throw new Exception(s"""|Unsupported combination:
                                |    wdlType:  ${wdlType}
                                |    wdlValue: ${wdlValue}""".stripMargin)
    }
  }

  // import a WDL value
  def importFromWDL(wdlType: WdlTypes.T, wdlValue: WdlValues.V): WdlVarLinks = {
    val jsValue = jsFromWdlValue(wdlType, wdlValue)
    WdlVarLinks(wdlType, DxlValue(jsValue))
  }

  // Convert a job input to a WdlValues.V. Do not download any files, convert them
  // to a string representation. For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
  //
  private def jobInputToWdlValue(name: String,
                                 wdlType: WdlTypes.T,
                                 jsValue: JsValue): WdlValues.V = {
    (wdlType, jsValue) match {
      // base case: primitive types
      case (WdlTypes.T_Boolean, JsBoolean(b)) => WdlValues.V_Boolean(b.booleanValue)
      case (WdlTypes.T_Int, JsNumber(bnm))    => WdlValues.V_Int(bnm.intValue)
      case (WdlTypes.T_Float, JsNumber(bnm))  => WdlValues.V_Float(bnm.doubleValue)
      case (WdlTypes.T_String, JsString(s))   => WdlValues.V_String(s)
      case (WdlTypes.T_File, JsString(s)) =>
        WdlValues.V_File(s)
      case (WdlTypes.T_File, JsObject(_)) =>
        // Convert the path in DNAx to a string. We can later
        // decide if we want to download it or not
        val dxFile = DxFile.fromJsValue(dxApi, jsValue)
        // use the cache value if there is one to save the API call
        WdlValues.V_File(dxFileCache.getOrElse(dxFile.id, dxFile).asUri)

      // Maps. These are serialized as an object with a keys array and
      // a values array.
      case (WdlTypes.T_Map(keyType, valueType), _) =>
        val fields = jsValue.asJsObject.fields
        // [mJs] is a map from json key to json value
        val mJs: Map[JsValue, JsValue] =
          (fields("keys"), fields("values")) match {
            case (JsArray(x), JsArray(y)) =>
              if (x.length != y.length)
                throw new Exception(s"""|len(keys) != len(values)
                                        |fields: ${fields}
                                        |""".stripMargin)
              (x zip y).toMap
            case _ =>
              throw new Exception(s"Malformed JSON ${fields}")
          }
        val m: Map[WdlValues.V, WdlValues.V] = mJs.map {
          case (k: JsValue, v: JsValue) =>
            val kWdl = jobInputToWdlValue(name, keyType, k)
            val vWdl = jobInputToWdlValue(name, valueType, v)
            kWdl -> vWdl
        }
        WdlValues.V_Map(m)

      case (WdlTypes.T_Pair(lType, rType), JsObject(fields))
          if List("left", "right").forall(fields.contains) =>
        val left = jobInputToWdlValue(name, lType, fields("left"))
        val right = jobInputToWdlValue(name, rType, fields("right"))
        WdlValues.V_Pair(left, right)

      // empty array
      case (WdlTypes.T_Array(_, _), JsNull) =>
        WdlValues.V_Array(Vector.empty[WdlValues.V])

      // array
      case (WdlTypes.T_Array(t, _), JsArray(vec)) =>
        val wVec: Vector[WdlValues.V] = vec.map { elem: JsValue =>
          jobInputToWdlValue(name, t, elem)
        }
        WdlValues.V_Array(wVec)

      case (WdlTypes.T_Optional(_), JsNull) =>
        WdlValues.V_Null
      case (WdlTypes.T_Optional(t), jsv) =>
        val value = jobInputToWdlValue(name, t, jsv)
        WdlValues.V_Optional(value)

      // structs
      case (WdlTypes.T_Struct(structName, typeMap), JsObject(fields)) =>
        val m: Map[String, WdlValues.V] = fields.map {
          case (key, jsValue) =>
            val t = typeMap.get(key) match {
              case None =>
                throw new Exception(s"""|ERROR
                                        |WdlTypes.T_Struct
                                        |  structName=${structName}
                                        |  typeMap=${typeMap}
                                        |  fields=${fields}
                                        |typeMap is missing key=${key}
                                        |""".stripMargin)
              case Some(t) => t
            }
            key -> jobInputToWdlValue(key, t, jsValue)
        }
        WdlValues.V_Struct(structName, m)

      case _ =>
        throw new AppInternalException(s"""|Unsupported combination
                                           |  name:    ${name}
                                           |  wdlType: ${wdlType}
                                           |  JSON:    ${jsValue.prettyPrint}
                                           |""".stripMargin)
    }
  }

  def unpackJobInput(name: String,
                     wdlType: WdlTypes.T,
                     jsv: JsValue): (WdlValues.V, Vector[DxFile]) = {
    val jsv1 =
      jsv match {
        case JsObject(fields) if fields contains "___" =>
          // unpack the hash with which complex JSON values are
          // wrapped in dnanexus.
          fields("___")
        case _ => jsv
      }
    val wdlValue = jobInputToWdlValue(name, wdlType, jsv1)
    val dxFiles = dxApi.findFiles(jsv)
    (wdlValue, dxFiles)
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

  // create input/output fields that bind the variable name [bindName] to
  // this WdlVar
  def genFields(wvl: WdlVarLinks,
                bindName: String,
                encodeDots: Boolean = true): List[(String, JsValue)] = {
    def nodots(s: String): String = encodeAppletVarName(WdlVarLinksConverter.transformVarName(s))
    val bindEncName =
      if (encodeDots) nodots(bindName)
      else bindName
    def mkSimple(): (String, JsValue) = {
      val jsv: JsValue = wvl.dxlink match {
        case DxlValue(jsn) => jsn
        case DxlStage(dxStage, ioRef, varEncName) =>
          ioRef match {
            case IORef.Input  => dxStage.getInputReference(nodots(varEncName))
            case IORef.Output => dxStage.getOutputReference(nodots(varEncName))
          }
        case DxlWorkflowInput(varEncName) =>
          JsObject(
              "$dnanexus_link" -> JsObject("workflowInputField" -> JsString(nodots(varEncName)))
          )
        case DxlExec(dxJob, varEncName) =>
          DxUtils.makeEBOR(dxJob, nodots(varEncName))
      }
      (bindEncName, jsv)
    }
    def mkComplex: Map[String, JsValue] = {
      val bindEncName_F = bindEncName + WdlVarLinksConverter.FLAT_FILES_SUFFIX
      wvl.dxlink match {
        case DxlValue(jsn) =>
          // files that are embedded in the structure
          val dxFiles = dxApi.findFiles(jsn)
          val jsFiles = dxFiles.map(_.getLinkAsJson)
          // Dx allows hashes as an input/output type. If the JSON value is
          // not a hash (js-object), we need to add an outer layer to it.
          val jsn1 = JsObject("___" -> jsn)
          Map(bindEncName -> jsn1, bindEncName_F -> JsArray(jsFiles))
        case DxlStage(dxStage, ioRef, varEncName) =>
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
        case DxlWorkflowInput(varEncName) =>
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
        case DxlExec(dxJob, varEncName) =>
          val varEncName_F = varEncName + WdlVarLinksConverter.FLAT_FILES_SUFFIX
          Map(bindEncName -> DxUtils.makeEBOR(dxJob, nodots(varEncName)),
              bindEncName_F -> DxUtils.makeEBOR(dxJob, nodots(varEncName_F)))
      }
    }

    val wdlType = stripOptional(wvl.wdlType)
    if (isNativeDxType(wdlType)) {
      // Types that are supported natively in DX
      List(mkSimple())
    } else {
      // General complex type requiring two fields: a JSON
      // structure, and a flat array of files.
      mkComplex.toList
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
