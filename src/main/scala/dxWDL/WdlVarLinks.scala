/**

Conversions from WDL types and data structures to DNAx JSON
representations. There are two difficulties this module needs to deal
with: (1) WDL has high order types which DNAx does not, and (2) the
file type is very different between WDL and DNAx.

  A file is a heavy weight data type, downloading it can fill up the
disk, and take a long time. There are import native WDL file
operations that do not require file data. For example, getting
file size, and passing around file references inside a
workflow. Therefore, we want to implement lazy file download.

A WDL file is equivalent to a path on a compute instance. There are
cases where files with the same name are referenced, for example, in a
scatter operation on an array AF of files. If we create local empty
files, representing members of AF, we will need to disambiguate the
files, and give them different paths. These "impure" paths, that are
implementation depedent, can then percolate to the rest of the
workflow. For this reason, we want to do lazy evaluation of WDL files,
not just lazy download.
  */
package dxWDL

import com.dnanexus.{DXFile, DXExecution, IOClass}
import java.nio.file.{Files, Paths}
import net.jcazevedo.moultingyaml._
import spray.json._
import Utils.{appletLog, dxFileFromJsValue, dxFileToJsValue,
    DX_URL_PREFIX, DXWorkflowStage, FLAT_FILES_SUFFIX, isNativeDxType}
import wdl.draft2.model.{WdlTask}
import wdl.draft2.model.types.WdlFlavoredWomType
import wom.types._
import wom.values._

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
case class DxlValue(jsn: JsValue) extends DxLink  // This may contain dx-files
case class DxlStage(dxStage: DXWorkflowStage, ioRef: IORef.Value, varName: String) extends DxLink
case class DxlWorkflowInput(varName: String) extends DxLink
case class DxlExec(dxExec: DXExecution, varName: String) extends DxLink

case class WdlVarLinks(womType: WomType,
                       attrs: DeclAttrs,
                       dxlink: DxLink)

object WdlVarLinks {
    // Human readable representation of a WdlVarLinks structure
    def yaml(wvl: WdlVarLinks) : YamlObject = {
        val (key, value) = wvl.dxlink match {
            case DxlValue(jsn) =>
                "JSON" -> jsn.prettyPrint
            case DxlStage(dxStage, ioRef, varEncName) =>
                "stageRef" -> varEncName
            case DxlWorkflowInput(varEncName) =>
                "workflowInput" -> varEncName
            case DxlExec(dxExec, varEncName) =>
                "execRef" -> varEncName
        }
        YamlObject(
            YamlString("type") -> YamlString(wvl.womType.toDisplayString),
            YamlString(key) -> YamlString(value))
    }

    private def getRawJsValue(wvl: WdlVarLinks) : JsValue = {
        wvl.dxlink match {
            case DxlValue(jsn) => jsn
            case _ =>
                throw new AppInternalException(
                    s"Unsupported conversion from ${wvl.dxlink} to WdlValue")
        }
    }

    private def isDxFile(jsValue: JsValue): Boolean = {
        jsValue match {
            case JsObject(fields) =>
                fields.get("$dnanexus_link") match {
                    case Some(JsString(s)) if s.startsWith("file-") => true
                    case Some(JsObject(linkFields)) =>
                        linkFields.get("id") match {
                            case Some(JsString(s)) if s.startsWith("file-") => true
                            case _ => false
                        }
                    case _ => false
                }
            case  _ => false
        }
    }

    // Search through a JSON value for all the dx:file links inside it. Returns
    // those as a vector.
    def findDxFiles(jsValue: JsValue) : Vector[DXFile] = {
        jsValue match {
            case JsBoolean(_) | JsNumber(_) | JsString(_) | JsNull =>
                Vector.empty[DXFile]
            case JsObject(_) if isDxFile(jsValue) =>
                Vector(dxFileFromJsValue(jsValue))
            case JsObject(fields) =>
                fields.map{ case(_,v) => findDxFiles(v) }.toVector.flatten
            case JsArray(elems) =>
                elems.map(e => findDxFiles(e)).flatten
        }
    }

    // find all dx:files referenced from the variable
    def findDxFiles(wvl: WdlVarLinks) : Vector[DXFile] = {
        findDxFiles(getRawJsValue(wvl))
    }

    // Get the file-id
    def getDxFile(wvl: WdlVarLinks) : DXFile = {
        assert(Utils.stripOptional(wvl.womType) == WomSingleFileType)
        wvl.dxlink match {
            case DxlValue(jsn) =>
                val dxFiles = findDxFiles(jsn)
                assert(dxFiles.length == 1)
                dxFiles.head
            case _ =>
                throw new Exception("cannot get file-id from non-JSON")
        }
    }

    // WDL maps and JSON objects are slightly different. A WDL map can
    // have keys of any type, whereas a JSON object can only have string
    // keys.
    //
    // For example, the WDL map {1 -> "A", 2 -> "B", 3 -> "C"} is converted into
    // the JSON object {
    //    "keys" ->   [1, 2, 3],
    //    "values" -> ["A", "B", "C"]
    // }
    //
    // If the WDL map key type is a string, we use the natural mapping. For example,
    // WDL value {"apple" -> "1$", "pear" -> "3$", "orange" -> "2$"}
    // is converted into JSON object {
    //    "apple": "1$",
    //    "pear" : "3$",
    //    "orange" : "2$"
    // }
    //
    // Unwrap only the top layers of the JSON map, do not dive in recursively.
    private def shallowUnmarshalWomMap(keyType: WomType,
                                       valueType: WomType,
                                       jsValue: JsValue) : Map[JsValue, JsValue] = {
        try {
            if (keyType == WomStringType) {
                // specialization for the case where keys are strings.
                val fields = jsValue.asJsObject.fields
                fields.map{ case (k:String, v:JsValue) =>
                    JsString(k) -> v
                }.toMap
            } else {
                // general case.
                val fields = jsValue.asJsObject.fields
                val kJs: Vector[JsValue] = fields("keys") match {
                    case JsArray(x) => x
                    case _ => throw new Exception("Malformed JSON")
                }
                val vJs: Vector[JsValue] = fields("values") match {
                    case JsArray(x) => x
                    case _ => throw new Exception("Malformed JSON")
                }
                assert(kJs.length == vJs.length)
                (kJs zip vJs).toMap
            }
        } catch {
            case e: Throwable =>
                System.err.println(s"Deserialization error: ${jsValue}")
                throw e
        }
    }

    private def unmarshalWomObject(m:Map[String, JsValue]) : Map[String, (WomType, JsValue)] = {
        m.map{
            case (key, JsObject(fields)) =>
                if (!List("type", "value").forall(fields contains _))
                    throw new Exception(
                        s"JSON object ${JsObject(fields)} does not contain fields {type, value}")
                val womType = fields("type") match {
                    case JsString(s) => WdlFlavoredWomType.fromDisplayString(s)
                    case other  => throw new Exception(s"type field is not a string (${other})")
                }
                key -> (womType, fields("value"))
            case (key, other) =>
                appletLog(true, s"Unmarshalling error for  JsObject=${JsObject(m)}")
                throw new Exception(s"key=${key}, expecting ${other} to be a JsObject")
        }.toMap
    }

    // Is this a local file path?
    private def isLocalPath(path:String) : Boolean = !(path contains "//")

    private def evalCoreHandleFile(jsv:JsValue,
                                   ioMode: IOMode.Value,
                                   ioDir: IODirection.Value) : WomValue = {
        (ioDir,jsv) match {
            case (IODirection.Upload, JsString(path)) if isLocalPath(path) =>
                // Local file that needs to be uploaded
                LocalDxFiles.upload(Paths.get(path))
                WomSingleFile(path)
            case (IODirection.Upload, JsObject(_)) =>
                // We already downloaded this file. We need to get from the dx:link
                // to a WomValue.
                val dxFile = dxFileFromJsValue(jsv)
                LocalDxFiles.get(dxFile) match {
                    case None =>
                        throw new AppInternalException(
                            s"dx:file ${jsv} is not found locally, it cannot be uploaded.")
                    case Some(path) => WomSingleFile(path.toString)
                }

            case (IODirection.Download, _) =>
                LocalDxFiles.download(jsv, ioMode)

            case (IODirection.Zero, JsString(path)) if path.startsWith(DX_URL_PREFIX) =>
                val dxFile = DxPath.lookupDxURLFile(path)
                WomSingleFile(DxPath.dxFileToURL(dxFile))
            case (IODirection.Zero, JsObject(_)) =>
                val dxFile = dxFileFromJsValue(jsv)
                val dxp = DxPath.dxFileToURL(dxFile)
                WomSingleFile(dxp)

            case (_,_) =>
                throw new Exception(s"Cannot transfer ${jsv} in context ${ioDir}")
        }
    }

    private def evalCore(womType: WomType,
                         jsValue: JsValue,
                         ioMode: IOMode.Value,
                         ioDir: IODirection.Value) : WomValue = {
        (womType, jsValue)  match {
            // base case: primitive types
            case (WomBooleanType, JsBoolean(b)) => WomBoolean(b.booleanValue)
            case (WomIntegerType, JsNumber(bnm)) => WomInteger(bnm.intValue)
            case (WomFloatType, JsNumber(bnm)) => WomFloat(bnm.doubleValue)
            case (WomStringType, JsString(s)) => WomString(s)
            case (WomSingleFileType, _) => evalCoreHandleFile(jsValue, ioMode, ioDir)

            // Maps. These are serialized as an object with a keys array and
            // a values array.
            case (WomMapType(keyType, valueType), _) =>
                val m: Map[WomValue, WomValue] =
                    shallowUnmarshalWomMap(keyType, valueType, jsValue).map{
                        case (k:JsValue, v:JsValue) =>
                            val kWom = evalCore(keyType, k, ioMode, ioDir)
                            val vWom = evalCore(valueType, v, ioMode, ioDir)
                            kWom -> vWom
                    }.toMap
                WomMap(WomMapType(keyType, valueType), m)

            case (WomPairType(lType, rType), JsObject(fields))
                    if (List("left", "right").forall(fields contains _)) =>
                val left = evalCore(lType, fields("left"), ioMode, ioDir)
                val right = evalCore(rType, fields("right"), ioMode, ioDir)
                WomPair(left, right)

            case (WomObjectType, JsObject(fields)) =>
                val m:Map[String, (WomType, JsValue)] = unmarshalWomObject(fields)
                val m2 = m.map{
                    case (key, (t, v)) =>
                        key -> evalCore(t, v, ioMode, ioDir)
                }.toMap
                WomObject(m2)

            // empty array
            case (WomArrayType(t), JsNull) =>
                WomArray(WomArrayType(t), List.empty[WomValue])

            // array
            case (WomArrayType(t), JsArray(vec)) =>
                val wVec: Seq[WomValue] = vec.map{
                    elem:JsValue => evalCore(t, elem, ioMode, ioDir)
                }
                WomArray(WomArrayType(t), wVec)

            case (WomOptionalType(t), JsNull) =>
                WomOptionalValue(t, None)
            case (WomOptionalType(t), jsv) =>
                val value = evalCore(t, jsv, ioMode, ioDir)
                WomOptionalValue(t, Some(value))

            case _ =>
                throw new AppInternalException(
                    s"Unsupported combination ${womType.toDisplayString} ${jsValue.prettyPrint}"
                )
        }
    }

    // Calculate a WdlValue from the dx-links structure. If [ioMode] is true,
    // any files included in the structure will be downloaded.
    def eval(wvl: WdlVarLinks,
             ioMode: IOMode.Value,
             ioDir: IODirection.Value) : WomValue = {
        val jsValue = getRawJsValue(wvl)
        evalCore(wvl.womType, jsValue, ioMode, ioDir)
    }

    // Download the dx:files in this wvl
    def localize(wvl:WdlVarLinks, ioMode:IOMode.Value) : WomValue = {
        eval(wvl, ioMode, IODirection.Download)
    }


    // Access a field in a complex WDL type, such as Pair, Map, Object.
    private def memberAccessStep(wvl: WdlVarLinks, fieldName: String) : WdlVarLinks = {
        val jsValue = getRawJsValue(wvl)
        (wvl.womType, jsValue) match {
            case (_:WomObject, JsObject(m)) =>
                unmarshalWomObject(m).get(fieldName) match {
                    case Some((womType,jsv)) => WdlVarLinks(womType, wvl.attrs, DxlValue(jsv))
                    case None => throw new Exception(s"Unknown field ${fieldName} in object ${wvl}")
                }
            case (WomPairType(lType, rType), JsObject(fields))
                    if (List("left", "right") contains fieldName) =>
                WdlVarLinks(lType, wvl.attrs, DxlValue(fields(fieldName)))
            case (WomOptionalType(t), _) =>
                // strip optional type
                val wvl1 = wvl.copy(womType = t)
                memberAccessStep(wvl1, fieldName)

            case _ =>
                throw new Exception(s"member access to field ${fieldName} wvl=${wvl}")
        }
    }

    // Multi step member access, appropriate for nested structures. For example:
    //
    // Pair[Int, Pair[String, File]] p
    // File veggies = p.left.right
    //
    def memberAccess(wvl: WdlVarLinks, components: List[String]) : WdlVarLinks = {
        components match {
            case Nil => wvl
            case head::tail =>
                val wvl1 = memberAccessStep(wvl, head)
                memberAccess(wvl1, tail)
        }
    }

    private def isDoubleOptional(t: WomType) : Boolean = {
        t match {
            case WomOptionalType(WomOptionalType(_)) => true
            case _ => false
        }
    }


    // Serialize a complex WDL value into a JSON value. The value could potentially point
    // to many files. Serialization proceeds recursively, as follows:
    // 1. Make a pass on the object, upload any files, and keep an in-memory JSON representation
    // 2. In memory we have a, potentially very large, JSON value. This can be handled pretty
    //    well by the platform as a dx:hash.
    private def jsFromWomValue(womType: WomType,
                               womValue: WomValue,
                               ioDir: IODirection.Value) : JsValue = {
        if (isDoubleOptional(womType) ||
                isDoubleOptional(womValue.womType)) {
            System.err.println(s"""|jsFromWomValue
                                   |    type=${womType.toDisplayString}
                                   |    val=${womValue.toWomString}
                                   |    val.type=${womValue.womType.toDisplayString}
                                   |    ioDir=${ioDir}
                                   |""".stripMargin)
            throw new Exception("a double optional type")
        }
        def handleFile(path:String) : JsValue =  {
            //System.err.println(s"jsFromWomValue:handleFile ${path}")
            ioDir match {
                case IODirection.Upload =>
                    if (Files.exists(Paths.get(path)))
                        LocalDxFiles.upload(Paths.get(path))
                    else
                        JsNull
                case IODirection.Download =>
                    LocalDxFiles.get(Paths.get(path)) match {
                        case None => throw new Exception(s"File ${path} has not been downloaded yet")
                        case Some(dxFile) => dxFileToJsValue(dxFile)
                    }
                case IODirection.Zero =>
                    if (!path.startsWith(DX_URL_PREFIX))
                        throw new Exception(s"${path} is not a dx:file, cannot transfer it in this context.")
                    val dxFile = DxPath.lookupDxURLFile(path)
                    dxFileToJsValue(dxFile)
            }
        }

        (womType, womValue) match {
            // Base case: primitive types
            case (WomSingleFileType, WomString(path)) => handleFile(path)
            case (WomSingleFileType, WomSingleFile(path)) => handleFile(path)
            case (WomStringType, WomSingleFile(path)) => JsString(path)
            case (WomStringType, WomString(buf)) =>
                if (buf.length > Utils.MAX_STRING_LEN)
                    throw new AppInternalException(s"string is longer than ${Utils.MAX_STRING_LEN}")
                JsString(buf)
            case (WomBooleanType,WomBoolean(b)) => JsBoolean(b)
            case (WomBooleanType,WomString("true")) => JsBoolean(true)
            case (WomBooleanType,WomString("false")) => JsBoolean(false)

            // Integer conversions
            case (WomIntegerType,WomInteger(n)) => JsNumber(n)
            case (WomIntegerType,WomString(s)) => JsNumber(s.toInt)
            case (WomIntegerType,WomFloat(x)) => JsNumber(x.toInt)

            // Float conversions
            case (WomFloatType, WomFloat(x)) => JsNumber(x)
            case (WomFloatType, WomInteger(n)) => JsNumber(n.toFloat)
            case (WomFloatType, WomString(s)) => JsNumber(s.toFloat)

            case (WomPairType(lType, rType), WomPair(l,r)) =>
                val lJs = jsFromWomValue(lType, l, ioDir)
                val rJs = jsFromWomValue(rType, r, ioDir)
                JsObject("left" -> lJs, "right" -> rJs)

            case (WomMapType(WomStringType, valueType), WomMap(_, m)) =>
                // keys are strings
                JsObject(m.map{
                             case (WomString(k), v) =>
                                 k -> jsFromWomValue(valueType, v, ioDir)
                             case (k,_) =>
                                 throw new Exception(s"key ${k.toWomString} should be a WomString")
                         }.toMap)

            // Maps. These are projections from a key to value, where
            // the key and value types are statically known. We
            // represent them in JSON as an array of keys, followed by
            // an array of values.
            case (WomMapType(keyType, valueType), WomMap(_, m)) =>
                // general case
                val keys:WomValue = WomArray(WomArrayType(keyType), m.keys.toVector)
                val kJs = jsFromWomValue(keys.womType, keys, ioDir)
                val values:WomValue = WomArray(WomArrayType(valueType), m.values.toVector)
                val vJs = jsFromWomValue(values.womType, values, ioDir)
                JsObject("keys" -> kJs, "values" -> vJs)

            // Arrays: these come after maps, because there is an automatic coercion from
            // a map to an array.
            //
            // Base case: empty array
            case (_, WomArray(_, ar)) if ar.length == 0 =>
                JsArray(Vector.empty)
            case (WomArrayType(t), null) =>
                JsArray(Vector.empty)

            // Non empty array
            case (WomArrayType(t), WomArray(_, elems)) =>
                val jsVals = elems.map{ x => jsFromWomValue(t, x, ioDir) }
                JsArray(jsVals.toVector)

            // keys are strings, requiring no conversion. Because objects
            // are not statically typed, we need to carry the types at runtime.
            case (WomObjectType, WomObject(m: Map[String, WomValue], _)) =>
                val jsm:Map[String, JsValue] = m.map{ case (key, w:WomValue) =>
                    key -> JsObject("type" -> JsString(w.womType.toDisplayString),
                                    "value" -> jsFromWomValue(w.womType, w, ioDir))
                }.toMap
                JsObject(jsm)

            // Strip optional type
            case (WomOptionalType(t), WomOptionalValue(_,Some(w))) =>
                jsFromWomValue(t, w, ioDir)
            case (WomOptionalType(t), WomOptionalValue(_,None)) =>
                JsNull
            case (WomOptionalType(t), w) =>
                jsFromWomValue(t, w, ioDir)
            case (t, WomOptionalValue(_,Some(w))) =>
                jsFromWomValue(t, w, ioDir)

            case (_,_) =>
                val womTypeStr =
                    if (womType == null)
                        "null"
                    else
                        womType.toDisplayString
                val womValueStr =
                    if (womValue == null)
                        "null"
                    else
                        s"(${womValue.toWomString}, ${womValue.womType.toDisplayString})"
                throw new Exception(s"""|Unsupported combination:
                                        |    womType:  ${womTypeStr}
                                        |    womValue: ${womValueStr}""".stripMargin)
        }
    }

    // import a WDL value
    def importFromWDL(womType: WomType,
                      attrs: DeclAttrs,
                      womValue: WomValue,
                      ioDir: IODirection.Value) : WdlVarLinks = {
        val jsValue = jsFromWomValue(womType, womValue, ioDir)
        WdlVarLinks(womType, attrs, DxlValue(jsValue))
    }


    // Dx allows hashes as an input/output type. If the JSON value is
    // not a hash (js-object), we need to add an outer layer to it.
    private def jsValueToDxHash(womType: WomType, jsVal: JsValue) : JsValue = {
        val m:Map[String, JsValue] = jsVal match {
            case JsObject(fields) =>
                assert(!(fields contains "womType"))
                fields
            case _ =>
                // Embed the value into a JSON object
                Map("value" -> jsVal)
        }
        val mWithType = m + ("womType" -> JsString(womType.toDisplayString))
        JsObject(mWithType)
    }

    private def unmarshalHash(jsv:JsValue) : (WomType, JsValue) = {
        jsv match {
            case JsObject(fields) =>
                // An object, the type is embedded as a 'womType' field
                fields.get("womType") match {
                    case Some(JsString(s)) =>
                        val t = WdlFlavoredWomType.fromDisplayString(s)
                        if (fields contains "value") {
                            // the value is encapsulated in the "value" field
                            (t, fields("value"))
                        } else {
                            // strip the womType field
                            (t, JsObject(fields - "womType"))
                        }
                    case _ => throw new Exception(
                        s"missing or malformed womType field in ${jsv}")
                }
            case other =>
                throw new Exception(s"JSON ${jsv} does not match the marshalled WDL value")
        }
    }

    // Convert an input field to a dx-links structure. This allows
    // passing it to other jobs.
    //
    // Note: we need to represent dx-files as local paths, even if we
    // do not download them. This is because accessing these files
    // later on will cause a WDL failure.
    def importFromDxExec(ioParam:DXIOParam,
                         attrs:DeclAttrs,
                         jsValue: JsValue) : WdlVarLinks = {
        if (ioParam.ioClass == IOClass.HASH) {
            val (t, v) = unmarshalHash(jsValue)
            if (ioParam.optional)
                (WomOptionalType(t), v)
            else
                (t, v)
            WdlVarLinks(t, attrs, DxlValue(v))
        } else {
            val womType =
                if (ioParam.optional) {
                    ioParam.ioClass match {
                        case IOClass.BOOLEAN => WomOptionalType(WomBooleanType)
                        case IOClass.INT => WomOptionalType(WomIntegerType)
                        case IOClass.FLOAT => WomOptionalType(WomFloatType)
                        case IOClass.STRING => WomOptionalType(WomStringType)
                        case IOClass.FILE => WomOptionalType(WomSingleFileType)
                        case IOClass.ARRAY_OF_BOOLEANS => WomMaybeEmptyArrayType(WomBooleanType)
                        case IOClass.ARRAY_OF_INTS => WomMaybeEmptyArrayType(WomIntegerType)
                        case IOClass.ARRAY_OF_FLOATS => WomMaybeEmptyArrayType(WomFloatType)
                        case IOClass.ARRAY_OF_STRINGS => WomMaybeEmptyArrayType(WomStringType)
                        case IOClass.ARRAY_OF_FILES => WomMaybeEmptyArrayType(WomSingleFileType)
                        case other => throw new Exception(s"unhandled IO class ${other}")
                    }
                } else {
                    ioParam.ioClass match {
                        case IOClass.BOOLEAN => WomBooleanType
                        case IOClass.INT => WomIntegerType
                        case IOClass.FLOAT => WomFloatType
                        case IOClass.STRING => WomStringType
                        case IOClass.FILE => WomSingleFileType
                        case IOClass.ARRAY_OF_BOOLEANS => WomNonEmptyArrayType(WomBooleanType)
                        case IOClass.ARRAY_OF_INTS => WomNonEmptyArrayType(WomIntegerType)
                        case IOClass.ARRAY_OF_FLOATS => WomNonEmptyArrayType(WomFloatType)
                        case IOClass.ARRAY_OF_STRINGS => WomNonEmptyArrayType(WomStringType)
                        case IOClass.ARRAY_OF_FILES => WomNonEmptyArrayType(WomSingleFileType)
                        case other => throw new Exception(s"unhandled IO class ${other}")
                    }
                }
            WdlVarLinks(womType, attrs, DxlValue(jsValue))
        }
    }

    // Import a value we got as an output from a dx:executable. In this case,
    // we don't have the dx:specification, but we do have the WomType.
    def importFromDxExec(womType:WomType,
                         attrs:DeclAttrs,
                         jsValue: JsValue) : WdlVarLinks = {
        val ioParam = womType match {
            // optional dx:native types
            case WomOptionalType(WomBooleanType) => DXIOParam(IOClass.BOOLEAN, true)
            case WomOptionalType(WomIntegerType) => DXIOParam(IOClass.INT, true)
            case WomOptionalType(WomFloatType) => DXIOParam(IOClass.FLOAT, true)
            case WomOptionalType(WomStringType) => DXIOParam(IOClass.STRING, true)
            case WomOptionalType(WomSingleFileType) => DXIOParam(IOClass.FILE, true)
            case WomMaybeEmptyArrayType(WomBooleanType) => DXIOParam(IOClass.ARRAY_OF_BOOLEANS, true)
            case WomMaybeEmptyArrayType(WomIntegerType) => DXIOParam(IOClass.ARRAY_OF_INTS, true)
            case WomMaybeEmptyArrayType(WomFloatType) => DXIOParam(IOClass.ARRAY_OF_FLOATS, true)
            case WomMaybeEmptyArrayType(WomStringType) => DXIOParam(IOClass.ARRAY_OF_STRINGS, true)
            case WomMaybeEmptyArrayType(WomSingleFileType) => DXIOParam(IOClass.ARRAY_OF_FILES, true)

            // compulsory dx:native types
            case WomBooleanType => DXIOParam(IOClass.BOOLEAN, false)
            case WomIntegerType => DXIOParam(IOClass.INT, false)
            case WomFloatType => DXIOParam(IOClass.FLOAT, false)
            case WomStringType => DXIOParam(IOClass.STRING, false)
            case WomSingleFileType => DXIOParam(IOClass.FILE, false)
            case WomNonEmptyArrayType(WomBooleanType) => DXIOParam(IOClass.ARRAY_OF_BOOLEANS, false)
            case WomNonEmptyArrayType(WomIntegerType) => DXIOParam(IOClass.ARRAY_OF_INTS, false)
            case WomNonEmptyArrayType(WomFloatType) => DXIOParam(IOClass.ARRAY_OF_FLOATS, false)
            case WomNonEmptyArrayType(WomStringType) => DXIOParam(IOClass.ARRAY_OF_STRINGS, false)
            case WomNonEmptyArrayType(WomSingleFileType) => DXIOParam(IOClass.ARRAY_OF_FILES, false)

            // non dx:native types, thse are converted to hashes
            case WomOptionalType(_) => DXIOParam(IOClass.HASH, true)
            case _ => DXIOParam(IOClass.HASH, false)
        }
        importFromDxExec(ioParam, attrs, jsValue)
    }

    // Import a value specified in a Cromwell style JSON input
    // file. Assume that all the platform files have already been
    // converted into dx:links.
    //
    // Challenges:
    // 1) avoiding an intermediate conversion into a WDL value. Most
    // types pose no issues. However, dx:files cannot be converted
    // into WDL files in all cases.
    // 2) JSON maps and WDL maps are slighly different. WDL maps can have
    // keys of any type, where JSON maps can only have string keys.
    private def importFromCromwell(womType: WomType,
                                   jsv: JsValue) : JsValue = {
        (womType, jsv) match {
            // base case: primitive types
            case (WomBooleanType, JsBoolean(_)) => jsv
            case (WomIntegerType, JsNumber(_)) => jsv
            case (WomFloatType, JsNumber(_)) => jsv
            case (WomStringType, JsString(_)) => jsv
            case (WomSingleFileType, JsObject(_)) => jsv

            // Maps. Since these have string values, they are, essentially,
            // mapped to WDL maps of type Map[String, T].
            case (WomMapType(keyType, valueType), JsObject(fields)) =>
                if (keyType != WomStringType)
                    throw new Exception("Importing a JSON object to a WDL map requires string keys")
                JsObject(fields.map{ case (k,v) =>
                             k -> importFromCromwell(valueType, v)
                         })

            case (WomPairType(lType, rType), JsArray(Vector(l,r))) =>
                val lJs = importFromCromwell(lType, l)
                val rJs = importFromCromwell(rType, r)
                JsObject("left" -> lJs, "right" -> rJs)

            case (WomObjectType, _) =>
                throw new Exception(
                    s"""|WDL Objects are not supported when converting from JSON inputs
                        |type = ${womType.toDisplayString}
                        |value = ${jsv.prettyPrint}
                        |""".stripMargin.trim)

            case (WomArrayType(t), JsArray(vec)) =>
                JsArray(vec.map{
                    elem => importFromCromwell(t, elem)
                })

            case (WomOptionalType(t), (null|JsNull)) => JsNull
            case (WomOptionalType(t), _) =>  importFromCromwell(t, jsv)

            case _ =>
                throw new Exception(
                    s"""|Unsupported/Invalid type/JSON combination in input file
                        |  womType= ${womType.toDisplayString}
                        |  JSON= ${jsv.prettyPrint}""".stripMargin.trim)
        }
    }

    def importFromCromwellJSON(womType: WomType,
                               attrs:DeclAttrs,
                               jsv: JsValue) : WdlVarLinks = {
        val importedJs = importFromCromwell(womType, jsv)
        WdlVarLinks(womType, attrs, DxlValue(importedJs))
    }

    // create input/output fields that bind the variable name [bindName] to
    // this WdlVar
    def genFields(wvl : WdlVarLinks,
                  bindName: String,
                  encodeDots: Boolean = true) : List[(String, JsValue)] = {
        val bindEncName =
            if (encodeDots)
                Utils.encodeAppletVarName(Utils.transformVarName(bindName))
            else
                bindName
        def mkSimple() : (String, JsValue) = {
            val jsv : JsValue = wvl.dxlink match {
                case DxlValue(jsn) => jsn
                case DxlStage(dxStage, ioRef, varEncName) =>
                    ioRef match {
                        case IORef.Input => dxStage.getInputReference(varEncName)
                        case IORef.Output => dxStage.getOutputReference(varEncName)
                    }
                case DxlWorkflowInput(varEncName) =>
                    JsObject("$dnanexus_link" -> JsObject(
                                 "workflowInputField" -> JsString(varEncName)))
                case DxlExec(dxJob, varEncName) =>
                    Utils.makeEBOR(dxJob, varEncName)
            }
            (bindEncName, jsv)
        }
        def mkComplex(womType: WomType) : Map[String, JsValue] = {
            val bindEncName_F = bindEncName + FLAT_FILES_SUFFIX
            wvl.dxlink match {
                case DxlValue(jsn) =>
                    // files that are embedded in the structure
                    val dxFiles = findDxFiles(jsn)
                    val jsFiles = dxFiles.map(x => Utils.jsValueOfJsonNode(x.getLinkAsJson))
                    // convert the top level structure into a hash
                    val hash = jsValueToDxHash(womType, jsn)
                    Map(bindEncName -> hash,
                        bindEncName_F -> JsArray(jsFiles))
                case DxlStage(dxStage, ioRef, varEncName) =>
                    val varEncName_F = varEncName + FLAT_FILES_SUFFIX
                    ioRef match {
                        case IORef.Input => Map(
                            bindEncName -> dxStage.getInputReference(varEncName),
                            bindEncName_F -> dxStage.getInputReference(varEncName_F)
                        )
                        case IORef.Output => Map(
                            bindEncName -> dxStage.getOutputReference(varEncName),
                            bindEncName_F -> dxStage.getOutputReference(varEncName_F)
                        )
                    }
                case DxlWorkflowInput(varEncName) =>
                    val varEncName_F = varEncName + FLAT_FILES_SUFFIX
                    Map( bindEncName ->
                            JsObject("$dnanexus_link" -> JsObject(
                                         "workflowInputField" -> JsString(varEncName))),
                         bindEncName_F ->
                            JsObject("$dnanexus_link" -> JsObject(
                                         "workflowInputField" -> JsString(varEncName_F)))
                    )
                case DxlExec(dxJob, varEncName) =>
                    val varEncName_F = varEncName + FLAT_FILES_SUFFIX
                    Map(bindEncName -> Utils.makeEBOR(dxJob, varEncName),
                        bindEncName_F -> Utils.makeEBOR(dxJob, varEncName_F))
            }
        }

        val womType = Utils.stripOptional(wvl.womType)
        if (isNativeDxType(womType)) {
            // Types that are supported natively in DX
            List(mkSimple())
        } else {
            // General complex type requiring two fields: a JSON
            // structure, and a flat array of files.
            mkComplex(womType).toList
        }
    }

    // Read the job-inputs JSON file, and convert the variables
    // to links that can be passed to other applets.
    def loadJobInputsAsLinks(inputLines: String,
                             inputSpec:Map[String, DXIOParam],
                             taskOpt: Option[WdlTask]): Map[String, WdlVarLinks] = {
        // Discard auxiliary fields
        val jsonAst : JsValue = inputLines.parseJson
        val fields : Map[String, JsValue] = jsonAst
            .asJsObject.fields
            .filter{ case (fieldName,_) => !fieldName.endsWith(FLAT_FILES_SUFFIX) }

        // Optional inputs could be missing, we want to convert
        // them into appropriate JSON null.
        val missingFields = inputSpec.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (key, ioParam)) if ioParam.optional =>
                fields.get(key) match {
                    case None if ioParam.optional =>
                        accu + (key -> JsNull)
                    case Some(v) => accu
                }
            case (accu, (key, ioClass)) => accu
        }

        // Create a mapping from each key to its WDL value,
        (fields ++ missingFields).map { case (key,jsValue) =>
            val ioParam = inputSpec.get(key) match {
                case Some(x) => x
                case None => throw new Exception(s"Key ${key} has no IO specification")
            }
            // Attach attributes, if any
            val attrs = taskOpt match {
                case None => DeclAttrs.empty
                case Some(task) => DeclAttrs.get(task, key)
            }
            val wvl = importFromDxExec(ioParam, attrs, jsValue)
            key -> wvl
        }.toMap
    }
}
