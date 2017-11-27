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

import com.dnanexus.{DXFile, DXJob, IOClass}
import java.nio.file.Paths
import net.jcazevedo.moultingyaml._
import spray.json._
import Utils.{appletLog, dxFileFromJsValue, dxFileToJsValue,
    DX_URL_PREFIX, DXWorkflowStage, FLAT_FILES_SUFFIX, isNativeDxType}
import wdl4s.wdl.types._
import wdl4s.wdl.values._

object IORef extends Enumeration {
    val Input, Output = Value
}

// The direction of IO:
//  Download: downloading files
//  Upload:  uploading files
//  Zero:    no upload or download should be attempted
object IODirection extends Enumeration {
    val Download, Upload, Zero = Value
}

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
case class DxlJob(dxJob: DXJob, varName: String) extends DxLink
case class DxlJobArray(dxJobVec: Vector[DXJob], varName: String) extends DxLink

case class WdlVarLinks(wdlType: WdlType,
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
            case DxlJob(dxJob, varEncName) =>
                "jobRef" -> varEncName
            case DxlJobArray(dxJobVec, varEncName) =>
                "jobRefArray" -> varEncName
        }
        YamlObject(
            YamlString("type") -> YamlString(wvl.wdlType.toWdlString),
            YamlString(key) -> YamlString(value))
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
            case JsBoolean(_) | JsNull | JsNumber(_) | JsString(_) =>
                Vector.empty[DXFile]
            case JsObject(_) if isDxFile(jsValue) =>
                Vector(dxFileFromJsValue(jsValue))
            case JsObject(fields) =>
                fields.map{ case(_,v) => findDxFiles(v) }.toVector.flatten
            case JsArray(elems) =>
                elems.map(e => findDxFiles(e)).flatten
        }
    }

    // Get the file-id
    def getFileId(wvl: WdlVarLinks) : String = {
        assert(Utils.stripOptional(wvl.wdlType) == WdlFileType)
        wvl.dxlink match {
            case DxlValue(jsn) =>
                val dxFiles = findDxFiles(jsn)
                assert(dxFiles.length == 1)
                dxFiles.head.getId()
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
    private def marshalWdlMap(keyType:WdlType,
                              valueType:WdlType,
                              m:Map[WdlValue, WdlValue],
                              ioDir: IODirection.Value) : JsValue = {
        if (keyType == WdlStringType) {
            // keys are strings
            JsObject(m.map{
                         case (WdlString(k), v) =>
                             k -> jsFromWdlValue(valueType, v, ioDir)
                         case (k,_) =>
                             throw new Exception(s"key ${k.toWdlString} should be a WdlStringType")
                     }.toMap)
        }
        else {
            // general case
            val keys:WdlValue = WdlArray(WdlArrayType(keyType), m.keys.toVector)
            val kJs = jsFromWdlValue(keys.wdlType, keys, ioDir)
            val values:WdlValue = WdlArray(WdlArrayType(valueType), m.values.toVector)
            val vJs = jsFromWdlValue(values.wdlType, values, ioDir)
            JsObject("keys" -> kJs, "values" -> vJs)
        }
    }

    // Unwrap only the top layers of the JSON map, do not dive in recursively.
    private def shallowUnmarshalWdlMap(keyType: WdlType,
                                       valueType: WdlType,
                                       jsValue: JsValue) : Map[JsValue, JsValue] = {
        try {
            if (keyType == WdlStringType) {
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

    private def marshalWdlObject(m: Map[String, WdlValue],
                                 ioDir: IODirection.Value) : JsValue = {
        val jsm:Map[String, JsValue] = m.map{ case (key, w:WdlValue) =>
            key -> JsObject("type" -> JsString(w.wdlType.toWdlString),
                            "value" -> jsFromWdlValue(w.wdlType, w, ioDir))
        }.toMap
        JsObject(jsm)
    }

    private def unmarshalWdlObject(m:Map[String, JsValue]) : Map[String, (WdlType, JsValue)] = {
        m.map{
            case (key, JsObject(fields)) =>
                if (!List("type", "value").forall(fields contains _))
                    throw new Exception(
                        s"JSON object ${JsObject(fields)} does not contain fields {type, value}")
                val wdlType = fields("type") match {
                    case JsString(s) => WdlType.fromWdlString(s)
                    case other  => throw new Exception(s"type field is not a string (${other})")
                }
                key -> (wdlType, fields("value"))
            case (key, other) =>
                appletLog(s"Unmarshalling error for  JsObject=${JsObject(m)}")
                throw new Exception(s"key=${key}, expecting ${other} to be a JsObject")
        }.toMap
    }

    // Is this a local file path?
    private def isLocalPath(path:String) : Boolean = !(path contains "//")

    private def evalCoreHandleFile(jsv:JsValue,
                                   force: Boolean,
                                   ioDir: IODirection.Value) : WdlValue = {
        (ioDir,jsv) match {
            case (IODirection.Upload, JsString(path)) if isLocalPath(path) =>
                // Local file that needs to be uploaded
                LocalDxFiles.upload(Paths.get(path))
                WdlSingleFile(path)
            case (IODirection.Upload, JsObject(_)) =>
                // We already downloaded this file. We need to get from the dx:link
                // to a WdlValue.
                val dxFile = dxFileFromJsValue(jsv)
                LocalDxFiles.get(dxFile) match {
                    case None =>
                        throw new AppInternalException(
                            s"dx:file ${jsv} is not found locally, it cannot be uploaded.")
                    case Some(path) => WdlSingleFile(path.toString)
                }

            case (IODirection.Download, _) =>
                LocalDxFiles.download(jsv, force)

            case (IODirection.Zero, JsString(path)) if path.startsWith(DX_URL_PREFIX) =>
                val dxFile = DxPath.lookupDxURLFile(path)
                WdlSingleFile(DxPath.dxFileToURL(dxFile))
            case (IODirection.Zero, JsObject(_)) =>
                val dxFile = dxFileFromJsValue(jsv)
                WdlSingleFile(DxPath.dxFileToURL(dxFile))

            case (_,_) =>
                throw new Exception(s"Cannot transfer ${jsv} in context ${ioDir}")
        }
    }

    private def evalCore(wdlType: WdlType,
                         jsValue: JsValue,
                         force: Boolean,
                         ioDir: IODirection.Value) : WdlValue = {
        (wdlType, jsValue)  match {
            // base case: primitive types
            case (WdlBooleanType, JsBoolean(b)) => WdlBoolean(b.booleanValue)
            case (WdlIntegerType, JsNumber(bnm)) => WdlInteger(bnm.intValue)
            case (WdlFloatType, JsNumber(bnm)) => WdlFloat(bnm.doubleValue)
            case (WdlStringType, JsString(s)) => WdlString(s)
            case (WdlFileType, _) => evalCoreHandleFile(jsValue, force, ioDir)

            // arrays
            case (WdlArrayType(t), JsArray(vec)) =>
                val wVec: Seq[WdlValue] = vec.map{
                    elem:JsValue => evalCore(t, elem, force, ioDir)
                }
                WdlArray(WdlArrayType(t), wVec)

            // Maps. These are serialized as an object with a keys array and
            // a values array.
            case (WdlMapType(keyType, valueType), _) =>
                val m: Map[WdlValue, WdlValue] =
                    shallowUnmarshalWdlMap(keyType, valueType, jsValue).map{
                        case (k:JsValue, v:JsValue) =>
                            val kWdl = evalCore(keyType, k, force, ioDir)
                            val vWdl = evalCore(valueType, v, force, ioDir)
                            kWdl -> vWdl
                    }.toMap
                WdlMap(WdlMapType(keyType, valueType), m)

            case (WdlObjectType, JsObject(fields)) =>
                val m:Map[String, (WdlType, JsValue)] = unmarshalWdlObject(fields)
                val m2 = m.map{
                    case (key, (t, v)) =>
                        key -> evalCore(t, v, force, ioDir)
                }.toMap
                WdlObject(m2)

            case (WdlPairType(lType, rType), JsObject(fields))
                    if (List("left", "right").forall(fields contains _)) =>
                val left = evalCore(lType, fields("left"), force, ioDir)
                val right = evalCore(rType, fields("right"), force, ioDir)
                WdlPair(left, right)

            case (WdlOptionalType(t), jsv) =>
                evalCore(t, jsv, force, ioDir)

            case _ =>
                throw new AppInternalException(
                    s"Unsupported combination ${wdlType.toWdlString} ${jsValue.prettyPrint}"
                )
        }
    }

    def getRawJsValue(wvl: WdlVarLinks) : JsValue = {
        wvl.dxlink match {
            case DxlValue(jsn) => jsn
            case _ =>
                throw new AppInternalException(
                    s"Unsupported conversion from ${wvl.dxlink} to WdlValue")
        }
    }

    // Calculate a WdlValue from the dx-links structure. If [force] is true,
    // any files included in the structure will be downloaded.
    def eval(wvl: WdlVarLinks,
             force: Boolean,
             ioDir: IODirection.Value) : WdlValue = {
        val jsValue = getRawJsValue(wvl)
        evalCore(wvl.wdlType, jsValue, force, ioDir)
    }

    // Download the dx:files in this wvl
    def localize(wvl:WdlVarLinks, force:Boolean) : WdlValue = {
        eval(wvl, force, IODirection.Download)
    }

    // The reason we need a special method for unpacking an array (or a map),
    // is because we DO NOT want to evaluate the sub-structures. The trouble is
    // files, that may all have the same paths, causing collisions.
    def unpackWdlArray(wvl: WdlVarLinks) : Seq[WdlVarLinks] = {
        val jsn = getRawJsValue(wvl)
        (wvl.wdlType, jsn) match {
            case (WdlArrayType(t), JsArray(l)) =>
                l.map(elem => WdlVarLinks(t, wvl.attrs, DxlValue(elem)))

            // Map. Convert into an array of WDL pairs.
            case (WdlMapType(keyType, valueType), _) =>
                val wdlType = WdlPairType(keyType, valueType)
                shallowUnmarshalWdlMap(keyType, valueType, jsn).map{
                    case (k:JsValue, v:JsValue) =>
                        val js:JsValue = JsObject("left" -> k, "right" -> v)
                        WdlVarLinks(wdlType, wvl.attrs, DxlValue(js))
                }.toVector

            // Strip optional type
            case (WdlOptionalType(t), _) =>
                val wvl1 = wvl.copy(wdlType = t)
                unpackWdlArray(wvl1)

            case (_,_) =>
                // Error
                throw new AppInternalException(s"Can't unpack ${wvl.wdlType.toWdlString} ${jsn}")
            }
    }

    // Access a field in a complex WDL type, such as Pair, Map, Object.
    private def memberAccessStep(wvl: WdlVarLinks, fieldName: String) : WdlVarLinks = {
        val jsValue = getRawJsValue(wvl)
        (wvl.wdlType, jsValue) match {
            case (_:WdlObject, JsObject(m)) =>
                unmarshalWdlObject(m).get(fieldName) match {
                    case Some((wdlType,jsv)) => WdlVarLinks(wdlType, wvl.attrs, DxlValue(jsv))
                    case None => throw new Exception(s"Unknown field ${fieldName} in object ${wvl}")
                }
            case (WdlPairType(lType, rType), JsObject(fields))
                    if (List("left", "right") contains fieldName) =>
                WdlVarLinks(lType, wvl.attrs, DxlValue(fields(fieldName)))
            case (WdlOptionalType(t), _) =>
                // strip optional type
                val wvl1 = wvl.copy(wdlType = t)
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

    // Serialize a complex WDL value into a JSON value. The value could potentially point
    // to many files. Serialization proceeds recursively, as follows:
    // 1. Make a pass on the object, upload any files, and keep an in-memory JSON representation
    // 2. In memory we have a, potentially very large, JSON value. This can be handled pretty
    //    well by the platform as a dx:hash.
    private def jsFromWdlValue(wdlType: WdlType,
                                    wdlValue: WdlValue,
                                    ioDir: IODirection.Value) : JsValue = {
        def handleFile(path:String) : JsValue = ioDir match {
            case IODirection.Upload =>
                LocalDxFiles.upload(Paths.get(path))
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

        (wdlType, wdlValue) match {
            // Base case: primitive types
            case (WdlFileType, WdlString(path)) => handleFile(path)
            case (WdlFileType, WdlSingleFile(path)) => handleFile(path)
            case (WdlStringType, WdlSingleFile(path)) => JsString(path)
            case (WdlStringType, WdlString(buf)) =>
                if (buf.length > Utils.MAX_STRING_LEN)
                    throw new AppInternalException(s"string is longer than ${Utils.MAX_STRING_LEN}")
                JsString(buf)
            case (WdlBooleanType,WdlBoolean(b)) => JsBoolean(b)
            case (WdlIntegerType,WdlInteger(n)) => JsNumber(n)
            case (WdlFloatType, WdlFloat(x)) => JsNumber(x)

            // Base case: empty array
            case (_, WdlArray(_, ar)) if ar.length == 0 =>
                JsArray(Vector.empty)

            // Non empty array
            case (WdlArrayType(t), WdlArray(_, elems)) =>
                val jsVals = elems.map(e => jsFromWdlValue(t, e, ioDir))
                JsArray(jsVals.toVector)

            // automatically cast an element from type T to Array[T]
            case (WdlArrayType(t), elem) =>
                JsArray(jsFromWdlValue(t,elem, ioDir))

            // Maps. These are projections from a key to value, where
            // the key and value types are statically known. We
            // represent them in JSON as an array of keys, followed by
            // an array of values.
            case (WdlMapType(keyType, valueType), WdlMap(_, m)) =>
                marshalWdlMap(keyType, valueType, m, ioDir)

            // keys are strings, requiring no conversion. Because objects
            // are not statically typed, we need to carry the types at runtime.
            case (WdlObjectType, WdlObject(m: Map[String, WdlValue])) =>
                marshalWdlObject(m, ioDir)

            case (WdlPairType(lType, rType), WdlPair(l,r)) =>
                val lJs = jsFromWdlValue(lType, l, ioDir)
                val rJs = jsFromWdlValue(rType, r, ioDir)
                JsObject("left" -> lJs, "right" -> rJs)

            // Strip optional type
            case (WdlOptionalType(t), WdlOptionalValue(_,Some(w))) =>
                jsFromWdlValue(t, w, ioDir)
            case (WdlOptionalType(t), w) =>
                jsFromWdlValue(t, w, ioDir)
            case (t, WdlOptionalValue(_,Some(w))) =>
                jsFromWdlValue(t, w, ioDir)

            // If the value is none then, it is a missing value
            // What if the value is null?

            case (_,_) => throw new Exception(
                s"""|Unsupported combination type=(${wdlType.toWdlString},${wdlType})
                    |value=(${wdlValue.toWdlString}, ${wdlValue})"""
                    .stripMargin.replaceAll("\n", " "))
        }
    }

    // import a WDL value
    def importFromWDL(wdlType: WdlType,
                      attrs: DeclAttrs,
                      wdlValue: WdlValue,
                      ioDir: IODirection.Value) : WdlVarLinks = {
        val jsValue = jsFromWdlValue(wdlType, wdlValue, ioDir)
        WdlVarLinks(wdlType, attrs, DxlValue(jsValue))
    }

    def mkJborArray(dxJobVec: Vector[DXJob],
                    varName: String) : JsValue = {
        val jbors: Vector[JsValue] = dxJobVec.map{ dxJob =>
            val jobId : String = dxJob.getId()
            Utils.makeJBOR(jobId, varName)
        }
        JsArray(jbors)
    }


    // Dx allows hashes as an input/output type. If the JSON value is
    // not a hash (js-object), we need to add an outer layer to it.
    private def jsValueToDxHash(wdlType: WdlType, jsVal: JsValue) : JsValue = {
        val m:Map[String, JsValue] = jsVal match {
            case JsObject(fields) =>
                assert(!(fields contains "wdlType"))
                fields
            case _ =>
                // Embed the value into a JSON object
                Map("value" -> jsVal)
        }
        val mWithType = m + ("wdlType" -> JsString(wdlType.toWdlString))
        JsObject(mWithType)
    }

    // Convert an input field to a dx-links structure. This allows
    // passing it to other jobs.
    //
    // Note: we need to represent dx-files as local paths, even if we
    // do not download them. This is because accessing these files
    // later on will cause a WDL failure.
    private [dxWDL] def importFromDxExec(ioClass:IOClass,
                                         attrs:DeclAttrs,
                                         jsValue: JsValue) : WdlVarLinks = {
        appletLog(s"importFromDxExec ioClass=${ioClass} js=${jsValue}")
        val (wdlType, jsv) = ioClass match {
            case IOClass.BOOLEAN => (WdlBooleanType, jsValue)
            case IOClass.INT => (WdlIntegerType, jsValue)
            case IOClass.FLOAT => (WdlFloatType, jsValue)
            case IOClass.STRING => (WdlStringType, jsValue)
            case IOClass.FILE => (WdlFileType, jsValue)
            case IOClass.ARRAY_OF_BOOLEANS => (WdlArrayType(WdlBooleanType), jsValue)
            case IOClass.ARRAY_OF_INTS => (WdlArrayType(WdlIntegerType), jsValue)
            case IOClass.ARRAY_OF_FLOATS => (WdlArrayType(WdlFloatType), jsValue)
            case IOClass.ARRAY_OF_STRINGS => (WdlArrayType(WdlStringType), jsValue)
            case IOClass.ARRAY_OF_FILES => (WdlArrayType(WdlFileType), jsValue)
            case IOClass.HASH =>
                jsValue match {
                    case JsObject(fields) =>
                        // An object, the type is embedded as a 'wdlType' field
                        fields.get("wdlType") match {
                            case Some(JsString(s)) =>
                                val t = WdlType.fromWdlString(s)
                                if (fields contains "value") {
                                    // the value is encapsulated in the "value" field
                                    (t, fields("value"))
                                } else {
                                    // strip the wdlType field
                                    (t, JsObject(fields - "wdlType"))
                                }
                            case _ => throw new Exception(
                                s"missing or malformed wdlType field in ${jsValue}")
                        }
                    case _ => throw new Exception(s"IO class is HASH, but JSON is ${jsValue}")
                }
            case other => throw new Exception(s"unhandled IO class ${other}")
        }

        WdlVarLinks(wdlType, attrs, DxlValue(jsv))
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
    private def importFromCromwell(wdlType: WdlType,
                                   jsv: JsValue) : JsValue = {
        (wdlType, jsv) match {
            // base case: primitive types
            case (WdlBooleanType, JsBoolean(_)) => jsv
            case (WdlIntegerType, JsNumber(_)) => jsv
            case (WdlFloatType, JsNumber(_)) => jsv
            case (WdlStringType, JsString(_)) => jsv
            case (WdlFileType, JsObject(_)) => jsv

            // strip optionals
            case (WdlOptionalType(t), _) =>
                importFromCromwell(t, jsv)

            // arrays
            case (WdlArrayType(t), JsArray(vec)) =>
                JsArray(vec.map{
                    elem => importFromCromwell(t, elem)
                })

            // Maps. Since these have string values, they are, essentially,
            // mapped to WDL maps of type Map[String, T].
            case (WdlMapType(keyType, valueType), JsObject(fields)) =>
                if (keyType != WdlStringType)
                    throw new Exception("Importing a JSON object to a WDL map requires string keys")
                JsObject(fields.map{ case (k,v) =>
                             k -> importFromCromwell(valueType, v)
                         })

            case (WdlPairType(lType, rType), JsArray(Vector(l,r))) =>
                val lJs = importFromCromwell(lType, l)
                val rJs = importFromCromwell(rType, r)
                JsObject("left" -> lJs, "right" -> rJs)

            case (WdlObjectType, _) =>
                throw new Exception(
                    s"""|WDL Objects are not supported when converting from JSON inputs
                        |type = ${wdlType.toWdlString}
                        |value = ${jsv.prettyPrint}
                        |""".stripMargin.trim)

            case _ =>
                throw new Exception(
                    s"""|Unsupported/Invalid type/JSON combination in input file
                        |  wdlType= ${wdlType.toWdlString}
                        |  JSON= ${jsv.prettyPrint}""".stripMargin.trim)
        }
    }

    def importFromCromwellJSON(wdlType: WdlType,
                               attrs:DeclAttrs,
                               jsv: JsValue) : WdlVarLinks = {
        val importedJs = importFromCromwell(wdlType, jsv)
        WdlVarLinks(wdlType, attrs, DxlValue(importedJs))
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
                case DxlJob(dxJob, varEncName) =>
                    val jobId : String = dxJob.getId()
                    Utils.makeJBOR(jobId, varEncName)
                case DxlJobArray(dxJobVec, varEncName) =>
                    mkJborArray(dxJobVec, varEncName)
            }
            (bindEncName, jsv)
        }
        def mkComplex(wdlType: WdlType) : Map[String, JsValue] = {
            val bindEncName_F = bindEncName + FLAT_FILES_SUFFIX
            wvl.dxlink match {
                case DxlValue(jsn) =>
                    // files that are embedded in the structure
                    val dxFiles = findDxFiles(jsn)
                    val jsFiles = dxFiles.map(x => Utils.jsValueOfJsonNode(x.getLinkAsJson))
                    // convert the top level structure into a hash
                    val hash = jsValueToDxHash(wdlType, jsn)
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
                case DxlJob(dxJob, varEncName) =>
                    val varEncName_F = varEncName + FLAT_FILES_SUFFIX
                    val jobId : String = dxJob.getId()
                    Map(bindEncName -> Utils.makeJBOR(jobId, varEncName),
                        bindEncName_F -> Utils.makeJBOR(jobId, varEncName_F))
                case DxlJobArray(dxJobVec, varEncName) =>
                    val varEncName_F = varEncName + FLAT_FILES_SUFFIX
                    Map(bindEncName -> mkJborArray(dxJobVec, varEncName),
                        bindEncName_F -> mkJborArray(dxJobVec, varEncName_F))
            }
        }

        val wdlType = Utils.stripOptional(wvl.wdlType)
        if (isNativeDxType(wdlType)) {
            // Types that are supported natively in DX
            List(mkSimple())
        } else {
            // General complex type requiring two fields: a JSON
            // structure, and a flat array of files.
            mkComplex(wdlType).toList
        }
    }

    // Read the job-inputs JSON file, and convert the variables
    // to links that can be passed to other applets.
    def loadJobInputsAsLinks(inputLines: String,
                             inputSpec:Map[String, IOClass]): Map[String, WdlVarLinks] = {
        def isArrayIOClass(ioClass:IOClass) : Boolean = ioClass match {
            case IOClass.ARRAY_OF_BOOLEANS => true
            case IOClass.ARRAY_OF_INTS => true
            case IOClass.ARRAY_OF_FLOATS => true
            case IOClass.ARRAY_OF_STRINGS => true
            case IOClass.ARRAY_OF_FILES => true
            case _ => false
        }

        // Discard auxiliary fields
        val jsonAst : JsValue = inputLines.parseJson
        val fields : Map[String, JsValue] = jsonAst
            .asJsObject.fields
            .filter{ case (fieldName,_) => !fieldName.endsWith(FLAT_FILES_SUFFIX) }

        // Some inputs could be missing. We want to convert
        // a missing input array to an empty array instead.
        //
        // This is like DNAx semantics, but not exactly WDL. If a task has
        // array input A, and it is called without A, then, the callee will
        // see A=[], instead of A=null.
        val missingFields = inputSpec.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (key, ioClass)) if isArrayIOClass(ioClass) =>
                fields.get(key) match {
                    case None => accu + (key -> JsArray(Vector.empty))
                    case Some(v) => accu
                }
            case (accu, (key, ioClass)) => accu
        }

        // Create a mapping from each key to its WDL value,
        (fields ++ missingFields).map { case (key,jsValue) =>
            val ioClass = inputSpec.get(key) match {
                case Some(x) => x
                case None => throw new Exception(s"Key ${key} has no IO specification")
            }
            val wvl = importFromDxExec(ioClass, DeclAttrs.empty, jsValue)
            key -> wvl
        }.toMap
    }

    // Merge an array of links into one. All the links
    // have to be of the same dxlink type.
    def merge(vec: Vector[WdlVarLinks]) : WdlVarLinks = {
        if (vec.isEmpty)
            throw new Exception("Sanity: WVL array has to be non empty")

        val wdlType = WdlArrayType(vec.head.wdlType)
        val declAttrs = vec.head.attrs
        vec.head.dxlink match {
            case DxlValue(_) =>
                val jsVec:Vector[JsValue] = vec.map{ wvl =>
                    wvl.dxlink match {
                        case DxlValue(jsv) => jsv
                        case _ => throw new Exception("Sanity")
                    }
                }
                WdlVarLinks(wdlType, declAttrs, DxlValue(JsArray(jsVec)))

            case DxlJob(_, varName) =>
                val jobVec:Vector[DXJob] = vec.map{ wvl =>
                    wvl.dxlink match {
                        case DxlJob(job,name) =>
                            assert(name == varName)
                            job
                        case _ => throw new Exception("Sanity")
                    }
                }
                WdlVarLinks(wdlType, declAttrs, DxlJobArray(jobVec, varName))
            case _ => throw new Exception(s"Don't know how to merge WVL arrays of type ${vec.head}")
        }
    }
}
