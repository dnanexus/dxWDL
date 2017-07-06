package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXFile, DXJob, DXProject, DXWorkflow}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.HashMap
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wdl4s.types._
import wdl4s.values._

object IORef extends Enumeration {
    val Input, Output = Value
}

// A union of all the different ways of building a value
// from JSON passed by the platform.
//
// Complex values are WDL values that have files embedded in them. For example:
// - Ragged file array:  Array[Array[File]]
// - Object with file elements
// - Map of files:     Map[String, File]
// A complex value is implemented as a json structure, and all the files it references.
sealed trait DxLink
case class DxlJsValue(jsn: JsValue) extends DxLink
case class DxlComplexValue(jsn: JsValue, files: Vector[DXFile]) extends DxLink
case class DxlStage(dxStage: DXWorkflow.Stage, ioRef: IORef.Value, varName: String) extends DxLink
case class DxlJob(dxJob: DXJob, ioRef: IORef.Value, varName: String) extends DxLink

case class WdlVarLinks(wdlType: WdlType, dxlink: DxLink)

// Bridge between WDL values and DNAx values.
case class BValue(wvl: WdlVarLinks, wdlValue: WdlValue)

object WdlVarLinks {
    // A dictionary of all WDL files that are also
    // platform files. This can happen if the file was downloaded
    // from the platform, or if it was uploaded.
    var localDxFiles = HashMap.empty[Path, DXFile]

    private def isDxFile(jsValue: JsValue): Boolean = {
        jsValue match {
            case JsObject(fields) =>
                fields.get("$dnanexus_link") match {
                    case None => false
                    case Some(_) => true
                }
            case  _ => false
        }
    }

    // Parse a dnanexus file descriptor. Examples:
    //
    // "$dnanexus_link": {
    //    "project": "project-BKJfY1j0b06Z4y8PX8bQ094f",
    //    "id": "file-BKQGkgQ0b06xG5560GGQ001B"
    //   }
    //
    //  {"$dnanexus_link": "file-F0J6JbQ0ZvgVz1J9q5qKfkqP"}
    //
    private def dxFileOfJsValue(jsValue : JsValue) : DXFile = {
        val innerObj = jsValue match {
            case JsObject(fields) =>
                fields.get("$dnanexus_link") match {
                    case None => throw new AppInternalException(s"Bad json of dnanexus link $jsValue")
                    case Some(x) => x
                }
            case  _ =>
                throw new AppInternalException(s"Bad json of dnanexus link $jsValue")
        }

        val (fid, projId) : (String, Option[String]) = innerObj match {
            case JsString(fid) =>
                // We just have a file-id
                (fid, None)
            case JsObject(linkFields) =>
                // file-id and project-id
                val fid =
                    linkFields.get("id") match {
                        case Some(JsString(s)) => s
                        case _ => throw new AppInternalException(s"No file ID found in dnanexus link $jsValue")
                    }
                linkFields.get("project") match {
                    case Some(JsString(pid : String)) => (fid, Some(pid))
                    case _ => (fid, None)
                }
            case _ =>
                throw new AppInternalException(s"Could not parse a dxlink from $innerObj")
        }

        projId match {
            case None => DXFile.getInstance(fid)
            case Some(pid) => DXFile.getInstance(fid, DXProject.getInstance(pid))
        }
    }

    private def wdlFileOfDxLink(jsValue: JsValue, force: Boolean) : WdlValue = {
        // Download the file, and place it in a local file, with the
        // same name as the platform. All files have to be downloaded
        // into the same directory; the only exception we make is for
        // disambiguation purposes.
        val dxFile = dxFileOfJsValue(jsValue)
        val fName = dxFile.describe().getName()
        val shortPath = Utils.inputFilesDirPath.resolve(fName)
        val path : Path =
            if (Files.exists(shortPath)) {
                // Short path already exists. Note: this check is brittle in the case
                // of concurrent downloads.
                val fid = dxFile.getId()
                System.err.println(s"Disambiguating file ${fid} with name ${fName}")
                val dir:Path = Utils.inputFilesDirPath.resolve(fid)
                Utils.safeMkdir(dir)
                Utils.inputFilesDirPath.resolve(fid).resolve(fName)
            } else {
                shortPath
            }
        localDxFiles.get(path) match {
            case None =>
                if (force) {
                    // Download right now
                    Utils.downloadFile(path, dxFile)
                } else {
                    // Create an empty file, to mark the fact that the path and
                    // file name are in use. We may not end up downloading the
                    // file, and accessing the data, however, we need to keep
                    // the path in the WdlFile value unique.
                    Files.createFile(path)
                    DxFunctions.registerRemoteFile(path.toString, dxFile)
                }
                localDxFiles(path) = dxFile
            case Some(dxFile) =>
                // we have already downloaded the file
                ()
        }
        WdlSingleFile(path.toString)
    }

    // Is this a WDL type that maps to a native DX type?
    private def hasNativeDxType(wdlType: WdlType) : Boolean = {
        wdlType match {
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType | WdlFileType
                   | WdlArrayType(WdlBooleanType)
                   | WdlArrayType(WdlIntegerType)
                   | WdlArrayType(WdlFloatType)
                   | WdlArrayType(WdlStringType)
                   | WdlArrayType(WdlFileType) => true
            case _ => false
        }
    }

    // Could a structure of this type include Files? Some examples:
    // Array[Int]        false
    // Map[String,File]  true
    // Object            true
    //
    // Objects may include files, because their types are only
    // known at runtime
    def mayHaveFiles(wdlType: WdlType) : Boolean = {
        wdlType match {
            // base cases
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType => false
            case WdlFileType => true
            case WdlObjectType => true

            // recursion
            case WdlOptionalType(t) => mayHaveFiles(t)
            case WdlArrayType(t) => mayHaveFiles(t)
            case WdlPairType(lType, rType) =>
                mayHaveFiles(lType) || mayHaveFiles(rType)
            case WdlMapType(keyType, valueType) =>
                mayHaveFiles(keyType) || mayHaveFiles(valueType)
        }
    }

    // Search through a JSON value for all the dx:file links inside it. Returns
    // those as a vector.
    private def findDxFiles(jsValue: JsValue) : Vector[DXFile] = {
        jsValue match {
            case JsBoolean(_) | JsNull | JsNumber(_) | JsString(_) =>
                Vector.empty[DXFile]
            case JsObject(_) if isDxFile(jsValue) =>
                Vector(dxFileOfJsValue(jsValue))
            case JsObject(fields) =>
                fields.map{ case(k,v) => findDxFiles(v) }.toVector.flatten
            case JsArray(elems) =>
                elems.map(e => findDxFiles(e)).flatten
        }
    }

    def unmarshalJsMap(jsValue: JsValue) : (Vector[JsValue], Vector[JsValue]) = {
        try {
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
            (kJs, vJs)
        } catch {
            case e : Throwable =>
                System.err.println(s"Deserialization error: ${jsValue}")
                throw new Exception("JSON value deserialization error, expected Map")
        }
    }

    private def evalCore(wdlType: WdlType, jsValue: JsValue, force: Boolean) : WdlValue = {
        (wdlType, jsValue)  match {
            // base case: primitive types
            case (WdlBooleanType, JsBoolean(b)) => WdlBoolean(b.booleanValue)
            case (WdlIntegerType, JsNumber(bnm)) => WdlInteger(bnm.intValue)
            case (WdlFloatType, JsNumber(bnm)) => WdlFloat(bnm.doubleValue)
            case (WdlStringType, JsString(s)) => WdlString(s)
            case (WdlFileType, _) => wdlFileOfDxLink(jsValue, force)

            // arrays
            case (WdlArrayType(t), JsArray(vec)) =>
                val wVec: Seq[WdlValue] = vec.map{
                    elem:JsValue => evalCore(t, elem, force)
                }
                WdlArray(WdlArrayType(t), wVec)

            // Maps. These are serialized as an object with a keys array and
            // a values array.
            case (WdlMapType(keyType, valueType), _) =>
                val (kJs, vJs) = unmarshalJsMap(jsValue)
                val kv = kJs zip vJs
                val m: Map[WdlValue, WdlValue] = kv.map{ case (k,v) =>
                    val kWdl = evalCore(keyType, k, force)
                    val vWdl = evalCore(valueType, v, force)
                    kWdl -> vWdl
                }.toMap
                WdlMap(WdlMapType(keyType, valueType), m)

            case (WdlPairType(lType, rType), JsArray(vec)) =>
                assert(vec.length == 2)
                val left = evalCore(lType, vec(0), force)
                val right = evalCore(rType, vec(1), force)
                WdlPair(left, right)

            // TODO
            //case (WdlObjectType, WdlObject())
            case _ =>
                throw new AppInternalException(
                    s"Unsupport combination ${wdlType.toWdlString} ${jsValue.prettyPrint}"
                )
        }
    }

    private def getRawJsValue(wvl: WdlVarLinks) : JsValue = {
        val jsRaw: JsValue = wvl.dxlink match {
            case DxlJsValue(jsn) => jsn
            case DxlComplexValue(jsn,_) => jsn
            case _ =>
                throw new AppInternalException(s"Unsupported conversion from ${wvl.dxlink} to WdlValue")
        }
        if (!hasNativeDxType(wvl.wdlType)) {
            jsRaw match {
                case JsObject(_) if isDxFile(jsRaw) =>
                    // The JSON points to a platform file, it needs
                    // to be downloaded and parsed.
                    val dxfile = dxFileOfJsValue(jsRaw)
                    val buf = Utils.downloadString(dxfile)
                    buf.parseJson
                case _ =>
                    //System.err.println(s"Non native DX type ${wvl.wdlType.toWdlString}")
                    jsRaw
            }
        } else {
            jsRaw
        }
    }

    // Calculate a WdlValue from the dx-links structure. If [force] is true,
    // any files included in the structure will be downloaded.
    def eval(wvl: WdlVarLinks, force: Boolean) : WdlValue = {
        val jsValue = getRawJsValue(wvl)
        evalCore(wvl.wdlType, jsValue, force)
    }

    // Access a field in a complex WDL type, such as Pair, Map, Object.
    def memberAccess(wvl: WdlVarLinks, field: String) : WdlVarLinks = {
        val jsValue = getRawJsValue(wvl)

        (wvl.wdlType, jsValue) match {
            case (WdlPairType(lType, rType), JsArray(vec)) =>
                field match {
                    case "left" =>  WdlVarLinks(lType, DxlJsValue(vec(0)))
                    case "right" =>  WdlVarLinks(rType, DxlJsValue(vec(1)))
                    case _ => throw new Exception(s"Unknown field ${field} in pair ${wvl}")
                }
            case _ =>
                throw new Exception(s"member access to field ${field} wvl=${wvl}")
        }
    }

    private def jsStringLimited(buf: String) : JsValue = {
        if (buf.length > Utils.MAX_STRING_LEN)
            throw new AppInternalException(s"string is longer than ${Utils.MAX_STRING_LEN}")
        JsString(buf)
    }

    // Serialize a complex WDL value into a JSON value. The value could potentially point
    // to many files. Serialization proceeds as follows:
    // 1. Make a pass on the object, upload any files, and keep an in-memory JSON representation
    // 2. In memory we have a, potentially very large, JSON value. Upload it to the platform
    //    as a file, and return a JSON link to the file.
    private def jsOfComplexWdlValue(wdlType: WdlType,
                                    wdlValue: WdlValue) : (JsValue, Vector[DXFile]) = {
        def uploadFile(path: Path) : (JsValue, Vector[DXFile]) =  {
            localDxFiles.get(path) match {
                case None =>
                    val dxLink = Utils.uploadFile(path)
                    val dxFile = dxFileOfJsValue(dxLink)
                    (dxLink, Vector(dxFile))
                case Some(dxFile) =>
                    val dxLink = Utils.jsValueOfJsonNode(dxFile.getLinkAsJson)
                    (dxLink, Vector(dxFile))
            }
        }

        (wdlType, wdlValue) match {
            // Base case: primitive types
            case (WdlFileType, WdlString(path)) => uploadFile(Paths.get(path))
            case (WdlFileType, WdlSingleFile(path)) => uploadFile(Paths.get(path))
            case (WdlStringType, WdlSingleFile(path)) => (JsString(path), Vector.empty[DXFile])
            case (_,WdlBoolean(b)) => (JsBoolean(b), Vector.empty[DXFile])
            case (_,WdlInteger(n)) => (JsNumber(n), Vector.empty[DXFile])
            case (_,WdlFloat(x)) => (JsNumber(x), Vector.empty[DXFile])
            case (_,WdlString(s)) => (jsStringLimited(s), Vector.empty[DXFile])

            // Base case: empty array
            case (_, WdlArray(_, ar)) if ar.length == 0 =>
                (JsArray(Vector.empty), Vector.empty[DXFile])

            // Non empty array
            case (WdlArrayType(t), WdlArray(_, elems)) =>
                val (jsVals, dxFiles) = elems.map(e => jsOfComplexWdlValue(t, e)).unzip
                (JsArray(jsVals.toVector), dxFiles.toVector.flatten)

            // Maps. These are projections from a key to value, where
            // the key and value types are statically known. We
            // represent them in JSON as an array of keys, followed by
            // an array of values.
            case (WdlMapType(keyType, valueType), WdlMap(_, m)) =>
                val keys:WdlValue = WdlArray(WdlArrayType(keyType), m.keys.toVector)
                val (kJs, kFiles) = jsOfComplexWdlValue(keys.wdlType, keys)
                val values:WdlValue = WdlArray(WdlArrayType(valueType), m.values.toVector)
                val (vJs, vFiles) = jsOfComplexWdlValue(values.wdlType, values)
                val jsm = JsObject("keys" -> kJs, "values" -> vJs)
                (jsm, kFiles ++ vFiles)

            case (WdlPairType(lType, rType), WdlPair(l,r)) =>
                val (lJs, lFiles) = jsOfComplexWdlValue(lType, l)
                val (rJs, rFiles) = jsOfComplexWdlValue(rType, r)
                (JsArray(lJs, rJs), lFiles ++ rFiles)

                // TODO
                //case (WdlObjectType, WdlObject())

            case _ => throw new Exception(
                s"Unsupported WDL type ${wdlType.toWdlString} ${wdlValue.toWdlString}"
            )
        }
    }

    // import a WDL value
    def apply(wdlTypeOrg: WdlType, wdlValue: WdlValue) : WdlVarLinks = {
        // Strip optional types
        val wdlType = Utils.stripOptional(wdlTypeOrg)
        if (hasNativeDxType(wdlType)) {
            val (js, _) = jsOfComplexWdlValue(wdlType, wdlValue)
            WdlVarLinks(wdlTypeOrg, DxlJsValue(js))
        } else {
            // Complex values, that may have files in them. For example, ragged file arrays.
            val (jsVal,dxFiles) = jsOfComplexWdlValue(wdlType, wdlValue)
            val buf = jsVal.prettyPrint
            val fileName = wdlType.toWdlString
            val jsSrlFile = Utils.uploadString(buf, fileName)
            WdlVarLinks(wdlTypeOrg, DxlComplexValue(jsSrlFile, dxFiles))
        }
    }

    // Convert an input field to a dx-links structure. This allows
    // passing it to other jobs.
    def apply(wdlType: WdlType, jsValue: JsValue) : WdlVarLinks = {
        if (hasNativeDxType(wdlType)) {
            // This is primitive value, or a single dimensional
            // array of primitive values.
            WdlVarLinks(wdlType, DxlJsValue(jsValue))
        } else {
            // complex types
            val dxfile = dxFileOfJsValue(jsValue)
            val buf = Utils.downloadString(dxfile)
            val jsSrlVal:JsValue = buf.parseJson
            val dxFiles = findDxFiles(jsSrlVal)
            WdlVarLinks(wdlType, DxlComplexValue(jsSrlVal, dxFiles))
        }
    }

    // The reason we need a special method for unpacking an array (or a map),
    // is because we DO NOT want to evaluate the sub-structures. The trouble is
    // files, that may all have the same paths, causing collisions.
    def unpackWdlArray(wvl: WdlVarLinks) : Seq[WdlVarLinks] = {
        val jsRaw = wvl.dxlink match {
            case DxlJsValue(jsn) => jsn
            case DxlComplexValue(jsn, _) => jsn
            case DxlStage(dxStage, _, _) =>
                throw new AppInternalException(s"Values must be unpacked, not dxStage ${dxStage}")
            case DxlJob(dxJob, _, _) =>
                throw new AppInternalException(s"Values must be unpacked, not dxJob ${dxJob}")
        }
        // download and unmarshal file, if needed
        val jsn =
            if (isDxFile(jsRaw) && !hasNativeDxType(wvl.wdlType)) {
                val dxfile = dxFileOfJsValue(jsRaw)
                val buf = Utils.downloadString(dxfile)
                buf.parseJson
            } else {
                jsRaw
            }
        (wvl.wdlType, jsn) match {
            case (WdlArrayType(t), JsArray(l)) =>
                // Array
                l.map(elem => WdlVarLinks(t, DxlComplexValue(elem, findDxFiles(elem))))

            case (WdlMapType(keyType, valueType), _) =>
                // Map. Convert into an array of WDL pairs.
                val (kJs, vJs) = unmarshalJsMap(jsn)
                val kv = kJs zip vJs
                val wdlType = WdlPairType(keyType, valueType)
                kv.map{ case (k, v) =>
                    val js:JsValue = JsArray(Vector(k, v))
                    WdlVarLinks(wdlType, DxlComplexValue(js, Vector.empty[DXFile]))
                }

            case (t,_) =>
                // Error
                throw new AppInternalException(s"Can't unpack ${wvl.wdlType.toWdlString} ${jsn}")
            }
    }

    // This needs more work, there are cases where it modifies file paths.
    def unpackWdlArray_withEval(wvl: WdlVarLinks) : Seq[WdlVarLinks] = {
        val v:WdlValue = eval(wvl, false)
        v match {
            case WdlArray(WdlArrayType(eType), elems) =>
                // import each array element into a WdlVarLinks structure
                elems.map(elem => apply(eType, elem))
            case _ => throw new Exception(s"${wvl} does not unpack to a WDL array")
        }
    }

    // create input/output fields that bind the variable name [bindName] to
    // this WdlVar
    def genFields(wvl : WdlVarLinks, bindName: String) : List[(String, JsonNode)] = {
        val bindEncName = Utils.encodeAppletVarName(Utils.transformVarName(bindName))

        def mkSimple() : (String, JsonNode) = {
            val jsNode : JsonNode = wvl.dxlink match {
                case DxlStage(dxStage, ioRef, varEncName) =>
                    ioRef match {
                        case IORef.Input => dxStage.getInputReference(varEncName)
                        case IORef.Output => dxStage.getOutputReference(varEncName)
                    }
                case DxlJob(dxJob, ioRef, varEncName) =>
                    val jobId : String = dxJob.getId()
                    Utils.jsonNodeOfJsValue(Utils.makeJBOR(jobId, varEncName))
                case DxlJsValue(jsn) => Utils.jsonNodeOfJsValue(jsn)
                case DxlComplexValue(jsn, _) => Utils.jsonNodeOfJsValue(jsn)
            }
            (bindEncName, jsNode)
        }
        def mkComplex() : Map[String,JsonNode] = {
            val bindEncName_F = bindEncName + Utils.FLAT_FILES_SUFFIX
            wvl.dxlink match {
                case DxlStage(dxStage, ioRef, varEncName) =>
                    val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
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
                case DxlJob(dxJob, ioRef, varEncName) =>
                    val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
                    val jobId : String = dxJob.getId()
                    Map(
                        bindEncName -> Utils.jsonNodeOfJsValue(Utils.makeJBOR(jobId, varEncName)),
                        bindEncName_F -> Utils.jsonNodeOfJsValue(Utils.makeJBOR(jobId, varEncName_F))
                    )
                case DxlJsValue(jsn) =>
                    Map(bindEncName -> Utils.jsonNodeOfJsValue(jsn),
                        bindEncName_F -> Utils.jsonNodeOfJsValue(JsArray(Vector.empty[JsValue])))
                case DxlComplexValue(jsn, dxFiles) =>
                    val jsFiles = dxFiles.map(x => Utils.jsValueOfJsonNode(x.getLinkAsJson))
                    Map(bindEncName -> Utils.jsonNodeOfJsValue(jsn),
                        bindEncName_F -> Utils.jsonNodeOfJsValue(JsArray(jsFiles.toVector)))
            }
        }

        val wdlType = Utils.stripOptional(wvl.wdlType)
        if (hasNativeDxType(wdlType)) {
            // Types that are supported natively in DX
            List(mkSimple())
        } else if (!mayHaveFiles(wdlType)) {
            // Complex type that is guarantied to have no files. It can be mapped
            // into a single JSON structure
            List(mkSimple())
        } else {
            // Most general complex type requiring two fields: a JSON structure, and a flat array of files.
            mkComplex().toList
        }
    }


    // Read the job-inputs JSON file, and convert the variables
    // to links that can be passed to other applets.
    def loadJobInputsAsLinks(inputLines : String, closureTypes : Map[String, Option[WdlType]]) :
            Map[String, WdlVarLinks] = {
        // Read the job_inputs.json file. Convert it to a mapping from string to JSON
        // value.
        val jsonAst : JsValue = inputLines.parseJson
        val fields : Map[String, JsValue] = jsonAst.asJsObject.fields

        // Create a mapping from each key to its WDL value,
        // ignore all untyped fields.
        closureTypes.map { case (key,wdlTypeOpt) =>
            wdlTypeOpt match {
                case None => None
                case Some(WdlOptionalType(wType)) =>
                    fields.get(key) match {
                        case None => None
                        case Some(jsValue) =>
                            val wvl = apply(wType, jsValue)
                            Some(key -> wvl)
                    }
                case Some(wType) =>
                    val jsValue = fields(key)
                    val wvl = apply(wType, jsValue)
                    Some(key -> wvl)
            }
        }.flatten.toMap
    }
}
