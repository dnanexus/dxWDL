package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXFile, DXJob, DXProject, DXWorkflow}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.ListBuffer
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import Utils.{MAX_STRING_LEN}
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wdl4s.types._
import wdl4s.values._

object IORef extends Enumeration {
    val Input, Output = Value
}

// A union of all the different ways of building a value
// from JSON passed by the platform
sealed trait DxLink
case class DxlJsonNode(jsn: JsonNode) extends DxLink
case class DxlJsValue(jsn : JsValue) extends DxLink
case class DxlStage(dxStage : DXWorkflow.Stage) extends DxLink
case class DxlJob(dxJob : DXJob) extends DxLink

case class WdlVarLinks(varName: String,
                       wdlType: WdlType,
                       dxlink: Option[(IORef.Value, DxLink)])

object WdlVarLinks {
    // Parse a dnanexus file descriptor. Examples:
    //
    // "$dnanexus_link": {
    //    "project": "project-BKJfY1j0b06Z4y8PX8bQ094f",
    //    "id": "file-BKQGkgQ0b06xG5560GGQ001B"
    //   }
    //
    //  {"$dnanexus_link": "file-F0J6JbQ0ZvgVz1J9q5qKfkqP"}
    //
    def dxFileOfJsValue(jsValue : JsValue) : DXFile = {
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

    // create input/output fields that bind the variable name [bindName] to
    // this WdlVar
    def genFields(wvl : WdlVarLinks, bindName: String) : List[(String, JsonNode)] = {
        val (ioRef,dxVal) = wvl.dxlink match {
            case None => throw new AppInternalException("Empty links WdlVarLinks")
            case Some((x,ioRef)) => (x,ioRef)
        }
        val varEncName = Utils.encodeAppletVarName(wvl.varName)
        val bindEncName = Utils.encodeAppletVarName(bindName)

        def mkPrimitive() : (String, JsonNode) = {
            val jsNode : JsonNode = dxVal match {
                case DxlStage(dxStage) =>
                    ioRef match {
                        case IORef.Input => dxStage.getInputReference(varEncName)
                        case IORef.Output => dxStage.getOutputReference(varEncName)
                    }
                case DxlJob(dxJob) =>
                    val jobId : String = dxJob.getId()
                    Utils.jsonNodeOfJsValue(Utils.makeJBOR(jobId, varEncName))
                case DxlJsonNode(jsn) => jsn
                case DxlJsValue(jsn) => Utils.jsonNodeOfJsValue(jsn)
                case t => throw new AppInternalException(s"Unsupported type ${t}")
            }
            (bindEncName, jsNode)
        }
        def mkRaggedArray() : (String, JsonNode) = {
            val jsNode : JsonNode = dxVal match {
                case DxlStage(dxStage) =>
                    ioRef match {
                        case IORef.Input =>
                            dxStage.getInputReference(varEncName)
                        case IORef.Output =>
                            dxStage.getOutputReference(varEncName)
                    }
                case DxlJob(dxJob) =>
                    val jobId : String = dxJob.getId()
                    Utils.jsonNodeOfJsValue(Utils.makeJBOR(jobId, varEncName))
                case DxlJsonNode(jsn) => jsn
                case DxlJsValue(jsn) => Utils.jsonNodeOfJsValue(jsn)
                case t => throw new AppInternalException(s"Unsupported type ${t}")
            }
            (bindEncName, jsNode)
        }

        // strip the optional attribute
        val wdlType = wvl.wdlType match {
            case WdlOptionalType(t) => t
            case t => t
        }
        wdlType match {
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType| WdlFileType |
                    WdlArrayType(WdlBooleanType) |
                    WdlArrayType(WdlIntegerType) |
                    WdlArrayType(WdlFloatType) |
                    WdlArrayType(WdlStringType) |
                    WdlArrayType(WdlFileType) =>
                List(mkPrimitive())
            case WdlArrayType(WdlArrayType(WdlBooleanType)) |
                    WdlArrayType(WdlArrayType(WdlIntegerType)) |
                    WdlArrayType(WdlArrayType(WdlFloatType)) |
                    WdlArrayType(WdlArrayType(WdlStringType)) =>
                List(mkRaggedArray())
            case WdlArrayType(WdlArrayType((WdlFileType)))=>
                throw new AppInternalException("Ragged file arrays are not supported yet")
            case t =>
                throw new AppInternalException(s"Unsupported wdlType ${t.toWdlString}")
        }
    }


    // Convert an input field to a dx-links structure. This allows
    // passing it to other jobs.
    def ofInputField(fieldName: String,
                     wdlType: WdlType,
                     jsValue: JsValue) : WdlVarLinks = {
        val (prefix, suffix) = Utils.appletVarNameSplit(fieldName)
        suffix match {
            case Utils.FLAT_FILE_ARRAY_SUFFIX =>
                throw new NotImplementedError(s"Suffix ${suffix} is not supported yet")
            case _ => ()
        }
        wdlType match {
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType | WdlFileType
                   | WdlArrayType(WdlBooleanType)
                   | WdlArrayType(WdlIntegerType)
                   | WdlArrayType(WdlFloatType)
                   | WdlArrayType(WdlStringType)
                   | WdlArrayType(WdlFileType) =>
                // This is primitive value, or a single dimensional
                // array of primitive values.
                WdlVarLinks(prefix, wdlType, Some(IORef.Input, DxlJsValue(jsValue)))

            case WdlArrayType(WdlArrayType(WdlBooleanType))
                   | WdlArrayType(WdlArrayType(WdlIntegerType))
                   | WdlArrayType(WdlArrayType(WdlFloatType))
                   | WdlArrayType(WdlArrayType(WdlStringType)) =>
                // ragged array. Download the file holding the data,
                // and unmarshal it.
                val dxfile = dxFileOfJsValue(jsValue)
                val buf = Utils.downloadString(dxfile)
                val raggedAr: JsValue = buf.parseJson
                WdlVarLinks(prefix, wdlType, Some(IORef.Input, DxlJsValue(raggedAr)))

            case _ =>
                throw new Exception(s"Unsupported wdlType ${wdlType}")
        }
    }

    // convert a WDL value to json, for primitive values, and arrays of primitives
    def jsOfBasicWdlValue(wdlType: WdlType, wdlValue : WdlValue) : JsValue = {
        def jsStringLimited(buf: String) : JsValue = {
            if (buf.length > MAX_STRING_LEN)
                throw new AppInternalException(s"string is longer than ${MAX_STRING_LEN}")
            JsString(buf)
        }

        (wdlType, wdlValue) match {
            // primitive types
            case (WdlFileType, WdlString(path)) => Utils.uploadFile(Paths.get(path))
            case (WdlFileType, WdlSingleFile(path)) => Utils.uploadFile(Paths.get(path))
            case (_,WdlBoolean(b)) => JsBoolean(b)
            case (_,WdlInteger(n)) => JsNumber(n)
            case (_,WdlFloat(x)) => JsNumber(x)
            case (_,WdlString(s)) => jsStringLimited(s)

            // uni-dimensional array types
            case (WdlArrayType(WdlFileType), WdlArray(WdlArrayType(WdlStringType), fileAr)) =>
                JsArray(fileAr.map {case x : WdlString =>
                            val path = x.value
                            Utils.uploadFile(Paths.get(path))
                        }.toList)
            case (WdlArrayType(WdlFileType), WdlArray(WdlArrayType(WdlFileType), fileAr)) =>
                JsArray(fileAr.map {case x : WdlSingleFile =>
                            val path = x.value
                            Utils.uploadFile(Paths.get(path))
                        }.toList)
            case (_, WdlArray(WdlArrayType(WdlBooleanType), boolAr)) =>
                JsArray(boolAr.map {case x : WdlBoolean => JsBoolean(x.value)}.toList)
            case (_, WdlArray(WdlArrayType(WdlIntegerType), intAr)) =>
                JsArray(intAr.map {case x : WdlInteger => JsNumber(x.value)}.toList)
            case (_, WdlArray(WdlArrayType(WdlFloatType), fAr)) =>
                JsArray(fAr.map {case x : WdlFloat => JsNumber(x.value)}.toList)
            case (_, WdlArray(WdlArrayType(WdlStringType), stAr)) =>
                JsArray(stAr.map {case x : WdlString => jsStringLimited(x.value)}.toList)

            case _ => throw new AppInternalException(s"Unsupport type ${wdlValue.wdlType}")
        }
    }

    // import a WDL value
    def outputFieldOfWdlValue(varName: String, wdlType: WdlType, wdlValue: WdlValue) : WdlVarLinks = {
        val jsValue: JsValue = wdlType match {
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType
                   | WdlFileType
                   | WdlArrayType(WdlBooleanType)
                   | WdlArrayType(WdlIntegerType)
                   | WdlArrayType(WdlFloatType)
                   | WdlArrayType(WdlStringType)
                   | WdlArrayType(WdlFileType) => jsOfBasicWdlValue(wdlType, wdlValue)

            // ragged arrays
            // convert to JSON, and marshal into a string, and
            // upload as a file. Return a file ID.
            case WdlArrayType(WdlArrayType(t)) =>
                t match {
                    case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType => ()
                    case _ => throw new AppInternalException(s"unsupported type ${wdlType}")
                }
                val raggedAr: JsValue = wdlValue match {
                    case WdlArray(_,l) => JsArray(l.toList.map{ case x =>
                                                      jsOfBasicWdlValue(WdlArrayType(t), x) })
                    case _ => throw new AppInternalException("Sanity")
                }
                val buf = raggedAr.prettyPrint
                Utils.uploadString(buf)

            case _ =>
                throw new AppInternalException(s"Type ${wdlType} unsupported")
         }

        WdlVarLinks(varName, wdlType, Some(IORef.Output, DxlJsValue(jsValue)))
    }


    // Open a WDL array into a sequence of elements. For example:
    // WdlArray(1, 2, 3) =>
    //     List(WdlInteger(1), WdlInteger(2), WdlInteger(3))
    // Each of the WDL values is represented as a WdlVarLinks structure.
    //
    // Note: the Wdl value must be an array.
    def unpackWdlArray(wvl: WdlVarLinks) : Seq[WdlVarLinks] = {
        val (ioRef,dxl) = wvl.dxlink match {
            case Some((ioRef,dxl)) => (ioRef,dxl)
            case None => throw new AppInternalException ("Empty dxlink field")
        }
        val elemType : WdlType = wvl.wdlType match  {
            case WdlArrayType(x) => x
            case t => throw new AppInternalException (s"${t} is not a wdl array type")
        }

        val l : List[JsValue] = dxl match {
            case DxlJsonNode(jsn) =>
                Utils.jsValueOfJsonNode(jsn) match {
                    case JsArray(l) => l.toList
                    case t => throw new AppInternalException(s"Wrong type ${t} for json array")
                }
            case DxlJsValue(jsn) =>
                jsn match {
                    case JsArray(l) => l.toList
                    case t => throw new AppInternalException(s"Wrong type ${t} for json array")
                }
            case DxlStage(dxStage) =>
                throw new AppInternalException(s"Values must be unpacked, not dxStage ${dxStage}")
            case DxlJob(dxJob) =>
                throw new AppInternalException(s"Values must be unpacked, not dxJob ${dxJob}")
        }

        l.map(elem => WdlVarLinks(wvl.varName, elemType, Some(ioRef, DxlJsValue(elem))))
    }

    def basicWdlValueOfJsValue(wdlType: WdlType, jsValue: JsValue) : WdlValue = {
        def wdlFileOfDxLink(jsValue : JsValue) : WdlValue = {
            // Download the file, and
            // place it in a local file, with the same name as the
            // platform. All files have to be downloaded into the same
            // directory; the only exception we make is for disambiguatio
            // purposes.
            val dxfile = dxFileOfJsValue(jsValue)
            val fName = dxfile.describe().getName()
            val shortPath = Utils.inputFilesDirPath.resolve(fName)
            val path : Path =
                if (Files.exists(shortPath)) {
                    // Short path already exists. Note: this check is brittle in the case
                    // of concurrent downloads.
                    val fid = dxfile.getId()
                    System.err.println(s"Disambiguating file ${fid} with name ${fName}")
                    val dir = Utils.inputFilesDirPath.resolve(fid).toFile
                    assert(dir.mkdir())
                    Utils.inputFilesDirPath.resolve(fid).resolve(fName)
                } else {
                    shortPath
                }
            Utils.downloadFile(path, dxfile)
            WdlSingleFile(path.toString)
        }
        (wdlType, jsValue)  match {
            case (WdlBooleanType, JsBoolean(b)) => WdlBoolean(b.booleanValue)
            case (WdlIntegerType, JsNumber(bnm)) => WdlInteger(bnm.intValue)
            case (WdlFloatType, JsNumber(bnm)) => WdlFloat(bnm.doubleValue)
            case (WdlStringType, JsString(s)) => WdlString(s)
            case (WdlFileType, _) => wdlFileOfDxLink(jsValue)

            // One dimensional arrays
            case (WdlArrayType(WdlBooleanType), JsArray(ba)) =>
                WdlArray(WdlArrayType(WdlBooleanType),
                         ba.map {
                             case JsBoolean(b) => WdlBoolean(b)
                             case _ => throw new AppInternalException("Expected JSON boolean")
                         })
            case (WdlArrayType(WdlIntegerType), JsArray(ia)) =>
                WdlArray(WdlArrayType(WdlIntegerType),
                         ia.map {
                             case JsNumber(bnm) => WdlInteger(bnm.intValue)
                             case _ => throw new AppInternalException("Expected JSON boolean")
                         })
            case (WdlArrayType(WdlFloatType), JsArray(fa)) =>
                WdlArray(WdlArrayType(WdlFloatType),
                         fa.map {
                             case JsNumber(bnm) => WdlFloat(bnm.doubleValue)
                             case _ => throw new AppInternalException("Expected JSON big-number")
                         })
            case (WdlArrayType(WdlStringType), JsArray(sa)) =>
                WdlArray(WdlArrayType(WdlStringType),
                         sa.map {
                             case JsString(s) => WdlString(s)
                             case _ => throw new AppInternalException("Expected JSON string")
                         })
            case (WdlArrayType(WdlFileType), JsArray(fa)) =>
                WdlArray(WdlArrayType(WdlFileType),
                         fa.map(x => wdlFileOfDxLink(x)))

            case _ => throw new AppInternalException("Unsupported type ${wvl.wdlType}")
        }
    }

    // Create a WdlValue from the dx-links structure.
    def wdlValueOfInputField(wvl: WdlVarLinks) : WdlValue = {
        val dxl = wvl.dxlink match {
            case Some((_,dxl)) => dxl
            case None => throw new AppInternalException ("Empty dxlink field")
        }
        val jsValue: JsValue = dxl match {
            case DxlJsonNode(jsn) => Utils.jsValueOfJsonNode(jsn)
            case DxlJsValue(jsn) => jsn
            case _ =>
                throw new AppInternalException(s"Unsupported conversion from ${dxl} to WdlValue")
        }

        wvl.wdlType match {
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType
                   | WdlFileType
                   | WdlArrayType(WdlBooleanType)
                   | WdlArrayType(WdlIntegerType)
                   | WdlArrayType(WdlFloatType)
                   | WdlArrayType(WdlStringType)
                   | WdlArrayType(WdlFileType) => basicWdlValueOfJsValue(wvl.wdlType, jsValue)

            // ragged arrays, we already downloaded the file, now we need
            // to make sense of the JSON data
            case WdlArrayType(WdlArrayType(t)) =>
                t match {
                    case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType =>
                        val l: List[JsValue] = jsValue match {
                            case JsArray(l) => l.toList
                            case _ => throw new AppInternalException(
                                "WDL ragged array not encoded as json array")
                        }
                        val wl: List[WdlValue] = l.map(jsElem =>
                            basicWdlValueOfJsValue( WdlArrayType(t), jsElem))
                        WdlArray(WdlArrayType(WdlArrayType(t)), wl)

                    case _ => throw new AppInternalException(s"unsupported type ${wvl.wdlType}")
                }

            case _ =>
                throw new AppInternalException(s"Type ${wvl.wdlType} unsupported")
        }
    }

    // Read the job-inputs JSON file, and convert the variables
    // to links that can be passed to other applets.
    def loadJobInputsAsLinks(inputLines : String, closureTypes : Map[String, Option[WdlType]]) :
            Map[String, WdlVarLinks] = {
        // Read the job_inputs.json file. Convert it to a mapping from string to JSON
        // value.
        val jsonAst : JsValue = inputLines.parseJson
        var fields : Map[String, JsValue] = jsonAst.asJsObject.fields

        // Create a mapping from each key to its WDL value,
        // ignore all untyped fields.
        closureTypes.map { case (key,wdlTypeOpt) =>
            wdlTypeOpt match {
                case None => None
                case Some(WdlOptionalType(wType)) =>
                    fields.get(key) match {
                        case None => None
                        case Some(jsValue) =>
                            val wvl = ofInputField(key, wType, jsValue)
                            Some(wvl.varName -> wvl)
                    }
                case Some(wType) =>
                    val jsValue = fields(key)
                    val wvl = ofInputField(key, wType, jsValue)
                    Some(wvl.varName -> wvl)
            }
        }.flatten.toMap
    }

    // Read the job-inputs JSON file, and convert the variables
    // to WDL values.
    def loadJobInputs(inputLines : String, closureTypes : Map[String, Option[WdlType]]) :
            Map[String, WdlValue] = {
        // Load in a lazy fashion, without downloading files
        val m: Map[String, WdlVarLinks] = loadJobInputsAsLinks(inputLines, closureTypes)

        // convert to WDL values
        m.map{ case (key, wvl) =>
            key -> wdlValueOfInputField(wvl)
        }.toMap
    }
}
