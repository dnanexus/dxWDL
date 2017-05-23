package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXFile, DXJob, DXProject, DXWorkflow}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files, Path, Paths}
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
case class DxlJsValue(jsn: JsValue) extends DxLink
case class DxlStage(dxStage: DXWorkflow.Stage, ioRef: IORef.Value, varName: String) extends DxLink
case class DxlJob(dxJob: DXJob, ioRef: IORef.Value, varName: String) extends DxLink

case class WdlVarLinks(wdlType: WdlType, dxlink: DxLink)

// Bridge between WDL values and DNAx values.
case class BValue(wvl: WdlVarLinks, wdlValue: WdlValue)

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
        WdlSingleFile(path.toString)
    }

    private def evalPrimitive(wdlType: WdlType, jsValue: JsValue, force: Boolean) : WdlValue = {
        (wdlType, jsValue)  match {
            case (WdlBooleanType, JsBoolean(b)) => WdlBoolean(b.booleanValue)
            case (WdlIntegerType, JsNumber(bnm)) => WdlInteger(bnm.intValue)
            case (WdlFloatType, JsNumber(bnm)) => WdlFloat(bnm.doubleValue)
            case (WdlStringType, JsString(s)) => WdlString(s)
            case (WdlFileType, _) => wdlFileOfDxLink(jsValue, force)

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
                         fa.map(x => wdlFileOfDxLink(x, force)))

            case _ => throw new AppInternalException("Unsupported type ${wdlType}")
        }
    }

    // Calculate a WdlValue from the dx-links structure. If [force] is true,
    // any files included in the structure will be downloaded.
    def eval(wvl: WdlVarLinks, force: Boolean) : WdlValue = {
        val jsValue: JsValue = wvl.dxlink match {
            case DxlJsValue(jsn) => jsn
            case _ =>
                throw new AppInternalException(s"Unsupported conversion from ${wvl.dxlink} to WdlValue")
        }

        wvl.wdlType match {
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType
                   | WdlFileType
                   | WdlArrayType(WdlBooleanType)
                   | WdlArrayType(WdlIntegerType)
                   | WdlArrayType(WdlFloatType)
                   | WdlArrayType(WdlStringType)
                   | WdlArrayType(WdlFileType) => evalPrimitive(wvl.wdlType, jsValue, force)

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
                            evalPrimitive(WdlArrayType(t), jsElem, force)
                        )
                        WdlArray(WdlArrayType(WdlArrayType(t)), wl)

                    case _ => throw new AppInternalException(s"unsupported type ${wvl.wdlType}")
                }

            case _ =>
                throw new AppInternalException(s"Type ${wvl.wdlType} unsupported")
        }
    }

    // convert a WDL value to json, for primitive values, and arrays of primitives
    private def jsOfBasicWdlValue(wdlType: WdlType, wdlValue : WdlValue) : JsValue = {
        def jsStringLimited(buf: String) : JsValue = {
            if (buf.length > MAX_STRING_LEN)
                throw new AppInternalException(s"string is longer than ${MAX_STRING_LEN}")
            JsString(buf)
        }

        (wdlType, wdlValue) match {
            // primitive types
            case (WdlFileType, WdlString(path)) => Utils.uploadFile(Paths.get(path))
            case (WdlFileType, WdlSingleFile(path)) => Utils.uploadFile(Paths.get(path))
            case (WdlStringType, WdlSingleFile(path)) => JsString(path)
            case (_,WdlBoolean(b)) => JsBoolean(b)
            case (_,WdlInteger(n)) => JsNumber(n)
            case (_,WdlFloat(x)) => JsNumber(x)
            case (_,WdlString(s)) => jsStringLimited(s)

            // uni-dimensional array types
            case (WdlArrayType(WdlFileType), WdlArray(WdlArrayType(WdlStringType), fileAr)) =>
                JsArray(fileAr.map {case x : WdlString =>
                            val path = x.value
                            Utils.uploadFile(Paths.get(path))
                        }.toVector)
            case (WdlArrayType(WdlFileType), WdlArray(WdlArrayType(WdlFileType), fileAr)) =>
                JsArray(fileAr.map {case x : WdlSingleFile =>
                            val path = x.value
                            Utils.uploadFile(Paths.get(path))
                        }.toVector)
            case (_, WdlArray(WdlArrayType(WdlBooleanType), boolAr)) =>
                JsArray(boolAr.map {case x : WdlBoolean => JsBoolean(x.value)}.toVector)
            case (_, WdlArray(WdlArrayType(WdlIntegerType), intAr)) =>
                JsArray(intAr.map {case x : WdlInteger => JsNumber(x.value)}.toVector)
            case (_, WdlArray(WdlArrayType(WdlFloatType), fAr)) =>
                JsArray(fAr.map {case x : WdlFloat => JsNumber(x.value)}.toVector)
            case (_, WdlArray(WdlArrayType(WdlStringType), stAr)) =>
                JsArray(stAr.map {case x : WdlString => jsStringLimited(x.value)}.toVector)
            case (_, WdlArray(_, ar)) if ar.length == 0 =>
                // An empty array, the type doesn't matter here
                JsArray(Vector.empty)

            case _ => throw new AppInternalException(
                s"Unsupported type combination (${wdlType}, ${wdlValue.wdlType})")
        }
    }

    // import a WDL value
    def apply(wdlTypeOrg: WdlType, wdlValue: WdlValue) : WdlVarLinks = {
        // Strip optional types
        val wdlType = wdlTypeOrg match {
            case WdlOptionalType(t) => t
            case t => t
        }
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
                    case WdlArray(_,l) => JsArray(l.toVector.map{ case x =>
                                                      jsOfBasicWdlValue(WdlArrayType(t), x) })
                    case _ => throw new AppInternalException("Sanity")
                }
                val buf = raggedAr.prettyPrint
                Utils.uploadString(buf, Utils.sanitize(wdlType.toWdlString))

            case _ =>
                throw new AppInternalException(s"Type ${wdlType} unsupported")
        }
        WdlVarLinks(wdlTypeOrg, DxlJsValue(jsValue))
    }

    // Convert an input field to a dx-links structure. This allows
    // passing it to other jobs.
    def apply(wdlType: WdlType, jsValue: JsValue) : WdlVarLinks = {
        wdlType match {
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType | WdlFileType
                   | WdlArrayType(WdlBooleanType)
                   | WdlArrayType(WdlIntegerType)
                   | WdlArrayType(WdlFloatType)
                   | WdlArrayType(WdlStringType)
                   | WdlArrayType(WdlFileType) =>
                // This is primitive value, or a single dimensional
                // array of primitive values.
                WdlVarLinks(wdlType, DxlJsValue(jsValue))

            case WdlArrayType(WdlArrayType(WdlBooleanType))
                   | WdlArrayType(WdlArrayType(WdlIntegerType))
                   | WdlArrayType(WdlArrayType(WdlFloatType))
                   | WdlArrayType(WdlArrayType(WdlStringType)) =>
                // ragged array. Download the file holding the data,
                // and unmarshal it.
                val dxfile = dxFileOfJsValue(jsValue)
                val buf = Utils.downloadString(dxfile)
                val raggedAr: JsValue = buf.parseJson
                WdlVarLinks(wdlType, DxlJsValue(raggedAr))

            case _ =>
                throw new Exception(s"Unsupported wdlType ${wdlType}")
        }
    }

    // Open a WDL array into a sequence of elements. For example:
    // WdlArray(1, 2, 3) =>
    //     List(WdlInteger(1), WdlInteger(2), WdlInteger(3))
    // Each of the WDL values is represented as a WdlVarLinks structure.
    //
    // Note: the Wdl value must be an array.
    def unpackWdlArray(wvl: WdlVarLinks) : Seq[WdlVarLinks] = {
        val elemType : WdlType = wvl.wdlType match  {
            case WdlArrayType(x) => x
            case t => throw new AppInternalException (s"${t} is not a wdl array type")
        }

        val l : List[JsValue] = wvl.dxlink match {
            case DxlJsValue(jsn) =>
                jsn match {
                    case JsArray(l) => l.toList
                    case t => throw new AppInternalException(s"Wrong type ${t} for json array")
                }
            case DxlStage(dxStage, _, _) =>
                throw new AppInternalException(s"Values must be unpacked, not dxStage ${dxStage}")
            case DxlJob(dxJob, _, _) =>
                throw new AppInternalException(s"Values must be unpacked, not dxJob ${dxJob}")
        }

        l.map(elem => WdlVarLinks(elemType, DxlJsValue(elem)))
    }

    // create input/output fields that bind the variable name [bindName] to
    // this WdlVar
    def genFields(wvl : WdlVarLinks, bindName: String) : List[(String, JsonNode)] = {
        val bindEncName = Utils.encodeAppletVarName(Utils.transformVarName(bindName))

        def mkPrimitive() : (String, JsonNode) = {
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
                case t => throw new AppInternalException(s"Unsupported type ${t}")
            }
            (bindEncName, jsNode)
        }
        def mkRaggedArray() : (String, JsonNode) = {
            val jsNode : JsonNode = wvl.dxlink match {
                case DxlStage(dxStage, ioRef, varEncName) =>
                    ioRef match {
                        case IORef.Input =>
                            dxStage.getInputReference(varEncName)
                        case IORef.Output =>
                            dxStage.getOutputReference(varEncName)
                    }
                case DxlJob(dxJob, ioRef, varEncName) =>
                    val jobId : String = dxJob.getId()
                    Utils.jsonNodeOfJsValue(Utils.makeJBOR(jobId, varEncName))
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
