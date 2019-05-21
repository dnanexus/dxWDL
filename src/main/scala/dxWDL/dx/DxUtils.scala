package dxWDL.dx

import com.dnanexus._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import java.nio.charset.{StandardCharsets}
import spray.json._
import wom.types._

import dxWDL.base.{AppInternalException, Utils, Verbose}
import dxWDL.base.Utils.trace

object DxUtils {
    lazy val dxEnv = DXEnvironment.create()

    def convertToDxObject(objName : String) : Option[DXDataObject] = {
        // If the object is a file-id (or something like it), then
        // shortcut the expensive findDataObjects call.
        if (objName.startsWith("applet-")) {
            return Some(DXApplet.getInstance(objName))
        }
        if (objName.startsWith("file-")) {
            return Some(DXFile.getInstance(objName))
        }
        if (objName.startsWith("record-")) {
            return Some(DXRecord.getInstance(objName))
        }
        if (objName.startsWith("workflow-")) {
            return Some(DXWorkflow.getInstance(objName))
        }
        return None
    }

    // Is this a WDL type that maps to a native DX type?
    def isNativeDxType(wdlType: WomType) : Boolean = {
        wdlType match {
            // optional dx:native types
            case WomOptionalType(WomBooleanType) => true
            case WomOptionalType(WomIntegerType) => true
            case WomOptionalType(WomFloatType) => true
            case WomOptionalType(WomStringType) => true
            case WomOptionalType(WomSingleFileType) => true
            case WomMaybeEmptyArrayType(WomBooleanType) => true
            case WomMaybeEmptyArrayType(WomIntegerType) => true
            case WomMaybeEmptyArrayType(WomFloatType) => true
            case WomMaybeEmptyArrayType(WomStringType) => true
            case WomMaybeEmptyArrayType(WomSingleFileType) => true

                // compulsory dx:native types
            case WomBooleanType => true
            case WomIntegerType => true
            case WomFloatType => true
            case WomStringType => true
            case WomSingleFileType => true
            case WomNonEmptyArrayType(WomBooleanType) => true
            case WomNonEmptyArrayType(WomIntegerType) => true
            case WomNonEmptyArrayType(WomFloatType) => true
            case WomNonEmptyArrayType(WomStringType) => true
            case WomNonEmptyArrayType(WomSingleFileType) => true

            // A tricky, but important case, is `Array[File]+?`. This
            // cannot be converted into a dx file array, unfortunately.
            case _ => false
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

    // Convert from spray-json to jackson JsonNode
    // Used to convert into the JSON datatype used by dxjava
    private val objMapper : ObjectMapper = new ObjectMapper()

    def jsonNodeOfJsValue(jsValue : JsValue) : JsonNode = {
        val s : String = jsValue.prettyPrint
        objMapper.readTree(s)
    }

    // Convert from jackson JsonNode to spray-json
    def jsValueOfJsonNode(jsNode : JsonNode) : JsValue = {
        jsNode.toString().parseJson
    }


    // Create a dx link to a field in an execution. The execution could
    // be a job or an analysis.
    def makeEBOR(dxExec: DXExecution, fieldName: String) : JsValue = {
        if (dxExec.isInstanceOf[DXJob]) {
            JsObject("$dnanexus_link" -> JsObject(
                         "field" -> JsString(fieldName),
                         "job" -> JsString(dxExec.getId)))
        } else if (dxExec.isInstanceOf[DXAnalysis]) {
            JsObject("$dnanexus_link" -> JsObject(
                         "field" -> JsString(fieldName),
                         "analysis" -> JsString(dxExec.getId)))
        } else {
            throw new Exception(s"makeEBOR can't work with ${dxExec.getId}")
        }
    }

    def runSubJob(entryPoint:String,
                  instanceType:Option[String],
                  inputs:JsValue,
                  dependsOn: Vector[DXExecution],
                  verbose: Boolean) : DXJob = {
        val fields = Map(
            "function" -> JsString(entryPoint),
            "input" -> inputs
        )
        val instanceFields = instanceType match {
            case None => Map.empty
            case Some(iType) =>
                Map("systemRequirements" -> JsObject(
                        entryPoint -> JsObject("instanceType" -> JsString(iType))
                    ))
        }
        val dependsFields =
            if (dependsOn.isEmpty) {
                Map.empty
            } else {
                val execIds = dependsOn.map{ dxExec => JsString(dxExec.getId) }.toVector
                Map("dependsOn" -> JsArray(execIds))
            }
        val req = JsObject(fields ++ instanceFields ++ dependsFields)
        Utils.appletLog(verbose, s"subjob request=${req.prettyPrint}")

        val retval: JsonNode = DXAPI.jobNew(jsonNodeOfJsValue(req), classOf[JsonNode])
        val info: JsValue =  jsValueOfJsonNode(retval)
        val id:String = info.asJsObject.fields.get("id") match {
            case Some(JsString(x)) => x
            case _ => throw new AppInternalException(
                s"Bad format returned from jobNew ${info.prettyPrint}")
        }
        DXJob.getInstance(id)
    }

    // describe a project, and extract fields that not currently available
    // through dxjava.
    def projectDescribeExtraInfo(dxProject: DXProject) : (String,String) = {
        val rep = DXAPI.projectDescribe(dxProject.getId(), classOf[JsonNode])
        val jso:JsObject = jsValueOfJsonNode(rep).asJsObject

        val billTo = jso.fields.get("billTo") match {
            case Some(JsString(x)) => x
            case _ => throw new Exception(
                s"Failed to get billTo from project ${dxProject.getId()}")
        }
        val region = jso.fields.get("region") match {
            case Some(JsString(x)) => x
            case _ => throw new Exception(
                s"Failed to get region from project ${dxProject.getId()}")
        }
        (billTo,region)
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
    def dxFileFromJsValue(jsValue : JsValue) : DXFile = {
        val innerObj = jsValue match {
            case JsObject(fields) =>
                fields.get("$dnanexus_link") match {
                    case None => throw new AppInternalException(s"Non-dxfile json $jsValue")
                    case Some(x) => x
                }
            case  _ =>
                throw new AppInternalException(s"Non-dxfile json $jsValue")
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
                        case _ => throw new AppInternalException(s"No file ID found in $jsValue")
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

    def dxFileToJsValue(dxFile: DXFile) : JsValue = {
        jsValueOfJsonNode(dxFile.getLinkAsJson)
    }

    // copy asset to local project, if it isn't already here.
    def cloneAsset(assetRecord: DXRecord,
                   dxProject: DXProject,
                   pkgName: String,
                   rmtProject: DXProject,
                   verbose: Verbose) : Unit = {
        if (dxProject == rmtProject) {
            trace(verbose.on, s"The asset ${pkgName} is from this project ${rmtProject.getId}, no need to clone")
            return
        }
        trace(verbose.on, s"The asset ${pkgName} is from a different project ${rmtProject.getId}")

        // clone
        val req = JsObject( "objects" -> JsArray(JsString(assetRecord.getId)),
                            "project" -> JsString(dxProject.getId),
                            "destination" -> JsString("/"))
        val rep = DXAPI.projectClone(rmtProject.getId,
                                     jsonNodeOfJsValue(req),
                                     classOf[JsonNode])
        val repJs:JsValue = jsValueOfJsonNode(rep)

        val exists = repJs.asJsObject.fields.get("exists") match {
            case None => throw new Exception("API call did not returnd an exists field")
            case Some(JsArray(x)) => x.map {
                case JsString(id) => id
                case _ => throw new Exception("bad type, not a string")
            }.toVector
            case other => throw new Exception(s"API call returned invalid exists field")
        }
        val existingRecords = exists.filter(_.startsWith("record-"))
        existingRecords.size match {
            case 0 =>
                val localAssetRecord = DXRecord.getInstance(assetRecord.getId)
                trace(verbose.on, s"Created ${localAssetRecord.getId} pointing to asset ${pkgName}")
            case 1 =>
                trace(verbose.on, s"The project already has a record pointing to asset ${pkgName}")
            case _ =>
                throw new Exception(s"clone returned too many existing records ${exists}")
        }
    }


    // Download platform file contents directly into an in-memory string.
    // This makes sense for small files.
    def downloadString(dxfile: DXFile) : String = {
        val bytes = dxfile.downloadBytes()
        new String(bytes, StandardCharsets.UTF_8)
    }
}
