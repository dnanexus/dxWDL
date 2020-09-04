package dxWDL.dx

import com.dnanexus.{DXEnvironment, DXAPI}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Path, Files}
import spray.json._
import wom.types._

import dxWDL.base.{AppInternalException, Utils, Verbose}

object DxUtils {
  private val DOWNLOAD_RETRY_LIMIT = 3
  private val UPLOAD_RETRY_LIMIT = 3

  lazy val dxEnv = DXEnvironment.create()
  lazy val dxCrntProject = DxProject(dxEnv.getProjectContext())

  def isDxId(objName: String): Boolean = {
    objName match {
      case _ if objName.startsWith("applet-")   => true
      case _ if objName.startsWith("file-")     => true
      case _ if objName.startsWith("record-")   => true
      case _ if objName.startsWith("workflow-") => true
      case _                                    => false
    }
  }

  def dxDataObjectToURL(dxObj: DxDataObject): String = {
    s"${Utils.DX_URL_PREFIX}${dxObj.id}"
  }

  // Is this a WDL type that maps to a native DX type?
  def isNativeDxType(wdlType: WomType): Boolean = {
    wdlType match {
      // optional dx:native types
      case WomOptionalType(WomBooleanType)           => true
      case WomOptionalType(WomIntegerType)           => true
      case WomOptionalType(WomFloatType)             => true
      case WomOptionalType(WomStringType)            => true
      case WomOptionalType(WomSingleFileType)        => true
      case WomMaybeEmptyArrayType(WomBooleanType)    => true
      case WomMaybeEmptyArrayType(WomIntegerType)    => true
      case WomMaybeEmptyArrayType(WomFloatType)      => true
      case WomMaybeEmptyArrayType(WomStringType)     => true
      case WomMaybeEmptyArrayType(WomSingleFileType) => true

      // compulsory dx:native types
      case WomBooleanType                          => true
      case WomIntegerType                          => true
      case WomFloatType                            => true
      case WomStringType                           => true
      case WomSingleFileType                       => true
      case WomNonEmptyArrayType(WomBooleanType)    => true
      case WomNonEmptyArrayType(WomIntegerType)    => true
      case WomNonEmptyArrayType(WomFloatType)      => true
      case WomNonEmptyArrayType(WomStringType)     => true
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
              case _                                          => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  // Search through a JSON value for all the dx:file links inside it. Returns
  // those as a vector.
  def findDxFiles(jsValue: JsValue): Vector[DxFile] = {
    jsValue match {
      case JsBoolean(_) | JsNumber(_) | JsString(_) | JsNull =>
        Vector.empty[DxFile]
      case JsObject(_) if isDxFile(jsValue) =>
        Vector(dxFileFromJsValue(jsValue))
      case JsObject(fields) =>
        fields.map { case (_, v) => findDxFiles(v) }.toVector.flatten
      case JsArray(elems) =>
        elems.map(e => findDxFiles(e)).flatten
    }
  }

  // Convert from spray-json to jackson JsonNode
  // Used to convert into the JSON datatype used by dxjava
  private val objMapper: ObjectMapper = new ObjectMapper()

  def jsonNodeOfJsValue(jsValue: JsValue): JsonNode = {
    val s: String = jsValue.prettyPrint
    objMapper.readTree(s)
  }

  // Convert from jackson JsonNode to spray-json
  def jsValueOfJsonNode(jsNode: JsonNode): JsValue = {
    jsNode.toString().parseJson
  }

  // Create a dx link to a field in an execution. The execution could
  // be a job or an analysis.
  def makeEBOR(dxExec: DxExecution, fieldName: String): JsValue = {
    if (dxExec.isInstanceOf[DxJob]) {
      JsObject(
          "$dnanexus_link" -> JsObject("field" -> JsString(fieldName), "job" -> JsString(dxExec.id))
      )
    } else if (dxExec.isInstanceOf[DxAnalysis]) {
      JsObject(
          "$dnanexus_link" -> JsObject("field" -> JsString(fieldName),
                                       "analysis" -> JsString(dxExec.id))
      )
    } else {
      throw new Exception(s"makeEBOR can't work with ${dxExec.id}")
    }
  }

  def runSubJob(entryPoint: String,
                instanceType: Option[String],
                inputs: JsValue,
                dependsOn: Vector[DxExecution],
                delayWorkspaceDestruction: Option[Boolean],
                verbose: Boolean,
                name: Option[String] = None,
                details: Option[JsValue] = None): DxJob = {
    val fields = Map(
        "function" -> JsString(entryPoint),
        "input" -> inputs
    )
    val instanceFields = instanceType match {
      case None => Map.empty
      case Some(iType) =>
        Map(
            "systemRequirements" -> JsObject(
                entryPoint -> JsObject("instanceType" -> JsString(iType))
            )
        )
    }
    val dependsFields =
      if (dependsOn.isEmpty) {
        Map.empty
      } else {
        val execIds = dependsOn.map { dxExec =>
          JsString(dxExec.id)
        }.toVector
        Map("dependsOn" -> JsArray(execIds))
      }
    val dwdFields = delayWorkspaceDestruction match {
      case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
      case _          => Map.empty
    }
    val nameFields = name match {
      case Some(n) => Map("name" -> JsString(n))
      case None    => Map.empty
    }
    val detailsField = details match {
      case Some(d) => Map("details" -> d)
      case None    => Map.empty
    }
    val req = JsObject(
        fields ++ instanceFields ++ dependsFields ++ dwdFields ++ nameFields ++ detailsField
    )
    Utils.appletLog(verbose, s"subjob request=${req.prettyPrint}")

    val retval: JsonNode = DXAPI.jobNew(jsonNodeOfJsValue(req), classOf[JsonNode])
    val info: JsValue = jsValueOfJsonNode(retval)
    val id: String = info.asJsObject.fields.get("id") match {
      case Some(JsString(x)) => x
      case _ =>
        throw new AppInternalException(s"Bad format returned from jobNew ${info.prettyPrint}")
    }
    DxJob(id)
  }

  // describe a project, and extract fields that not currently available
  // through dxjava.
  def projectDescribeExtraInfo(dxProject: DxProject): (String, String) = {
    val rep = DXAPI.projectDescribe(dxProject.id, classOf[JsonNode])
    val jso: JsObject = jsValueOfJsonNode(rep).asJsObject

    val billTo = jso.fields.get("billTo") match {
      case Some(JsString(x)) => x
      case _                 => throw new Exception(s"Failed to get billTo from project ${dxProject.id}")
    }
    val region = jso.fields.get("region") match {
      case Some(JsString(x)) => x
      case _                 => throw new Exception(s"Failed to get region from project ${dxProject.id}")
    }
    (billTo, region)
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
  def dxFileFromJsValue(jsValue: JsValue): DxFile = {
    val innerObj = jsValue match {
      case JsObject(fields) =>
        fields.get("$dnanexus_link") match {
          case None    => throw new AppInternalException(s"Non-dxfile json $jsValue")
          case Some(x) => x
        }
      case _ =>
        throw new AppInternalException(s"Non-dxfile json $jsValue")
    }

    val (fid, projId): (String, Option[String]) = innerObj match {
      case JsString(fid) =>
        // We just have a file-id
        (fid, None)
      case JsObject(linkFields) =>
        // file-id and project-id
        val fid =
          linkFields.get("id") match {
            case Some(JsString(s)) => s
            case _                 => throw new AppInternalException(s"No file ID found in $jsValue")
          }
        linkFields.get("project") match {
          case Some(JsString(pid: String)) => (fid, Some(pid))
          case _                           => (fid, None)
        }
      case _ =>
        throw new AppInternalException(s"Could not parse a dxlink from $innerObj")
    }

    projId match {
      case None      => DxFile(fid, None)
      case Some(pid) => DxFile(fid, Some(DxProject(pid)))
    }
  }

  def dxFileToJsValue(dxFile: DxFile): JsValue = {
    dxFile.getLinkAsJson
  }

  // copy asset to local project, if it isn't already here.
  def cloneAsset(assetRecord: DxRecord,
                 dxProject: DxProject,
                 pkgName: String,
                 rmtProject: DxProject,
                 verbose: Verbose): Unit = {
    if (dxProject == rmtProject) {
      Utils.trace(verbose.on,
                  s"The asset ${pkgName} is from this project ${rmtProject.id}, no need to clone")
      return
    }
    Utils.trace(verbose.on, s"The asset ${pkgName} is from a different project ${rmtProject.id}")

    // clone
    val req = JsObject("objects" -> JsArray(JsString(assetRecord.id)),
                       "project" -> JsString(dxProject.id),
                       "destination" -> JsString("/"))
    val rep = DXAPI.projectClone(rmtProject.id, jsonNodeOfJsValue(req), classOf[JsonNode])
    val repJs: JsValue = jsValueOfJsonNode(rep)

    val exists = repJs.asJsObject.fields.get("exists") match {
      case None => throw new Exception("API call did not returnd an exists field")
      case Some(JsArray(x)) =>
        x.map {
          case JsString(id) => id
          case _            => throw new Exception("bad type, not a string")
        }.toVector
      case other => throw new Exception(s"API call returned invalid exists field")
    }
    val existingRecords = exists.filter(_.startsWith("record-"))
    existingRecords.size match {
      case 0 =>
        val localAssetRecord = DxRecord(assetRecord.id, None)
        Utils.trace(verbose.on, s"Created ${localAssetRecord.id} pointing to asset ${pkgName}")
      case 1 =>
        Utils.trace(verbose.on, s"The project already has a record pointing to asset ${pkgName}")
      case _ =>
        throw new Exception(s"clone returned too many existing records ${exists}")
    }
  }

  // download a file from the platform to a path on the local disk. Use
  // 'dx download' as a separate process.
  //
  // Note: this function assumes that the target path does not exist yet
  def downloadFile(path: Path, dxfile: DxFile, verbose: Boolean): Unit = {
    def downloadOneFile(path: Path, dxfile: DxFile, counter: Int): Boolean = {
      val fid = dxfile.id
      try {
        // Use dx download. Quote the path, because it may contains spaces.
        val dxDownloadCmd = s"""dx download ${fid} -o "${path.toString()}" """
        val (outmsg, errmsg) = Utils.execCommand(dxDownloadCmd, None)
        true
      } catch {
        case e: Throwable =>
          if (counter < DOWNLOAD_RETRY_LIMIT)
            false
          else throw e
      }
    }
    val dir = path.getParent()
    if (dir != null) {
      if (!Files.exists(dir))
        Files.createDirectories(dir)
    }
    var rc = false
    var counter = 0
    while (!rc && counter < DOWNLOAD_RETRY_LIMIT) {
      Utils.appletLog(verbose, s"downloading file ${path.toString} (try=${counter})")
      rc = downloadOneFile(path, dxfile, counter)
      counter = counter + 1
    }
    if (!rc)
      throw new Exception(s"Failure to download file ${path}")
  }

  // Upload a local file to the platform, and return a json link.
  // Use 'dx upload' as a separate process.
  def uploadFile(path: Path, verbose: Boolean): DxFile = {
    if (!Files.exists(path))
      throw new AppInternalException(s"Output file ${path.toString} is missing")
    def uploadOneFile(path: Path, counter: Int): Option[String] = {
      try {
        // shell out to dx upload. We need to quote the path, because it may contain
        // spaces
        val dxUploadCmd = s"""dx upload "${path.toString}" --brief"""
        Utils.appletLog(verbose, s"--  ${dxUploadCmd}")
        val (outmsg, errmsg) = Utils.execCommand(dxUploadCmd, None)
        if (!outmsg.startsWith("file-"))
          return None
        Some(outmsg.trim())
      } catch {
        case e: Throwable =>
          if (counter < UPLOAD_RETRY_LIMIT)
            None
          else throw e
      }
    }

    var counter = 0
    while (counter < UPLOAD_RETRY_LIMIT) {
      Utils.appletLog(verbose, s"upload file ${path.toString} (try=${counter})")
      uploadOneFile(path, counter) match {
        case Some(fid) =>
          return DxFile(fid, None)
        case None => ()
      }
      counter = counter + 1
    }
    throw new Exception(s"Failure to upload file ${path}")
  }

  private def silentFileDelete(p: Path): Unit = {
    try {
      Files.delete(p)
    } catch {
      case e: Throwable => ()
    }
  }

  // Read the contents of a platform file into a string
  def downloadString(dxFile: DxFile, verbose: Boolean): String = {
    // We don't want to use the dxjava implementation
    //val bytes = dxFile.downloadBytes()
    //new String(bytes, StandardCharsets.UTF_8)

    // We don't want to use "dx cat" because it doesn't validate the checksum.
    //val (outmsg, errmsg) = Utils.execCommand(s"dx cat ${dxFile.id}")
    //outmsg

    // create a temporary file, and write the contents into it.
    val tempFi: Path = Files.createTempFile(s"${dxFile.id}", ".tmp")
    silentFileDelete(tempFi)
    downloadFile(tempFi, dxFile, verbose)
    val content = Utils.readFileContent(tempFi)
    silentFileDelete(tempFi)
    content
  }
}
