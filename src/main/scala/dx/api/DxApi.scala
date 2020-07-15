package dx.api

import java.nio.file.{Files, Path}

import com.dnanexus.{DXAPI, DXEnvironment}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import dx.api.DxPath.DxPathComponents
import dx.{AppInternalException, IllegalArgumentException}
import spray.json._
import wdlTools.util.{Logger, Util}

// wrapper around DNAnexus Java API
case class DxApi(logger: Logger = Logger.Quiet, dxEnv: DXEnvironment = DXEnvironment.create()) {
  lazy val currentProject: DxProject = DxProject(this, dxEnv.getProjectContext)
  lazy val currentJob: DxJob = DxJob(this, dxEnv.getJob)
  // Convert from spray-json to jackson JsonNode
  // Used to convert into the JSON datatype used by dxjava
  private lazy val objMapper: ObjectMapper = new ObjectMapper()
  private val DOWNLOAD_RETRY_LIMIT = 3
  private val UPLOAD_RETRY_LIMIT = 3
  private val DXAPI_NUM_OBJECTS_LIMIT = 1000 // maximal number of objects in a single API request

  // We are expecting string like:
  //    record-FgG51b00xF63k86F13pqFv57
  //    file-FV5fqXj0ffPB9bKP986j5kVQ
  //
  def getObject(id: String, container: Option[DxProject] = None): DxObject = {
    val (objType, _) = DxUtils.parseObjectId(id)
    objType match {
      case "analysis"  => DxAnalysis(this, id, container)
      case "app"       => DxApp(this, id)
      case "applet"    => DxApplet(this, id, container)
      case "container" => DxProject(this, id)
      case "file"      => DxFile(this, id, container)
      case "job"       => DxJob(this, id, container)
      case "project"   => DxProject(this, id)
      case "record"    => DxRecord(this, id, container)
      case "workflow"  => DxWorkflow(this, id, container)
      case _ =>
        throw new IllegalArgumentException(
            s"${id} does not belong to a know DNAnexus object class"
        )
    }
  }

  def dataObject(id: String, project: Option[DxProject] = None): DxDataObject = {
    getObject(id, project) match {
      case dataObj: DxDataObject => dataObj
      case _                     => throw new IllegalArgumentException(s"${id} isn't a data object")
    }
  }

  def isDataObjectId(id: String): Boolean = {
    try {
      dataObject(id)
      true
    } catch {
      case _: Throwable => false
    }
  }

  def executable(id: String, project: Option[DxProject] = None): DxExecutable = {
    getObject(id, project) match {
      case exe: DxExecutable => exe
      case _                 => throw new IllegalArgumentException(s"${id} isn't an executable")
    }
  }

  // Lookup cache for projects. This saves repeated searches for projects we already found.
  private var projectDict: Map[String, DxProject] = Map.empty

  def resolveProject(projName: String): DxProject = {
    if (projName.startsWith("project-")) {
      // A project ID
      return project(projName)
    }

    if (projectDict.contains(projName)) {
      return projectDict(projName)
    }

    // A project name, resolve it
    val responseJs = findProjects(
        Map(
            "name" -> JsString(projName),
            "level" -> JsString("VIEW"),
            "limit" -> JsNumber(2)
        )
    )
    val results = responseJs.fields.get("results") match {
      case Some(JsArray(x)) => x
      case _ =>
        throw new Exception(
            s"""Bad response from systemFindProject API call (${responseJs.prettyPrint}), 
               |when resolving project ${projName}.""".stripMargin
        )
    }
    if (results.length > 1) {
      throw new Exception(s"Found more than one project named ${projName}")
    }
    if (results.isEmpty) {
      throw new Exception(s"Project ${projName} not found")
    }
    val dxProject = results(0).asJsObject.fields.get("id") match {
      case Some(JsString(id)) => project(id)
      case _ =>
        throw new Exception(
            s"Bad response from SystemFindProject API call ${responseJs.prettyPrint}"
        )
    }
    projectDict += (projName -> dxProject)
    dxProject
  }

  def analysis(id: String, project: Option[DxProject] = None): DxAnalysis = {
    getObject(id, project) match {
      case a: DxAnalysis => a
      case _             => throw new IllegalArgumentException(s"${id} isn't an analysis")
    }
  }

  def app(id: String): DxApp = {
    getObject(id) match {
      case a: DxApp => a
      case _        => throw new IllegalArgumentException(s"${id} isn't an app")
    }
  }

  def applet(id: String, project: Option[DxProject] = None): DxApplet = {
    getObject(id, project) match {
      case a: DxApplet => a
      case _           => throw new IllegalArgumentException(s"${id} isn't an applet")
    }
  }

  def file(id: String, project: Option[DxProject] = None): DxFile = {
    getObject(id, project) match {
      case a: DxFile => a
      case _         => throw new IllegalArgumentException(s"${id} isn't a file")
    }
  }

  def job(id: String, project: Option[DxProject] = None): DxJob = {
    getObject(id, project) match {
      case a: DxJob => a
      case _        => throw new IllegalArgumentException(s"${id} isn't a job")
    }
  }

  def project(id: String): DxProject = {
    getObject(id) match {
      case a: DxProject => a
      case _            => throw new IllegalArgumentException(s"${id} isn't a project")
    }
  }

  def record(id: String, project: Option[DxProject] = None): DxRecord = {
    getObject(id, project) match {
      case a: DxRecord => a
      case _           => throw new IllegalArgumentException(s"${id} isn't a record")
    }
  }

  def workflow(id: String, project: Option[DxProject] = None): DxWorkflow = {
    getObject(id, project) match {
      case a: DxWorkflow => a
      case _             => throw new IllegalArgumentException(s"${id} isn't a workflow")
    }
  }

  private def call(fn: (Any, Class[JsonNode], DXEnvironment) => JsonNode,
                   fields: Map[String, JsValue]): JsObject = {
    val request = objMapper.readTree(JsObject(fields).prettyPrint)
    val response = fn(request, classOf[JsonNode], dxEnv)
    response.toString.parseJson.asJsObject
  }

  private def callObject(fn: (String, Any, Class[JsonNode], DXEnvironment) => JsonNode,
                         objectId: String,
                         fields: Map[String, JsValue] = Map.empty): JsObject = {
    val request = objMapper.readTree(JsObject(fields).prettyPrint)
    val response = fn(objectId, request, classOf[JsonNode], dxEnv)
    response.toString.parseJson.asJsObject
  }

  def analysisDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.analysisDescribe[JsonNode], id, fields)
  }

  def analysisSetProperties(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.analysisSetProperties[JsonNode], id, fields)
    logger.ignore(result)
  }

  def appDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appDescribe[JsonNode], id, fields)
  }

  def appRun(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appRun[JsonNode], id, fields)
  }

  def appletDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appletDescribe[JsonNode], id, fields)
  }

  def appletNew(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.appletNew[JsonNode], fields)
  }

  def appletRename(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appletRename[JsonNode], id, fields)
  }

  def appletRun(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appletRun[JsonNode], id, fields)
  }

  def containerDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.containerDescribe[JsonNode], id, fields)
  }

  def containerListFolder(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.containerListFolder[JsonNode], id, fields)
  }

  def containerMove(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.containerMove[JsonNode], id, fields)
    logger.ignore(result)
  }

  def containerNewFolder(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.containerNewFolder[JsonNode], id, fields)
    logger.ignore(result)
  }

  def containerRemoveObjects(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.containerRemoveObjects[JsonNode], id, fields)
    logger.ignore(result)
  }

  def executionsDescribe(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemDescribeExecutions[JsonNode], fields)
  }

  def fileDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.fileDescribe[JsonNode], id, fields)
  }

  // Describe the names of all the files in one batch. This is much more efficient
  // than submitting file describes one-by-one.
  def fileBulkDescribe(
      files: Vector[DxFile],
      extraFields: Set[Field.Value] = Set.empty
  ): Vector[DxFile] = {
    if (files.isEmpty) {
      // avoid an unnessary API call; this is important for unit tests
      // that do not have a network connection.
      return Vector.empty
    }

    val dxFindDataObjects = DxFindDataObjects(this, None)

    // Describe a large number of platform objects in bulk.
    // DxFindDataObjects caches the desc on the DxFile object, so we only
    // need to return the DxFile.
    def submitRequest(objs: Vector[DxFile],
                      extraFields: Set[Field.Value],
                      project: Option[DxProject]): Vector[DxFile] = {
      val ids = objs.map(file => file.getId)
      dxFindDataObjects
        .apply(
            dxProject = project,
            folder = None,
            recurse = true,
            klassRestriction = Some("file"),
            withProperties = Vector.empty,
            nameConstraints = Vector.empty,
            withInputOutputSpec = true,
            idConstraints = ids,
            extrafields = extraFields
        )
        .asInstanceOf[Map[DxFile, DxFileDescribe]]
        .keys
        .toVector
    }

    // group files by projects, in order to avoid searching in all projects (unless project is not specified)
    files.groupBy(file => file.project).foldLeft(Vector.empty[DxFile]) {
      case (accuOuter, (proj, files)) =>
        // Limit on number of objects in one API request
        val slices = files.grouped(DXAPI_NUM_OBJECTS_LIMIT).toList
        // iterate on the ranges
        accuOuter ++ slices.foldLeft(Vector.empty[DxFile]) {
          case (accu, objRange) =>
            accu ++ submitRequest(objRange, extraFields, proj)
        }
    }
  }

  def findApps(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemFindApps[JsonNode], fields)
  }

  def findDataObjects(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemFindDataObjects[JsonNode], fields)
  }

  def findExecutions(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemFindExecutions[JsonNode], fields)
  }

  // Search through a JSON value for all the dx:file links inside it. Returns
  // those as a vector.
  def findFiles(jsValue: JsValue): Vector[DxFile] = {
    jsValue match {
      case JsBoolean(_) | JsNumber(_) | JsString(_) | JsNull =>
        Vector.empty[DxFile]
      case JsObject(_) if DxFile.isDxFile(jsValue) =>
        Vector(DxFile.fromJsValue(this, jsValue))
      case JsObject(fields) =>
        fields.map { case (_, v) => findFiles(v) }.toVector.flatten
      case JsArray(elems) =>
        elems.flatMap(e => findFiles(e))
    }
  }

  def findProjects(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemFindProjects[JsonNode], fields)
  }

  def jobDescribe(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    callObject(DXAPI.jobDescribe[JsonNode], id, fields)
  }

  def jobNew(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.jobNew[JsonNode], fields)
  }

  def orgDescribe(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    try {
      callObject(DXAPI.orgDescribe[JsonNode], id, fields)
    } catch {
      case cause: com.dnanexus.exceptions.PermissionDeniedException =>
        throw new dx.PermissionDeniedException(
            s"You do not have permission to describe org ${id}",
            cause
        )
    }
  }

  def projectClone(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    callObject(DXAPI.projectClone[JsonNode], id, fields)
  }

  def projectDescribe(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    callObject(DXAPI.projectDescribe[JsonNode], id, fields)
  }

  def projectListFolder(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.projectListFolder[JsonNode], id, fields)
  }

  def projectMove(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.projectMove[JsonNode], id, fields)
    logger.ignore(result)
  }

  def projectNewFolder(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.projectNewFolder[JsonNode], id, fields)
    logger.ignore(result)
  }

  def projectRemoveObjects(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.projectRemoveObjects[JsonNode], id, fields)
    logger.ignore(result)
  }

  def recordDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.recordDescribe[JsonNode], id, fields)
  }

  def resolveDataObjects(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemResolveDataObjects[JsonNode], fields)
  }

  def userDescribe(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    try {
      callObject(DXAPI.userDescribe[JsonNode], id, fields)
    } catch {
      case cause: com.dnanexus.exceptions.PermissionDeniedException =>
        throw new dx.PermissionDeniedException(
            s"You do not have permission to describe user ${id}",
            cause
        )
    }
  }

  def workflowClose(id: String): Unit = {
    callObject(DXAPI.workflowClose[JsonNode], id)
  }

  def workflowDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.workflowDescribe[JsonNode], id, fields)
  }

  def workflowNew(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.workflowNew[JsonNode], fields)
  }

  def workflowRename(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.workflowRename[JsonNode], id, fields)
  }

  def workflowRun(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.workflowRun[JsonNode], id, fields)
  }

  def runSubJob(entryPoint: String,
                instanceType: Option[String],
                inputs: JsValue,
                dependsOn: Vector[DxExecution],
                delayWorkspaceDestruction: Option[Boolean]): DxJob = {
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
        }
        Map("dependsOn" -> JsArray(execIds))
      }
    val dwd = delayWorkspaceDestruction match {
      case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
      case _          => Map.empty
    }
    val request = fields ++ instanceFields ++ dependsFields ++ dwd
    logger.traceLimited(s"subjob request=${JsObject(request).prettyPrint}")

    val info = jobNew(request)
    val id: String = info.fields.get("id") match {
      case Some(JsString(x)) => x
      case _ =>
        throw new AppInternalException(s"Bad format returned from jobNew ${response.prettyPrint}")
    }
    job(id)
  }

  // copy asset to local project, if it isn't already here.
  def cloneAsset(assetRecord: DxRecord,
                 dxProject: DxProject,
                 pkgName: String,
                 rmtProject: DxProject): Unit = {
    if (dxProject == rmtProject) {
      logger.trace(s"The asset ${pkgName} is from this project ${rmtProject.id}, no need to clone")
      return
    }
    logger.trace(s"The asset ${pkgName} is from a different project ${rmtProject.id}")

    // clone
    val request = Map("objects" -> JsArray(JsString(assetRecord.id)),
                      "project" -> JsString(dxProject.id),
                      "destination" -> JsString("/"))
    val responseJs = projectClone(rmtProject.id, request)

    val exists = responseJs.fields.get("exists") match {
      case None => throw new Exception("API call did not returnd an exists field")
      case Some(JsArray(x)) =>
        x.map {
          case JsString(id) => id
          case _            => throw new Exception("bad type, not a string")
        }
      case _ => throw new Exception(s"API call returned invalid exists field")
    }
    val existingRecords = exists.filter(_.startsWith("record-"))
    existingRecords.size match {
      case 0 =>
        val localAssetRecord = record(assetRecord.id, None)
        logger.trace(s"Created ${localAssetRecord.id} pointing to asset ${pkgName}")
      case 1 =>
        logger.trace(s"The project already has a record pointing to asset ${pkgName}")
      case _ =>
        throw new Exception(s"clone returned too many existing records ${exists}")
    }
  }

  // download a file from the platform to a path on the local disk. Use
  // 'dx download' as a separate process.
  //
  // Note: this function assumes that the target path does not exist yet
  def downloadFile(path: Path, dxfile: DxFile): Unit = {
    def downloadOneFile(path: Path, dxfile: DxFile, counter: Int): Boolean = {
      val fid = dxfile.id
      try {
        // Use dx download. Quote the path, because it may contains spaces.
        val dxDownloadCmd = s"""dx download ${fid} -o "${path.toString}" """
        val (_, _) = Util.execCommand(dxDownloadCmd, None)
        true
      } catch {
        case e: Throwable =>
          if (counter < DOWNLOAD_RETRY_LIMIT)
            false
          else throw e
      }
    }

    val dir = path.getParent
    if (dir != null) {
      if (!Files.exists(dir))
        Files.createDirectories(dir)
    }
    var rc = false
    var counter = 0
    while (!rc && counter < DOWNLOAD_RETRY_LIMIT) {
      logger.traceLimited(s"downloading file ${path.toString} (try=${counter})")
      rc = downloadOneFile(path, dxfile, counter)
      counter = counter + 1
    }
    if (!rc) {
      throw new Exception(s"Failure to download file ${path}")
    }
  }

  // Upload a local file to the platform, and return a json link.
  // Use 'dx upload' as a separate process.
  def uploadFile(path: Path): DxFile = {
    if (!Files.exists(path))
      throw new AppInternalException(s"Output file ${path.toString} is missing")

    def uploadOneFile(path: Path, counter: Int): Option[String] = {
      try {
        // shell out to dx upload. We need to quote the path, because it may contain
        // spaces
        val dxUploadCmd = s"""dx upload "${path.toString}" --brief"""
        logger.traceLimited(s"--  ${dxUploadCmd}")
        val (outmsg, _) = Util.execCommand(dxUploadCmd, None)
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
      logger.traceLimited(s"upload file ${path.toString} (try=${counter})")
      uploadOneFile(path, counter) match {
        case Some(fid) => return file(fid, None)
        case None      => ()
      }
      counter = counter + 1
    }
    throw new Exception(s"Failure to upload file ${path}")
  }

  private def silentFileDelete(p: Path): Unit = {
    try {
      Files.delete(p)
    } catch {
      case _: Throwable => ()
    }
  }

  // Read the contents of a platform file into a string
  def downloadString(dxFile: DxFile): String = {
    // We don't want to use the dxjava implementation
    //val bytes = dxFile.downloadBytes()
    //new String(bytes, StandardCharsets.UTF_8)

    // We don't want to use "dx cat" because it doesn't validate the checksum.
    //val (outmsg, errmsg) = Utils.execCommand(s"dx cat ${dxFile.id}")
    //outmsg

    // create a temporary file, and write the contents into it.
    val tempFi: Path = Files.createTempFile(s"${dxFile.id}", ".tmp")
    silentFileDelete(tempFi)
    downloadFile(tempFi, dxFile)
    val content = Util.readFileContent(tempFi)
    silentFileDelete(tempFi)
    content
  }

  private def triageOne(components: DxPathComponents): Either[DxDataObject, DxPathComponents] = {
    if (isDataObjectId(components.name)) {
      val dxDataObj = dataObject(components.name)
      val dxObjWithProj = components.projName match {
        case None => dxDataObj
        case Some(pid) =>
          val dxProj = resolveProject(pid)
          dataObject(dxDataObj.getId, Some(dxProj))
      }
      Left(dxObjWithProj)
    } else {
      Right(components)
    }
  }

  // split between files that have already been resolved (we have their file-id), and
  // those that require lookup.
  private def triage(
      allDxPaths: Seq[String]
  ): (Map[String, DxDataObject], Vector[DxPathComponents]) = {
    var alreadyResolved = Map.empty[String, DxDataObject]
    var rest = Vector.empty[DxPathComponents]

    allDxPaths.foreach { p =>
      triageOne(DxPath.parse(p)) match {
        case Left(dxObjWithProj) => alreadyResolved += (p -> dxObjWithProj)
        case Right(components)   => rest :+= components
      }
    }
    (alreadyResolved, rest)
  }

  // Create a request from a path like:
  //   "dx://dxWDL_playground:/test_data/fileB",
  private def makeResolutionReq(components: DxPathComponents): JsValue = {
    val reqFields: Map[String, JsValue] = Map("name" -> JsString(components.name))
    val folderField: Map[String, JsValue] = components.folder match {
      case None    => Map.empty
      case Some(x) => Map("folder" -> JsString(x))
    }
    val projectField: Map[String, JsValue] = components.projName match {
      case None => Map.empty
      case Some(x) =>
        val dxProj = resolveProject(x)
        Map("project" -> JsString(dxProj.getId))
    }
    JsObject(reqFields ++ folderField ++ projectField)
  }

  private def submitRequest(dxPaths: Vector[DxPathComponents],
                            dxProject: DxProject): Map[String, DxDataObject] = {
    val objectReqs: Vector[JsValue] = dxPaths.map(makeResolutionReq)
    val request = Map("objects" -> JsArray(objectReqs), "project" -> JsString(dxProject.getId))
    val responseJs = resolveDataObjects(request)
    val resultsPerObj: Vector[JsValue] = responseJs.fields.get("results") match {
      case Some(JsArray(x)) => x
      case other            => throw new Exception(s"API call returned invalid data ${other}")
    }
    resultsPerObj.zipWithIndex.map {
      case (descJs: JsValue, i) =>
        val path = dxPaths(i).sourcePath
        val o = descJs match {
          case JsArray(x) if x.isEmpty =>
            throw new Exception(
                s"Path ${path} not found req=${objectReqs(i)}, i=${i}, project=${dxProject.getId}"
            )
          case JsArray(x) if x.length == 1 => x(0)
          case JsArray(_) =>
            throw new Exception(s"Found more than one dx object in path ${path}")
          case obj: JsObject => obj
          case other         => throw new Exception(s"malformed json ${other}")
        }
        val fields = o.asJsObject.fields
        val dxid = fields.get("id") match {
          case Some(JsString(x)) => x
          case _                 => throw new Exception("no id returned")
        }

        // could be a container, not a project
        val dxContainer: Option[DxProject] = fields.get("project") match {
          case Some(JsString(x)) => Some(project(x))
          case _                 => None
        }

        // safe conversion to a dx-object
        path -> dataObject(dxid, dxContainer)
    }.toMap
  }

  // Describe the names of all the data objects in one batch. This is much more efficient
  // than submitting object describes one-by-one.
  def resolveBulk(dxPaths: Seq[String], dxProject: DxProject): Map[String, DxDataObject] = {
    if (dxPaths.isEmpty) {
      // avoid an unnessary API call; this is important for unit tests
      // that do not have a network connection.
      return Map.empty
    }

    // peel off objects that have already been resolved
    val (alreadyResolved, dxPathsToResolve) = triage(dxPaths)
    if (dxPathsToResolve.isEmpty)
      return alreadyResolved

    // Limit on number of objects in one API request
    val slices = dxPathsToResolve.grouped(DXAPI_NUM_OBJECTS_LIMIT).toList

    // iterate on the ranges
    val resolved = slices.foldLeft(Map.empty[String, DxDataObject]) {
      case (accu, pathsRange) =>
        accu ++ submitRequest(pathsRange, dxProject)
    }

    alreadyResolved ++ resolved
  }

  def resolveOnePath(dxPath: String,
                     dxProject: Option[DxProject] = None,
                     dxPathComponents: Option[DxPathComponents] = None): DxDataObject = {
    val components = dxPathComponents.getOrElse(DxPath.parse(dxPath))
    val proj = dxProject.getOrElse(components.projName match {
      case Some(projName) => resolveProject(projName)
      case None           => currentProject
    })
    // peel off objects that have already been resolved
    val found = triageOne(components) match {
      case Left(alreadyResolved) => Vector(alreadyResolved)
      case Right(dxPathsToResolve) =>
        submitRequest(Vector(dxPathsToResolve), proj).values.toVector
    }

    found.size match {
      case 0 =>
        throw new Exception(s"Could not find ${dxPath} in project ${proj.getId}")
      case 1 => found.head
      case _ =>
        throw new Exception(
            s"Found more than one dx:object in path ${dxPath}, project=${proj.getId}"
        )
    }
  }

  // More accurate types
  def resolveDxUrlRecord(buf: String): DxRecord = {
    val dxObj = resolveOnePath(buf)
    if (!dxObj.isInstanceOf[DxRecord]) {
      throw new Exception(s"Found dx:object of the wrong type ${dxObj}")
    }
    dxObj.asInstanceOf[DxRecord]
  }

  def resolveDxUrlFile(buf: String): DxFile = {
    val dxObj = resolveOnePath(buf)
    if (!dxObj.isInstanceOf[DxFile]) {
      throw new Exception(s"Found dx:object of the wrong type ${dxObj}")
    }
    dxObj.asInstanceOf[DxFile]
  }
}
