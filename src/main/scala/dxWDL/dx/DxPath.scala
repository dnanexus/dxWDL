// Resolve a large number of platform paths.
// The path format is:
//   "dx://dxWDL_playground:/test_data/fileB",
//   "dx://dxWDL_playground:/test_data/fileC",
//   "dx://dxWDL_playground:/test_data/1/fileC",
//   "dx://dxWDL_playground:/test_data/fruit_list.txt"

package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode

import spray.json._
import DxUtils.{jsValueOfJsonNode, jsonNodeOfJsValue}

import scala.collection.mutable

// maximal number of objects in a single API request
import dxWDL.base.Utils.{DX_URL_PREFIX, DXAPI_NUM_OBJECTS_LIMIT}

object DxPath {

  case class DxPathComponents(name: String,
                              folder: Option[String],
                              projName: Option[String],
                              objFullName: String,
                              sourcePath: String)

  def parse(dxPath: String): DxPathComponents = {
    // strip the prefix
    if (!dxPath.startsWith(DX_URL_PREFIX)) {
      throw new Exception(s"Path ${dxPath} does not start with prefix ${DX_URL_PREFIX}")
    }
    val s = dxPath.substring(DX_URL_PREFIX.length)

    // take out the project, if it is specified
    val components = s.split(":").toList
    val (projName, dxObjectPath) = components match {
      case Nil =>
        throw new Exception(s"Path ${dxPath} is invalid")
      case List(objName) =>
        (None, objName)
      case projName :: tail =>
        val rest = tail.mkString(":")
        (Some(projName), rest)
    }

    // sanity test
    projName match {
      case None => ()
      case Some(proj) =>
        if (proj.startsWith("file-"))
          throw new Exception("""|Path ${dxPath} does not look like: dx://PROJECT_NAME:/FILE_PATH
                                 |For example:
                                 |   dx://dxWDL_playground:/test_data/fileB
                                 |""".stripMargin)
    }

    // split the object path into folder/name
    val index = dxObjectPath.lastIndexOf('/')
    val (folderRaw, name) =
      if (index == -1) {
        ("/", dxObjectPath)
      } else {
        (dxObjectPath.substring(0, index), dxObjectPath.substring(index + 1))
      }

    // We don't want a folder if this is a dx-data-object (file-xxxx, record-yyyy)
    val folder =
      if (DxUtils.isDxId(name)) None
      else if (folderRaw == "") Some("/")
      else Some(folderRaw)

    DxPathComponents(name, folder, projName, dxObjectPath, dxPath)
  }

  // Lookup cache for projects. This saves
  // repeated searches for projects we already found.
  private val projectDict = mutable.HashMap.empty[String, DxProject]

  def resolveProject(projName: String): DxProject = {
    if (projName.startsWith("project-")) {
      // A project ID
      return DxProject.getInstance(projName)
    }
    if (projectDict contains projName) {
      return projectDict(projName)
    }

    // A project name, resolve it
    val req =
      JsObject("name" -> JsString(projName), "level" -> JsString("VIEW"), "limit" -> JsNumber(2))
    val rep = DXAPI.systemFindProjects(jsonNodeOfJsValue(req), classOf[JsonNode], DxUtils.dxEnv)
    val repJs: JsValue = jsValueOfJsonNode(rep)

    val results = repJs.asJsObject.fields.get("results") match {
      case Some(JsArray(x)) => x
      case _ =>
        throw new Exception(
            s"Bad response from systemFindProject API call (${repJs.prettyPrint}), when resolving project ${projName}."
        )
    }
    if (results.length > 1)
      throw new Exception(s"Found more than one project named ${projName}")
    if (results.isEmpty)
      throw new Exception(s"Project ${projName} not found")
    val dxProject = results(0).asJsObject.fields.get("id") match {
      case Some(JsString(id)) => DxProject.getInstance(id)
      case _ =>
        throw new Exception(s"Bad response from SystemFindProject API call ${repJs.prettyPrint}")
    }
    projectDict(projName) = dxProject
    dxProject
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
    val request = JsObject("objects" -> JsArray(objectReqs), "project" -> JsString(dxProject.getId))

    val response = DXAPI.systemResolveDataObjects(DxUtils.jsonNodeOfJsValue(request),
                                                  classOf[JsonNode],
                                                  DxUtils.dxEnv)
    val repJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    val resultsPerObj: Vector[JsValue] = repJs.asJsObject.fields.get("results") match {
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
          case Some(JsString(x)) => Some(DxProject.getInstance(x))
          case _                 => None
        }

        // safe conversion to a dx-object
        val dxobj = DxObject.getInstance(dxid, dxContainer)
        path -> dxobj.asInstanceOf[DxDataObject]
    }.toMap
  }

  // split between files that have already been resolved (we have their file-id), and
  // those that require lookup.
  private def triage(
      allDxPaths: Seq[String]
  ): (Map[String, DxDataObject], Vector[DxPathComponents]) = {
    var alreadyResolved = Map.empty[String, DxDataObject]
    var rest = Vector.empty[DxPathComponents]

    for (p <- allDxPaths) {
      val components = parse(p)
      if (DxObject.isDataObject(components.name)) {
        val o = DxObject.getInstance(components.name, None)
        val dxDataObj = o.asInstanceOf[DxDataObject]
        val dxobjWithProj = components.projName match {
          case None => dxDataObj
          case Some(pid) =>
            val dxProj = resolveProject(pid)
            DxObject.getInstance(dxDataObj.getId, dxProj).asInstanceOf[DxDataObject]
        }
        alreadyResolved = alreadyResolved + (p -> dxobjWithProj)
      } else {
        rest = rest :+ components
      }
    }
    (alreadyResolved, rest)
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

  def resolveOnePath(dxPath: String, dxProject: DxProject): DxDataObject = {
    val found: Map[String, DxDataObject] =
      resolveBulk(Vector(dxPath), dxProject)

    if (found.isEmpty)
      throw new Exception(s"Could not find ${dxPath} in project ${dxProject.getId}")
    if (found.size > 1)
      throw new Exception(
          s"Found more than one dx:object in path ${dxPath}, project=${dxProject.getId}"
      )

    found.values.head
  }

  def resolveDxPath(dxPath: String): DxDataObject = {
    val components = parse(dxPath)
    components.projName match {
      case None =>
        resolveOnePath(dxPath, DxUtils.dxCrntProject)
      case Some(pName) =>
        resolveOnePath(dxPath, resolveProject(pName))
    }
  }

  // More accurate types
  def resolveDxURLRecord(buf: String): DxRecord = {
    val dxObj = resolveDxPath(buf)
    if (!dxObj.isInstanceOf[DxRecord])
      throw new Exception(s"Found dx:object of the wrong type ${dxObj}")
    dxObj.asInstanceOf[DxRecord]
  }

  def resolveDxURLFile(buf: String): DxFile = {
    val dxObj = resolveDxPath(buf)
    if (!dxObj.isInstanceOf[DxFile])
      throw new Exception(s"Found dx:object of the wrong type ${dxObj}")
    dxObj.asInstanceOf[DxFile]
  }
}
