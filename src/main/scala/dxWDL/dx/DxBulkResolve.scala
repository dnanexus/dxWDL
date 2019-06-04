// Resolve a large number of platform paths.
// The path format is:
//   "dx://dxWDL_playground:/test_data/fileB",
//   "dx://dxWDL_playground:/test_data/fileC",
//   "dx://dxWDL_playground:/test_data/1/fileC",
//   "dx://dxWDL_playground:/test_data/fruit_list.txt"

package dxWDL.dx

import com.dnanexus.{DXAPI, DXContainer, DXDataObject, DXFile, DXProject, DXRecord}
import com.fasterxml.jackson.databind.JsonNode
import scala.collection.mutable.HashMap
import spray.json._

import DxUtils.{jsonNodeOfJsValue, jsValueOfJsonNode}

// maximal number of objects in a single API request
import dxWDL.base.Utils.{DX_URL_PREFIX, DXAPI_NUM_OBJECTS_LIMIT}

object DxBulkResolve {

    private case class DxPathComponents(name: String,
                                        folder: Option[String],
                                        projName: Option[String],
                                        objFullName : String,
                                        sourcePath: String)

    private def parse(dxPath : String) : DxPathComponents = {
        // strip the prefix
        assert(dxPath.startsWith(DX_URL_PREFIX))
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

        // split the object path into folder/name
        val index = dxObjectPath.lastIndexOf('/')
        val (folderRaw, name) =
            if (index == -1) {
                ("/", dxObjectPath)
            } else {
                (dxObjectPath.substring(0, index),
                 dxObjectPath.substring(index + 1))
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
    private val projectDict = HashMap.empty[String, DXProject]

    def lookupProject(projName: String): DXProject = {
        if (projName.startsWith("project-")) {
            // A project ID
            return DXProject.getInstance(projName)
        }
        if (projectDict contains projName) {
            //System.err.println(s"Cached project ${projName}")
            return projectDict(projName)
        }

        // A project name, resolve it
        val req = JsObject("name" -> JsString(projName),
                           "level" -> JsString("VIEW"),
                           "limit" -> JsNumber(2))
        val rep = DXAPI.systemFindProjects(jsonNodeOfJsValue(req),
                                           classOf[JsonNode],
                                           DxUtils.dxEnv)
        val repJs:JsValue = jsValueOfJsonNode(rep)

        val results = repJs.asJsObject.fields.get("results") match {
            case Some(JsArray(x)) => x
            case _ => throw new Exception(
                s"Bad response from systemFindProject API call (${repJs.prettyPrint}), when resolving project ${projName}.")
        }
        if (results.length > 1)
            throw new Exception(s"Found more than one project named ${projName}")
        if (results.length == 0)
            throw new Exception(s"Project ${projName} not found")
        val dxProject = results(0).asJsObject.fields.get("id") match {
            case Some(JsString(id)) => DXProject.getInstance(id)
            case _ => throw new Exception(s"Bad response from SystemFindProject API call ${repJs.prettyPrint}")
        }
        projectDict(projName) = dxProject
        return dxProject
    }


    // Create a request from a path like:
    //   "dx://dxWDL_playground:/test_data/fileB",
    private def makeResolutionReq(components: DxPathComponents) : JsValue = {
        val reqFields : Map[String, JsValue] = Map("name" -> JsString(components.name))
        val folderField : Map[String, JsValue] = components.folder match {
            case None => Map.empty
            case Some(x) => Map("folder" -> JsString(x))
        }
        val projectField : Map[String, JsValue] = components.projName match {
            case None => Map.empty
            case Some(x) =>
                val dxProj = lookupProject(x)
                Map("project" -> JsString(dxProj.getId))
        }
        JsObject(reqFields ++ folderField ++ projectField)
    }

    private def submitRequest(dxPaths : Vector[DxPathComponents],
                              dxProject : DXProject) : Map[String, DXDataObject] = {
        val objectReqs : Vector[JsValue] = dxPaths.map{ makeResolutionReq(_) }
        val request = JsObject("objects" -> JsArray(objectReqs),
                               "project" -> JsString(dxProject.getId))

        val response = DXAPI.systemResolveDataObjects(DxUtils.jsonNodeOfJsValue(request),
                                                      classOf[JsonNode],
                                                      DxUtils.dxEnv)
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val resultsPerObj:Vector[JsValue] = repJs.asJsObject.fields.get("results") match {
            case Some(JsArray(x)) => x
            case other => throw new Exception(s"API call returned invalid data ${other}")
        }
        resultsPerObj.zipWithIndex.map{
            case (descJs : JsValue, i) =>
                val path = dxPaths(i).sourcePath
                val o = descJs match {
                    case JsArray(x) if x.isEmpty =>
                        throw new Exception(s"Path ${path} not found req=${objectReqs(i)}, i=${i}, project=${dxProject.getId}")
                    case JsArray(x) if x.length == 1 => x(0)
                    case JsArray(x) =>
                        throw new Exception(s"Found more than one dx object in path ${path}")
                    case obj :JsObject => obj
                    case other => throw new Exception(s"malformed json ${other}")
                }
                val fields = o.asJsObject.fields
                val dxid = fields.get("id") match {
                    case Some(JsString(x)) => x
                    case _ => throw new Exception("no id returned")
                }

                // could be a container, not a project
                val dxContainer : Option[DXContainer] = fields.get("project") match {
                    case Some(JsString(x)) => Some(DXContainer.getInstance(x))
                    case _ => None
                }

                // safe conversion to a dx-object
                val dxobj = DxUtils.convertToDxObject(dxid, dxContainer) match {
                    case None => throw new Exception(s"Bad dxid=${dxid}")
                    case Some(x) => x
                }

                path -> dxobj
        }.toMap
    }

    // split between files that have already been resolved (we have their file-id), and
    // those that require lookup.
    private def triage(allDxPaths: Seq[String]) : (Map[String, DXDataObject],
                                                   Vector[DxPathComponents]) = {
        var alreadyResolved = Map.empty[String, DXDataObject]
        var rest = Vector.empty[DxPathComponents]

        for (p <- allDxPaths) {
            val dxPathComp = parse(p)
            DxUtils.convertToDxObject(dxPathComp.name, None) match {
                case None =>
                    rest = rest :+ dxPathComp
                case Some(dxobj) =>
                    val dxobjWithProj = dxPathComp.projName match {
                        case None => dxobj
                        case Some(pid) =>
                            val dxProj = lookupProject(pid)
                            DXDataObject.getInstance(dxobj.getId, dxProj)
                    }
                    alreadyResolved = alreadyResolved + (p -> dxobjWithProj)
            }
        }
        (alreadyResolved, rest)
    }

    // Describe the names of all the data objects in one batch. This is much more efficient
    // than submitting object describes one-by-one.
    def apply(dxPaths: Seq[String],
              dxProject: DXProject) : Map[String, DXDataObject] = {
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
        val resolved = slices.foldLeft(Map.empty[String, DXDataObject]) {
            case (accu, pathsRange) =>
                accu ++ submitRequest(pathsRange.toVector, dxProject)
        }

        alreadyResolved ++ resolved
    }

    def lookupOnePath(dxPath: String,
                      dxProject : DXProject) : DXDataObject = {
        val found : Map[String, DXDataObject] =
            apply(Vector(dxPath), dxProject)

        if (found.size == 0)
            throw new Exception(s"Could not find ${dxPath} in project ${dxProject.getId}")
        if (found.size > 1)
            throw new Exception(s"Found more than one dx:object in path ${dxPath}, project=${dxProject.getId}")

        found.values.head
    }

    private def lookupDxPath(dxPath: String) : DXDataObject = {
        val components = parse(dxPath)
        components.projName match {
            case None =>
                lookupOnePath(dxPath,
                              DxUtils.dxEnv.getProjectContext())
            case Some(pName) =>
                lookupOnePath(dxPath,
                              lookupProject(pName))
        }
    }

    // More accurate types
    def lookupDxURLRecord(buf: String) : DXRecord = {
        val dxObj = lookupDxPath(buf)
        if (!dxObj.isInstanceOf[DXRecord])
            throw new Exception(s"Found dx:object of the wrong type ${dxObj}")
        dxObj.asInstanceOf[DXRecord]
    }

    def lookupDxURLFile(buf: String) : DXFile = {
        val dxObj = lookupDxPath(buf)
        if (!dxObj.isInstanceOf[DXFile])
            throw new Exception(s"Found dx:object of the wrong type ${dxObj}")
        dxObj.asInstanceOf[DXFile]
    }
}
