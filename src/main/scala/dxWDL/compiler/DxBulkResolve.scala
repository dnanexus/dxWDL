// Resolve a large number of platform paths.
// The path format is:
//   "dx://dxWDL_playground:/test_data/fileB",
//   "dx://dxWDL_playground:/test_data/fileC",
//   "dx://dxWDL_playground:/test_data/1/fileC",
//   "dx://dxWDL_playground:/test_data/fruit_list.txt"

package dxWDL.compiler

import com.dnanexus.{DXAPI, DXFile, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

import dxWDL.util.DxPath
import dxWDL.util.Utils

// maximal number of objects in a single API request
import dxWDL.util.Utils.{DX_URL_PREFIX, DXAPI_NUM_OBJECTS_LIMIT}

case class DxBulkResolve(dxProject: DXProject) {

    case class DxPathParsed(name: String,
                            folder: Option[String],
                            projName: Option[String],
                            sourcePath: String)

    private def parse(dxPath : String) : DxPathParsed = {
        // strip the prefix
        assert(dxPath.startsWith(DX_URL_PREFIX))
        val s = dxPath.substring(DX_URL_PREFIX.length)

        // take out the project, if it is specified
        val components = s.split(":")
        val (projName, rest) =
            if (components.length > 2) {
                throw new Exception(s"Path ${dxPath} cannot have more than two components")
            } else if (components.length == 2) {
                val projName = Some(components(0))
                val rest = components(1)
                (projName, rest)
            } else if (components.length == 1) {
                (None, components(0))
            } else {
                throw new Exception(s"Path ${dxPath} is invalid")
            }

        // split into folder/name
        val index = rest.lastIndexOf('/')
        val (folder, name) =
            if (index == -1)
                (None, rest)
            else
                (Some(rest.substring(0, index)),
                 rest.substring(index + 1))

        DxPathParsed(name, folder, projName, dxPath)
    }

    // Create a request from a path like:
    //   "dx://dxWDL_playground:/test_data/fileB",
    private def makeResolutionReq(pDxPath: DxPathParsed) : JsValue = {
        val reqFields : Map[String, JsValue] = Map("name" -> JsString(pDxPath.name))
        val folderField : Map[String, JsValue] = pDxPath.folder match {
            case None => Map.empty
            case Some(x) => Map("folder" -> JsString(x))
        }
        val projectField : Map[String, JsValue] = pDxPath.projName match {
            case None => Map.empty
            case Some(x) =>
                val dxProj = DxPath.lookupProject(x)
                Map("project" -> JsString(dxProj.getId))
        }
        JsObject(reqFields ++ folderField ++ projectField)
    }

    private def submitRequest(dxPaths : Vector[DxPathParsed]) : Map[String, DXFile] = {
        val objectReqs : Vector[JsValue] = dxPaths.map{ makeResolutionReq(_) }
        val request = JsObject("objects" -> JsArray(objectReqs),
                               "project" -> JsString(dxProject.getId))

        val response = DXAPI.systemResolveDataObjects(Utils.jsonNodeOfJsValue(request),
                                                      classOf[JsonNode])
        val repJs:JsValue = Utils.jsValueOfJsonNode(response)
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
                val fileId = fields.get("id") match {
                    case Some(JsString(x)) => x
                    case _ => throw new Exception("no id returned")
                }
                val projId = fields.get("project") match {
                    case Some(JsString(x)) => Some(x)
                    case _ => None
                }
                val dxFile = projId match  {
                    case None => DXFile.getInstance(fileId)
                    case Some(x) => DXFile.getInstance(fileId, DXProject.getInstance(x))
                }
                path -> dxFile
        }.toMap
    }

    // split between files that have already been resolved (we have their file-id), and
    // those that require lookup.
    private def triage(allDxPaths: Seq[String]) : (Map[String, DXFile],
                                                   Vector[DxPathParsed]) = {
        var alreadyResolved = Map.empty[String, DXFile]
        var rest = Vector.empty[DxPathParsed]

        for (p <- allDxPaths) {
            val pDxPath = parse(p)

            if (pDxPath.name.startsWith("file-")) {
                val fid = pDxPath.name
                val dxFile = pDxPath.projName match {
                    case None => DXFile.getInstance(pDxPath.name)
                    case Some(pid) =>
                        val dxProj = DxPath.lookupProject(pid)
                        DXFile.getInstance(fid, dxProj)
                }
                alreadyResolved = alreadyResolved + (p -> dxFile)
            } else {
                rest = rest :+ pDxPath
            }
        }
        (alreadyResolved, rest)
    }

    // Describe the names of all the files in one batch. This is much more efficient
    // than submitting file describes one-by-one.
    def apply(dxPaths: Seq[String]) : Map[String, DXFile] = {
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
        val resolved = slices.foldLeft(Map.empty[String, DXFile]) {
            case (accu, pathsRange) =>
                accu ++ submitRequest(pathsRange.toVector)
        }

        alreadyResolved ++ resolved
    }
}
