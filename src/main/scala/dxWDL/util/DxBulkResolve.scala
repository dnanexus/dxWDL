// Resolve a large number of platform paths.
// The path format is:
//   "dx://dxWDL_playground:/test_data/fileB",
//   "dx://dxWDL_playground:/test_data/fileC",
//   "dx://dxWDL_playground:/test_data/1/fileC",
//   "dx://dxWDL_playground:/test_data/fruit_list.txt"

package dxWDL.util

import com.dnanexus.{DXAPI, DXFile, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

// maximal number of objects in a single API request
import dxWDL.util.Utils.{DX_URL_PREFIX, DXAPI_NUM_OBJECTS_LIMIT}

object DxBulkResolve {

    // Create a request from a path like:
    //   "dx://dxWDL_playground:/test_data/fileB",
    private def makeReqFromDxPath(dxPath: String) : JsValue = {
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
                ("/", rest)
            else
                (rest.substring(0, index),
                 rest.substring(index + 1))

        projName match {
            case None =>
                JsObject("name" -> JsString(name),
                         "folder" -> JsString(folder))
            case Some(proj) =>
                JsObject("name" -> JsString(name),
                         "folder" -> JsString(folder),
                         "project" -> JsString(proj))
        }
    }

    private def submitRequest(dxPaths : Vector[String]) : Map[String, DXFile] = {
        val objectReqs : Vector[JsValue] = dxPaths.map{ makeReqFromDxPath(_) }

        val request = JsObject("objects" -> JsArray(objectReqs))
        val response = DXAPI.systemDescribeDataObjects(Utils.jsonNodeOfJsValue(request),
                                                       classOf[JsonNode])
        val repJs:JsValue = Utils.jsValueOfJsonNode(response)
        val resultsPerObj:Vector[JsValue] = repJs.asJsObject.fields.get("results") match {
            case Some(JsArray(x)) => x
            case other => throw new Exception(s"API call returned invalid data ${other}")
        }
        resultsPerObj.zipWithIndex.map{
            case (descJs, i) =>
                val path = dxPaths(i)
                val dxFile = descJs.asJsObject.getFields("project", "id") match {
                    case Seq(JsString(projectId), JsString(fileId)) =>
                        DXFile.getInstance(fileId, DXProject.getInstance(projectId))
                    case _ =>
                        throw new Exception(s"bad describe object ${descJs}")
                }
                path -> dxFile
        }.toMap
    }

    // Describe the names of all the files in one batch. This is much more efficient
    // than submitting file describes one-by-one.
    def apply(dxPaths: Seq[String]) : Map[String, DXFile] = {
        if (dxPaths.isEmpty) {
            // avoid an unnessary API call; this is important for unit tests
            // that do not have a network connection.
            return Map.empty
        }

        // Limit on number of objects in one API request
        val slices = dxPaths.grouped(DXAPI_NUM_OBJECTS_LIMIT).toList

        // iterate on the ranges
        slices.foldLeft(Map.empty[String, DXFile]) {
            case (accu, pathsRange) =>
                accu ++ submitRequest(pathsRange.toVector)
        }
    }
}
