// Resolve a large number of platform paths.
// The path format is:
//   "dx://dxWDL_playground:/test_data/fileB",
//   "dx://dxWDL_playground:/test_data/fileC",
//   "dx://dxWDL_playground:/test_data/1/fileC",
//   "dx://dxWDL_playground:/test_data/fruit_list.txt"

package dxWDL.util

import com.dnanexus.{DXAPI, DXDataObject, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

// maximal number of objects in a single API request
import dxWDL.util.Utils.{DX_URL_PREFIX, DXAPI_NUM_OBJECTS_LIMIT}

object DxBulkResolve {

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

  private def submitRequest(dxPaths : Vector[DxPathParsed],
                            dxProject : DXProject) : Map[String, DXDataObject] = {
        val objectReqs : Vector[JsValue] = dxPaths.map{ makeResolutionReq(_) }
        val request = JsObject("objects" -> JsArray(objectReqs),
                               "project" -> JsString(dxProject.getId))

        val response = DXAPI.systemResolveDataObjects(Utils.jsonNodeOfJsValue(request),
                                                      classOf[JsonNode],
                                                      Utils.dxEnv)
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
                val dxid = fields.get("id") match {
                    case Some(JsString(x)) => x
                    case _ => throw new Exception("no id returned")
                }
                // safe conversion to a dx-object
                val dxobj = DxDescribe.convertToDxObject(dxid) match {
                    case None => throw new Exception(s"Bad dxid=${dxid}")
                    case Some(x) => x
                }

                val dxProj : Option[DXProject] = fields.get("project") match {
                    case Some(JsString(x)) => Some(DXProject.getInstance(x))
                    case _ => None
                }

                val dxobjWithProj = dxProj match  {
                    case None => dxobj
                    case Some(proj) => DXDataObject.getInstance(dxobj.getId, proj)
                }
                path -> dxobjWithProj
        }.toMap
    }

    // split between files that have already been resolved (we have their file-id), and
    // those that require lookup.
    private def triage(allDxPaths: Seq[String]) : (Map[String, DXDataObject],
                                                   Vector[DxPathParsed]) = {
        var alreadyResolved = Map.empty[String, DXDataObject]
        var rest = Vector.empty[DxPathParsed]

        for (p <- allDxPaths) {
            val pDxPath = parse(p)
            DxDescribe.convertToDxObject(pDxPath.name) match {
                case None =>
                    rest = rest :+ pDxPath
                case Some(dxobj) =>
                    val dxobjWithProj = pDxPath.projName match {
                        case None => dxobj
                        case Some(pid) =>
                            val dxProj = DxPath.lookupProject(pid)
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
}
