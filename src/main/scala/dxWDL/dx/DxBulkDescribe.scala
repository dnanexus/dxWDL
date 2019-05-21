// Describe a large number of platform objects in bulk.

package dxWDL.dx

import com.dnanexus.{DXAPI, DXFile, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

// maximal number of objects in a single API request
import dxWDL.base.Utils.DXAPI_NUM_OBJECTS_LIMIT

object DxBulkDescribe {
    private def submitRequest(dxFiles : Vector[DXFile],
                              dxProject : Option[DXProject]) : Map[DXFile, DxDescribe] = {
        val oids = dxFiles.map(_.getId).toVector

        val request = JsObject("objects" ->
                                   JsArray(oids.map{x => JsString(x) }))
        val response = DXAPI.systemDescribeDataObjects(DxUtils.jsonNodeOfJsValue(request),
                                                       classOf[JsonNode],
                                                       DxUtils.dxEnv)
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val resultsPerObj:Vector[JsValue] = repJs.asJsObject.fields.get("results") match {
            case Some(JsArray(x)) => x
            case other => throw new Exception(s"API call returned invalid data ${other}")
        }
        resultsPerObj.zipWithIndex.map{ case (jsv, i) =>
            val dxFile = dxFiles(i)
            val fileName = jsv.asJsObject.fields.get("describe") match {
                case None =>
                    throw new Exception(s"Could not describe object ${dxFile.getId}")
                case Some(descJs) =>
                    descJs.asJsObject.getFields("name", "folder", "size", "id", "project", "created") match {
                        case Seq(JsString(name), JsString(folder),
                                 JsNumber(size), JsString(fid), JsString(projectId), JsNumber(created)) =>
                            assert(fid == dxFile.getId)
                            val crDate = new java.util.Date(created.toLong)
                            DxDescribe(name,
                                       folder,
                                       Some(size.toLong),
                                       DXProject.getInstance(projectId),
                                       DxUtils.convertToDxObject(fid).get,
                                       crDate,
                                       Map.empty,
                                       None,
                                       None)
                        case _ =>
                            throw new Exception(s"bad describe object ${descJs}")
                    }
            }
            dxFile -> fileName
        }.toMap
    }

    // Describe the names of all the files in one batch. This is much more efficient
    // than submitting file describes one-by-one.
    def apply(files: Seq[DXFile],
              dxProject : Option[DXProject]) : Map[DXFile, DxDescribe] = {
        if (files.isEmpty) {
            // avoid an unnessary API call; this is important for unit tests
            // that do not have a network connection.
            return Map.empty
        }

        // Limit on number of objects in one API request
        val slices = files.grouped(DXAPI_NUM_OBJECTS_LIMIT).toList

        // iterate on the ranges
        slices.foldLeft(Map.empty[DXFile, DxDescribe]) {
            case (accu, fileRange) =>
                accu ++ submitRequest(fileRange.toVector, dxProject)
        }
    }
}
