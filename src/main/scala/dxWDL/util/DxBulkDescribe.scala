// Describe a large number of platform objects in bulk.

package dxWDL.util

import com.dnanexus.{DXAPI, DXFile, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

// maximal number of objects in a single API request
import dxWDL.util.Utils.DXAPI_NUM_OBJECTS_LIMIT

object DxBulkDescribe {
    private def submitRequest(dxFiles : Vector[DXFile]) : Map[DXFile, DxDescribe] = {
        val oids = dxFiles.map(_.getId).toVector

        val request = JsObject("objects" ->
                                   JsArray(oids.map{x => JsString(x) }))
        val response = DXAPI.systemDescribeDataObjects(Utils.jsonNodeOfJsValue(request),
                                                       classOf[JsonNode],
                                                       Utils.dxEnv)
        val repJs:JsValue = Utils.jsValueOfJsonNode(response)
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
                    descJs.asJsObject.getFields("name", "folder", "size", "id", "project") match {
                        case Seq(JsString(name), JsString(folder),
                                 JsNumber(size), JsString(fid), JsString(projectId)) =>
                            assert(fid == dxFile.getId)
                            DxDescribe(name,
                                       folder,
                                       size.toLong,
                                       DXProject.getInstance(projectId),
                                       DxDescribe.convertToDxObject(fid).get,
                                       Map.empty, // properties
                                       Vector.empty,  // inputSpec
                                       Vector.empty  // runtimeSpec
                            )
                        case _ =>
                            throw new Exception(s"bad describe object ${descJs}")
                    }
            }
            dxFile -> fileName
        }.toMap
    }

    // Describe the names of all the files in one batch. This is much more efficient
    // than submitting file describes one-by-one.
    def apply(files: Seq[DXFile]) : Map[DXFile, DxDescribe] = {
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
                accu ++ submitRequest(fileRange.toVector)
        }
    }
}
