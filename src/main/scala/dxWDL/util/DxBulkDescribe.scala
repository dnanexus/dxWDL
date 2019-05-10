// Describe a large number of platform objects in bulk.

package dxWDL.util

import com.dnanexus.{DXAPI, DXFile}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

object DxBulkDescribe {
    // this is a subset of the what you can get from DXDataObject.Describe
    case class MiniDescribe(name : String,
                            folder: String,
                            projectId: String,
                            fileId : String)

    // Describe the names of all the files in one batch. This is much more efficient
    // than submitting file describes one-by-one.
    def apply(files: Seq[DXFile]) : Map[DXFile, MiniDescribe] = {
        if (files.isEmpty) {
            // avoid an unnessary API call; this is important for unit tests
            // that do not have a network connection.
            return Map.empty
        }

        val oids = files.map(_.getId).toVector

        // Temporary, until we implement paging.
        assert(oids.size < 1000)
        val request = JsObject("objects" ->
                                   JsArray(oids.map{x => JsString(x) }))
        val response = DXAPI.systemDescribeDataObjects(Utils.jsonNodeOfJsValue(request),
                                                       classOf[JsonNode])
        val repJs:JsValue = Utils.jsValueOfJsonNode(response)
        val resultsPerObj:Vector[JsValue] = repJs.asJsObject.fields.get("results") match {
            case Some(JsArray(x)) => x
            case other => throw new Exception(s"API call returned invalid data ${other}")
        }
        resultsPerObj.zipWithIndex.map{ case (jsv, i) =>
            val dxFile = files(i)
            val fileName = jsv.asJsObject.fields.get("describe") match {
                case None =>
                    throw new Exception(s"Could not describe object ${dxFile.getId}")
                case Some(descJs) =>
                    descJs.asJsObject.getFields("id", "project", "name", "folder") match {
                        case Seq(JsString(id), JsString(projectId), JsString(name), JsString(folder)) =>
                            assert(id == dxFile.getId)
                            MiniDescribe(id, projectId, name, folder)
                        case _ =>
                            throw new Exception(s"bad describe object ${descJs}")
                    }
            }
            dxFile -> fileName
        }.toMap
    }
}
