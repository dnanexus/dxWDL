// Describe a large number of platform objects in bulk.

package dxWDL.dx

import com.dnanexus.{DXAPI, DXFile, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

// maximal number of objects in a single API request
import dxWDL.base.Utils.DXAPI_NUM_OBJECTS_LIMIT

object DxBulkDescribe {

    // Parse the parts from a description of a file
    // The format is something like this:
    // {
    //  "1": {
    //    "md5": "71565d7f4dc0760457eb252a31d45964",
    //    "size": 42,
    //    "state": "complete"
    //  }
    //}
    //
    private def parseFileParts(jsv: JsValue) : Map[Int, DxFilePart] = {
        //System.out.println(jsv.prettyPrint)
        jsv.asJsObject.fields.map{
            case (partNumber, partDesc) =>
                val dxPart = partDesc.asJsObject.getFields("md5", "size", "state") match {
                    case Seq(JsString(md5), JsNumber(size), JsString(state)) =>
                        DxFilePart(state, size.toLong, md5)
                    case _ => throw new Exception(s"malformed part description ${partDesc.prettyPrint}")
                }
                partNumber.toInt -> dxPart
        }.toMap
    }

    private def submitRequest(dxFiles : Vector[DXFile],
                              dxProject : Option[DXProject],
                              parts : Boolean) : Map[DXFile, DxDescribe] = {
        val oids = dxFiles.map(_.getId).toVector

        val requestFields = Map("objects" ->
                                   JsArray(oids.map{x => JsString(x) }))

        // extra describe options, if specified
        val extraDescribeFields : Map[String, JsValue] =
            if (parts) {
                Map("classDescribeOptions" -> JsObject(
                        "file" ->
                            JsObject("parts" -> JsBoolean(true))))
            } else {
                Map.empty
            }
        val request = JsObject(requestFields ++ extraDescribeFields)

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
            val dxFullDesc = jsv.asJsObject.fields.get("describe") match {
                case None =>
                    throw new Exception(s"Could not describe object ${dxFile.getId}")
                case Some(descJs) =>
                    val dxDesc = descJs.asJsObject.getFields("name", "folder", "size", "id", "project", "created") match {
                        case Seq(JsString(name), JsString(folder),
                                 JsNumber(size), JsString(fid), JsString(projectId), JsNumber(created)) =>
                            assert(fid == dxFile.getId)
                            val crDate = new java.util.Date(created.toLong)
                            val dxProj =
                                if (projectId.startsWith("project-"))
                                    Some(DXProject.getInstance(projectId))
                                else
                                    None
                            DxDescribe(name,
                                       folder,
                                       Some(size.toLong),
                                       dxProj,
                                       DxUtils.convertToDxObject(fid).get,
                                       crDate,
                                       Map.empty,
                                       None,
                                       None,
                                       None)
                        case _ =>
                            throw new Exception(s"bad describe object ${descJs}")
                    }

                    // The parts may be empty, only files have it, and we don't always ask for it.
                    val parts = descJs.asJsObject.fields.get("parts").map(parseFileParts)
                    dxDesc.copy(parts = parts)
            }
            dxFile -> dxFullDesc
        }.toMap
    }

    // Describe the names of all the files in one batch. This is much more efficient
    // than submitting file describes one-by-one.
    def apply(files: Seq[DXFile],
              dxProject : Option[DXProject],
              parts : Boolean = false) : Map[DXFile, DxDescribe] = {
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
                accu ++ submitRequest(fileRange.toVector, dxProject, parts)
        }
    }
}
