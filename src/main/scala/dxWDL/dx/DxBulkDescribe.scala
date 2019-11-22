// Describe a large number of platform objects in bulk.

package dxWDL.dx

import com.dnanexus.DXAPI
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

    private def submitRequest(objIds : Vector[String],
                              extraFields : Vector[String]) : Map[DxObject, DxDescribe] = {
        val requestFields = Map("objects" ->
                                   JsArray(objIds.map{x => JsString(x) }))

        // extra describe options, if specified
        val extraDescribeFields : Map[String, JsValue] =
            if (extraFields.isEmpty) {
                Map.empty
            } else {
                val m = extraFields.map{ fieldName =>
                    fieldName -> JsBoolean(true)
                }.toMap
                Map("classDescribeOptions" -> JsObject(
                        "*" -> JsObject(m)
                    ))
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
            val dxFullDesc = jsv.asJsObject.fields.get("describe") match {
                case None =>
                    throw new Exception(s"Could not describe object ${objIds(i)}")
                case Some(descJs) =>
                    val dxDesc =
                        descJs.asJsObject.getFields("name", "folder", "size", "id", "project", "created", "modified") match {
                            case Seq(JsString(name), JsString(folder),
                                     JsNumber(size), JsString(oid), JsString(projectId),
                                     JsNumber(created), JsNumber(modified)) =>
                                // This could be a container, not a project.
                                val dxContainer = DxContainer.getInstance(projectId)
                                val dxObj = DxUtils.convertToDxObject(oid, Some(dxContainer)).get
                                DxDescribe(name,
                                           folder,
                                           Some(size.toLong),
                                           dxContainer,
                                           dxObj,
                                           created.toLong,
                                           modified.toLong,
                                           Map.empty,
                                           None,
                                           None,
                                           None,
                                           None)
                            case _ =>
                                throw new Exception(s"bad describe object ${descJs}")
                        }

                    // The parts may be empty, only files have it, and we don't always ask for it.
                    val parts = descJs.asJsObject.fields.get("parts").map(parseFileParts)
                    val details = descJs.asJsObject.fields.get("details")
                    dxDesc.copy(parts = parts, details = details)
            }
            dxFullDesc.dxObj -> dxFullDesc
        }.toMap
    }

    // Describe the names of all the files in one batch. This is much more efficient
    // than submitting file describes one-by-one.
    def apply(objIds: Seq[DxObject],
              extraFields : Vector[Field.Value]) : Map[DxObject, DxDescribe] = {
        if (dataObjs.isEmpty) {
            // avoid an unnessary API call; this is important for unit tests
            // that do not have a network connection.
            return Map.empty
        }

        // Limit on number of objects in one API request
        val slices = files.grouped(DXAPI_NUM_OBJECTS_LIMIT).toList

        val extraFields = extraFields.map{
            case Details => "details"
            case Parts => "parts"
        }.toSet.toVector

        // iterate on the ranges
        slices.foldLeft(Map.empty[DxObject, DxDescribe]) {
            case (accu, objRange) =>
                accu ++ submitRequest(objRange.toVector, extraFields)
        }
    }
}
