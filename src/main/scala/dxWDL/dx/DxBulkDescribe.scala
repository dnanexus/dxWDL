// Describe a large number of platform objects in bulk.

package dxWDL.dx

import com.dnanexus.exceptions.ResourceNotFoundException
import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

// maximal number of objects in a single API request
import dxWDL.base.Utils.DXAPI_NUM_OBJECTS_LIMIT

object DxBulkDescribe {

    private def submitRequest(objs : Vector[DxFile],
                              extraFields : Vector[String]) : Map[DxFile, DxFileDescribe] = {
        val requestFields = Map("objects" ->
                                   JsArray(objs.map{ x : DxObject => JsString(x.id) }))

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
            val (dxFile, dxFullDesc) = jsv.asJsObject.fields.get("describe") match {
                case None =>
                    throw new ResourceNotFoundException(s""""${objs(i).id}" is not a recognized ID""", 404)
                case Some(descJs) =>
                    val (dxFile, dxDesc) =
                        descJs.asJsObject.getFields("name", "folder", "size", "id", "project", "created", "modified") match {
                            case Seq(JsString(name), JsString(folder),
                                     JsNumber(size), JsString(oid), JsString(projectId),
                                     JsNumber(created), JsNumber(modified)) =>
                                // This could be a container, not a project.
                                val dxContainer = DxProject.getInstance(projectId)
                                val dxFile = DxFile.getInstance(oid, dxContainer)
                                val desc = DxFileDescribe(projectId,
                                                          oid,
                                                          name,
                                                          folder,
                                                          created.toLong,
                                                          modified.toLong,
                                                          size.toLong,
                                                          None,
                                                          None,
                                                          None)
                                (dxFile, desc)
                            case _ =>
                                throw new Exception(s"bad describe object ${descJs}")
                        }

                    // The parts may be empty, only files have it, and we don't always ask for it.
                    val parts = descJs.asJsObject.fields.get("parts").map(DxObject.parseFileParts)
                    val details = descJs.asJsObject.fields.get("details")
                    val dxDescFull = dxDesc.copy(parts = parts, details = details)
                    (dxFile, dxDescFull)
            }
            dxFile -> dxFullDesc
        }.toMap
    }

    // Describe the names of all the files in one batch. This is much more efficient
    // than submitting file describes one-by-one.
    def apply(objs: Vector[DxFile],
              extraFields : Vector[Field.Value] = Vector.empty) : Map[DxFile, DxFileDescribe] = {
        if (objs.isEmpty) {
            // avoid an unnessary API call; this is important for unit tests
            // that do not have a network connection.
            return Map.empty
        }

        // Limit on number of objects in one API request
        val slices = objs.grouped(DXAPI_NUM_OBJECTS_LIMIT).toList

        val extraFieldsStr = extraFields.map{
            case Field.Details => "details"
            case Field.Parts => "parts"
        }.toSet.toVector

        // iterate on the ranges
        slices.foldLeft(Map.empty[DxFile, DxFileDescribe]) {
            case (accu, objRange) =>
                accu ++ submitRequest(objRange.toVector, extraFieldsStr)
        }
    }
}
