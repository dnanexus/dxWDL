package dxWDL.dx

import com.dnanexus.{DXAPI, DXJob, DXAnalysis, DXExecution}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

object DxFindExecutions {

    private def parseOneResult(value : JsValue) : DXExecution = {
        value.asJsObject.fields.get("id") match {
            case None => throw new Exception(s"field id not found in ${value.prettyPrint}")
            case Some(JsString(id)) if id.startsWith("job-") => DXJob.getInstance(id)
            case Some(JsString(id)) if id.startsWith("analysis-") => DXAnalysis.getInstance(id)
            case Some(other) => throw new Exception(s"malformed id field ${other.prettyPrint}")
        }
    }

    private def submitRequest(parentJob: Option[DXJob],
                              cursor: Option[JsValue]) : (Vector[DXExecution], Option[JsValue]) = {
        val parentField = parentJob match {
            case None => Map.empty
            case Some(job) => Map("parentJob" -> JsString(job.getId))
        }
        val cursorField = cursor match {
            case None => Map.empty
            case Some(cursorValue) => Map("starting" -> cursorValue)
        }
        val request = JsObject(parentField ++ cursorField)
        val response = DXAPI.systemFindExecutions(DxUtils.jsonNodeOfJsValue(request),
                                                  classOf[JsonNode],
                                                  DxUtils.dxEnv)
        val repJs : JsValue = DxUtils.jsValueOfJsonNode(response)

        val next : Option[JsValue] = repJs.asJsObject.fields.get("next") match {
            case None => None
            case Some(JsNull) => None
            case Some(other : JsObject) => Some(other)
            case Some(other) => throw new Exception(s"malformed ${other.prettyPrint}")
        }
        val results : Vector[DXExecution] =
            repJs.asJsObject.fields.get("results") match {
                case None => throw new Exception(s"missing results field ${repJs}")
                case Some(JsArray(results)) => results.map(parseOneResult)
                case Some(other) => throw new Exception(s"malformed results field ${other.prettyPrint}")
            }

        (results, next)
    }

    def apply(parentJob: Option[DXJob]) : Vector[DXExecution] = {
        var allResults = Vector.empty[DXExecution]
        var cursor : Option[JsValue] = None
        do {
            val (results, next) = submitRequest(parentJob, cursor)
            allResults = allResults ++ results
            cursor = next
        } while (cursor != None);
        allResults
    }
}
