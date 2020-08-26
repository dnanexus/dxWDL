package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

object DxFindExecutions {
  private def parseOneResult(value: JsValue): (DxExecution, DxObjectDescribe) = {
    val fields = value.asJsObject.fields
    fields.get("id") match {
      case Some(JsString(id)) if id.startsWith("job-") =>
        val job = DxJob.getInstance(id)
        val desc = DxJob.parseDescJson(fields("describe").asJsObject)
        (job, desc)
      case Some(JsString(id)) if id.startsWith("analysis-") =>
        val analysis = DxAnalysis.getInstance(id)
        val desc = DxAnalysis.parseDescJson(fields("describe").asJsObject)
        (analysis, desc)
      case Some(other) =>
        throw new Exception(s"malformed id field ${other.prettyPrint}")
      case None =>
        throw new Exception(s"field id not found in ${value.prettyPrint}")
    }
  }

  private def submitRequest(
      parentJob: Option[DxJob],
      cursor: Option[JsValue],
      describe: Set[Field.Value],
      limit: Option[Int]
  ): (Vector[(DxExecution, DxObjectDescribe)], JsValue) = {
    val parentField: Map[String, JsValue] = parentJob match {
      case None      => Map.empty
      case Some(job) => Map("parentJob" -> JsString(job.getId))
    }
    val cursorField: Map[String, JsValue] = cursor match {
      case None              => Map.empty
      case Some(cursorValue) => Map("starting" -> cursorValue)
    }
    val limitField: Map[String, JsValue] = limit match {
      case None    => Map.empty
      case Some(i) => Map("limit" -> JsNumber(i))
    }
    val describeField: Map[String, JsValue] = if (describe.isEmpty) {
      Map.empty
    } else {
      Map("describe" -> DxObject.requestFields(describe))
    }

    val request = JsObject(parentField ++ cursorField ++ limitField ++ describeField)
    val response = DXAPI.systemFindExecutions(DxUtils.jsonNodeOfJsValue(request),
                                              classOf[JsonNode],
                                              DxUtils.dxEnv)
    val responseJs: JsObject = DxUtils.jsValueOfJsonNode(response).asJsObject
    val results: Vector[(DxExecution, DxObjectDescribe)] =
      responseJs.fields.get("results") match {
        case Some(JsArray(results)) => results.map(parseOneResult)
        case Some(other)            => throw new Exception(s"malformed results field ${other.prettyPrint}")
        case None                   => throw new Exception(s"missing results field ${response}")
      }
    (results, responseJs.fields("next"))
  }

  def apply(parentJob: Option[DxJob],
            describe: Set[Field.Value] = Set.empty,
            limit: Option[Int] = None): Vector[DxExecution] = {

    Iterator
      .unfold[Vector[(DxExecution, DxObjectDescribe)], Option[JsValue]](Some(JsNull)) {
        case None => None
        case Some(cursor) =>
          submitRequest(parentJob, cursor, describe, limit) match {
            case (Vector(), _)     => None
            case (results, JsNull) => Some(results, None)
            case (results, next)   => Some(results, Some(next))
          }
      }
      .toVector
      .flatten
  }
}
