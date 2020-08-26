package dx.api

import spray.json._

case class DxFindExecutions(dxApi: DxApi) {
  private def parseOneResult(value: JsValue): DxExecution = {
    val fields = value.asJsObject.fields
    fields.get("id") match {
      case Some(JsString(id)) if id.startsWith("job-") =>
        val job = dxApi.job(id)
        if (fields.contains("describe")) {
          job.cacheDescribe(DxJob.parseDescribeJson(fields("describe").asJsObject, dxApi))
        }
        job
      case Some(JsString(id)) if id.startsWith("analysis-") =>
        val analysis = dxApi.analysis(id)
        if (fields.contains("describe")) {
          analysis.cacheDescribe(DxAnalysis.parseDescribeJson(fields("describe").asJsObject))
        }
        analysis
      case Some(other) =>
        throw new Exception(s"malformed id field ${other.prettyPrint}")
      case None =>
        throw new Exception(s"field id not found in ${value.prettyPrint}")
    }
  }

  private def submitRequest(
      parentJob: Option[DxJob],
      cursor: JsValue,
      describe: Set[Field.Value],
      limit: Option[Int]
  ): (Vector[DxExecution], JsValue) = {
    val parentField: Map[String, JsValue] = parentJob match {
      case None      => Map.empty
      case Some(job) => Map("parentJob" -> JsString(job.getId))
    }
    val cursorField: Map[String, JsValue] = cursor match {
      case JsNull      => Map.empty
      case cursorValue => Map("starting" -> cursorValue)
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
    val response = dxApi.findExecutions(parentField ++ cursorField ++ limitField ++ describeField)
    val results: Vector[DxExecution] =
      response.fields.get("results") match {
        case Some(JsArray(results)) => results.map(parseOneResult)
        case Some(other)            => throw new Exception(s"malformed results field ${other.prettyPrint}")
        case None                   => throw new Exception(s"missing results field ${response}")
      }
    (results, response.fields("next"))
  }

  def apply(parentJob: Option[DxJob],
            describe: Set[Field.Value] = Set.empty,
            limit: Option[Int] = None): Vector[DxExecution] = {
    Iterator
      .unfold[Vector[DxExecution], Option[JsValue]](Some(JsNull)) {
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
