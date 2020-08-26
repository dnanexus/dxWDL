package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

import scala.collection.AbstractIterator

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
      cursor: JsValue,
      describe: Set[Field.Value],
      limit: Option[Int]
  ): (Vector[(DxExecution, DxObjectDescribe)], JsValue) = {
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
            limit: Option[Int] = None): Vector[(DxExecution, DxObjectDescribe)] = {

    new UnfoldIterator[Vector[(DxExecution, DxObjectDescribe)], Option[JsValue]](Some(JsNull))({
      case None => None
      case Some(cursor: JsValue) =>
        submitRequest(parentJob, cursor, describe, limit) match {
          case (Vector(), _)     => None
          case (results, JsNull) => Some(results, None)
          case (results, next)   => Some(results, Some(next))
        }
    }).toVector.flatten
  }
}

// copy the UnfoldIterator from scala 2.13
private final class UnfoldIterator[A, S](init: S)(f: S => Option[(A, S)])
    extends AbstractIterator[A] {
  private[this] var state: S = init
  private[this] var nextResult: Option[(A, S)] = null

  override def hasNext: Boolean = {
    if (nextResult eq null) {
      nextResult = {
        val res = f(state)
        if (res eq null) throw new NullPointerException("null during unfold")
        res
      }
      state = null.asInstanceOf[S] // allow GC
    }
    nextResult.isDefined
  }

  override def next(): A = {
    if (hasNext) {
      val (value, newState) = nextResult.get
      state = newState
      nextResult = null
      value
    } else Iterator.empty.next()
  }
}
