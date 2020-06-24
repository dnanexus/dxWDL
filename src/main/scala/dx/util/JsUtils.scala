package dx.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import spray.json._

import scala.collection.immutable.TreeMap

object JsUtils {
  // Make a JSON value deterministically sorted.  This is used to
  // ensure that the checksum does not change when maps
  // are ordered in different ways.
  //
  // Note: this does not handle the case of arrays that
  // may have different equivalent orderings.
  def makeDeterministic(jsValue: JsValue): JsValue = {
    jsValue match {
      case JsObject(m: Map[String, JsValue]) =>
        // deterministically sort maps by using a tree-map instead
        // a hash-map
        val mTree = m
          .map { case (k, v) => k -> JsUtils.makeDeterministic(v) }
          .to(TreeMap)
        JsObject(mTree)
      case other =>
        other
    }
  }

  // Convert from spray-json to jackson JsonNode
  // Used to convert into the JSON datatype used by dxjava
  private val objMapper: ObjectMapper = new ObjectMapper()

  def jsonNodeOfJsValue(jsValue: JsValue): JsonNode = {
    val s: String = jsValue.prettyPrint
    objMapper.readTree(s)
  }

  // Convert from jackson JsonNode to spray-json
  def jsValueOfJsonNode(jsNode: JsonNode): JsValue = {
    jsNode.toString.parseJson
  }

  // Replace all special json characters from with a white space.
  def sanitize(s: String): String = {
    def sanitizeChar(ch: Char): String = ch match {
      case '}'                     => " "
      case '{'                     => " "
      case '$'                     => " "
      case '/'                     => " "
      case '\\'                    => " "
      case '\"'                    => " "
      case '\''                    => " "
      case _ if ch.isLetterOrDigit => ch.toString
      case _ if ch.isControl       => " "
      case _                       => ch.toString
    }

    if (s != null) {
      s.flatMap(sanitizeChar)
    } else {
      ""
    }
  }

  // Extract an integer fields from a JsObject
  def getJsIntField(js: JsValue, fieldName: String): Int = {
    js.asJsObject.fields.get(fieldName) match {
      case Some(JsNumber(x)) => x.toInt
      case Some(JsString(x)) => x.toInt
      case _                 => throw new Exception(s"Missing field ${fieldName} in JSON ${js.prettyPrint}}")
    }
  }

  def getJsStringField(js: JsValue, fieldName: String): String = {
    js.asJsObject.fields.get(fieldName) match {
      case Some(JsNumber(x)) => x.toString
      case Some(JsString(x)) => x
      case _                 => throw new Exception(s"Missing field ${fieldName} in JSON ${js.prettyPrint}}")
    }
  }

  def getJsField(js: JsValue, fieldName: String): JsValue = {
    js.asJsObject.fields.get(fieldName) match {
      case Some(x: JsValue) => x
      case None             => throw new Exception(s"Missing field ${fieldName} in ${js.prettyPrint}")
    }
  }
}
