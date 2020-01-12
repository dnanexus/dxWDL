package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

case class DxAppDescribe(
    id: String,
    name: String,
    created: Long,
    modified: Long,
    properties: Option[Map[String, String]],
    details: Option[JsValue],
    inputSpec: Option[Vector[IOParameter]],
    outputSpec: Option[Vector[IOParameter]]
) extends DxObjectDescribe

case class DxApp(id: String) extends DxExecutable {
  def describe(fields: Set[Field.Value] = Set.empty): DxAppDescribe = {
    val defaultFields =
      Set(Field.Id, Field.Name, Field.Created, Field.Modified, Field.InputSpec, Field.OutputSpec)
    val allFields = fields ++ defaultFields
    val request   = JsObject("fields" -> DxObject.requestFields(allFields))
    val response =
      DXAPI.appDescribe(id, DxUtils.jsonNodeOfJsValue(request), classOf[JsonNode], DxUtils.dxEnv)
    val descJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    val desc = descJs.asJsObject.getFields(
      "id",
      "name",
      "created",
      "modified",
      "inputSpec",
      "outputSpec"
    ) match {
      case Seq(
          JsString(id),
          JsString(name),
          JsNumber(created),
          JsNumber(modified),
          JsArray(inputSpec),
          JsArray(outputSpec)
          ) =>
        DxAppDescribe(
          id,
          name,
          created.toLong,
          modified.toLong,
          None,
          None,
          Some(DxObject.parseIOSpec(inputSpec.toVector)),
          Some(DxObject.parseIOSpec(outputSpec.toVector))
        )
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    val details = descJs.asJsObject.fields.get("details")
    val props   = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
    desc.copy(details = details, properties = props)
  }
}

object DxApp {
  def getInstance(id: String): DxApp = {
    DxObject.getInstance(id) match {
      case a: DxApp => a
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't an app")
    }
  }
}
