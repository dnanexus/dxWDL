package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import dxWDL.base._
import spray.json._

case class DxAppDescribe(id: String,
                         name: String,
                         created: Long,
                         modified: Long,
                         properties: Option[Map[String, String]],
                         details: Option[JsValue],
                         inputSpec: Option[Vector[IOParameter]],
                         outputSpec: Option[Vector[IOParameter]])
    extends DxObjectDescribe

case class DxApp(id: String) extends DxExecutable {
  def describe(fields: Set[Field.Value] = Set.empty): DxAppDescribe = {
    val defaultFields =
      Set(Field.Id, Field.Name, Field.Created, Field.Modified, Field.InputSpec, Field.OutputSpec)
    val allFields = fields ++ defaultFields
    val request = JsObject("fields" -> DxObject.requestFields(allFields))
    val response =
      DXAPI.appDescribe(id, DxUtils.jsonNodeOfJsValue(request), classOf[JsonNode], DxUtils.dxEnv)
    val descJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    val desc = descJs.asJsObject
      .getFields("id", "name", "created", "modified", "inputSpec", "outputSpec") match {
      case Seq(JsString(id),
               JsString(name),
               JsNumber(created),
               JsNumber(modified),
               JsArray(inputSpec),
               JsArray(outputSpec)) =>
        DxAppDescribe(id,
                      name,
                      created.toLong,
                      modified.toLong,
                      None,
                      None,
                      Some(DxObject.parseIOSpec(inputSpec)),
                      Some(DxObject.parseIOSpec(outputSpec)))
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    val details = descJs.asJsObject.fields.get("details")
    val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
    desc.copy(details = details, properties = props)
  }

  def newRun(name: String,
             input: JsValue,
             instanceType: Option[String] = None,
             properties: Map[String, JsValue] = Map.empty,
             delayWorkspaceDestruction: Option[Boolean] = None): DxJob = {
    val fields = Map(
        "name" -> JsString(name),
        "input" -> input
    )
    // If this is a task that specifies the instance type
    // at runtime, launch it in the requested instance.
    val instanceFields = instanceType match {
      case None => Map.empty
      case Some(iType) =>
        Map(
            "systemRequirements" -> JsObject(
                "main" -> JsObject("instanceType" -> JsString(iType))
            )
        )
    }

    val props =
      if (properties.isEmpty)
        Map.empty
      else
        Map("properties" -> JsObject(properties))

    val dwd = delayWorkspaceDestruction match {
      case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
      case _          => Map.empty
    }
    val req = JsObject(fields ++ instanceFields ++ props ++ dwd)
    val retval: JsonNode =
      DXAPI.appRun(this.id, DxUtils.jsonNodeOfJsValue(req), classOf[JsonNode])
    val info: JsValue = DxUtils.jsValueOfJsonNode(retval)
    val id: String = info.asJsObject.fields.get("id") match {
      case Some(JsString(x)) => x
      case _ =>
        throw new AppInternalException(s"Bad format returned from jobNew ${info.prettyPrint}")
    }
    DxJob.getInstance(id)
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
