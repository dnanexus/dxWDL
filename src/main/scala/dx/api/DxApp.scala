package dx.api

import dx.AppInternalException
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

case class DxApp(dxApi: DxApi, id: String)
    extends CachingDxObject[DxAppDescribe]
    with DxExecutable {
  override def describeNoCache(fields: Set[Field.Value] = Set.empty): DxAppDescribe = {
    val defaultFields =
      Set(Field.Id, Field.Name, Field.Created, Field.Modified, Field.InputSpec, Field.OutputSpec)
    val allFields = fields ++ defaultFields
    val descJs = dxApi.appDescribe(id, Map("fields" -> DxObject.requestFields(allFields)))
    val desc =
      descJs.getFields("id", "name", "created", "modified", "inputSpec", "outputSpec") match {
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
                        Some(IOParameter.parseIOSpec(dxApi, inputSpec)),
                        Some(IOParameter.parseIOSpec(dxApi, outputSpec)))
        case _ =>
          throw new Exception(s"Malformed JSON ${descJs}")
      }
    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
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
    val info = dxApi.appRun(this.id, fields ++ instanceFields ++ props ++ dwd)
    val id: String = info.fields.get("id") match {
      case Some(JsString(x)) => x
      case _ =>
        throw new AppInternalException(s"Bad format returned from jobNew ${info.prettyPrint}")
    }
    dxApi.job(id)
  }
}
