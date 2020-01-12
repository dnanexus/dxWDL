package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

case class DxJobDescribe(
    project: String,
    id: String,
    name: String,
    created: Long,
    modified: Long,
    properties: Option[Map[String, String]],
    details: Option[JsValue],
    applet: DxApplet,
    parentJob: Option[DxJob],
    analysis: Option[DxAnalysis]
) extends DxObjectDescribe

case class DxJob(id: String, project: Option[DxProject] = None) extends DxObject with DxExecution {
  def describe(fields: Set[Field.Value] = Set.empty): DxJobDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields = Set(
      Field.Project,
      Field.Id,
      Field.Name,
      Field.Created,
      Field.Modified,
      Field.Applet,
      Field.ParentJob,
      Field.Analysis
    )
    val allFields = fields ++ defaultFields
    val request = JsObject(
      projSpec + ("fields" -> DxObject.requestFields(allFields))
    )
    val response = DXAPI.analysisDescribe(
      id,
      DxUtils.jsonNodeOfJsValue(request),
      classOf[JsonNode],
      DxUtils.dxEnv
    )
    val descJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    val desc =
      descJs.asJsObject.getFields(
        "project",
        "id",
        "name",
        "created",
        "modified",
        "applet"
      ) match {
        case Seq(
            JsString(project),
            JsString(id),
            JsString(name),
            JsNumber(created),
            JsNumber(modified),
            JsString(applet)
            ) =>
          DxJobDescribe(
            project,
            id,
            name,
            created.toLong,
            modified.toLong,
            None,
            None,
            DxApplet.getInstance(applet),
            None,
            None
          )
        case _ =>
          throw new Exception(s"Malformed JSON ${descJs}")
      }

    val details = descJs.asJsObject.fields.get("details")
    val props = descJs.asJsObject.fields
      .get("properties")
      .map(DxObject.parseJsonProperties)
    val parentJob: Option[DxJob] =
      descJs.asJsObject.fields.get("parentJob") match {
        case None              => None
        case Some(JsNull)      => None
        case Some(JsString(x)) => Some(DxJob.getInstance(x))
        case Some(other)       => throw new Exception(s"should be a job ${other}")
      }
    val analysis = descJs.asJsObject.fields.get("analysis") match {
      case None              => None
      case Some(JsNull)      => None
      case Some(JsString(x)) => Some(DxAnalysis.getInstance(x))
      case Some(other)       => throw new Exception(s"should be an analysis ${other}")
    }
    desc.copy(
      details = details,
      properties = props,
      parentJob = parentJob,
      analysis = analysis
    )
  }
}

object DxJob {
  def getInstance(id: String): DxJob = {
    DxObject.getInstance(id, None) match {
      case j: DxJob => j
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't a job")
    }
  }

  def getInstance(id: String, project: DxProject): DxJob = {
    DxObject.getInstance(id, Some(project)) match {
      case j: DxJob => j
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't a job")
    }
  }
}
