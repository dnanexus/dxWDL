package dx.api

import spray.json._

case class DxJobDescribe(project: String,
                         id: String,
                         name: String,
                         created: Long,
                         modified: Long,
                         properties: Option[Map[String, String]],
                         details: Option[JsValue],
                         applet: DxApplet,
                         parentJob: Option[DxJob],
                         analysis: Option[DxAnalysis])
    extends DxObjectDescribe

case class DxJob(dxApi: DxApi, id: String, project: Option[DxProject] = None) extends DxExecution {
  def describe(fields: Set[Field.Value] = Set.empty): DxJobDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields = Set(Field.Project,
                            Field.Id,
                            Field.Name,
                            Field.Created,
                            Field.Modified,
                            Field.Applet,
                            Field.ParentJob,
                            Field.Analysis)
    val allFields = fields ++ defaultFields
    val descJs =
      dxApi.analysisDescribe(id, projSpec + ("fields" -> DxObject.requestFields(allFields)))
    val desc =
      descJs.getFields("project", "id", "name", "created", "modified", "applet") match {
        case Seq(JsString(project),
                 JsString(id),
                 JsString(name),
                 JsNumber(created),
                 JsNumber(modified),
                 JsString(applet)) =>
          DxJobDescribe(project,
                        id,
                        name,
                        created.toLong,
                        modified.toLong,
                        None,
                        None,
                        dxApi.applet(applet),
                        None,
                        None)
        case _ =>
          throw new Exception(s"Malformed JSON ${descJs}")
      }

    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    val parentJob: Option[DxJob] = descJs.fields.get("parentJob") match {
      case None              => None
      case Some(JsNull)      => None
      case Some(JsString(x)) => Some(dxApi.job(x))
      case Some(other)       => throw new Exception(s"should be a job ${other}")
    }
    val analysis = descJs.fields.get("analysis") match {
      case None              => None
      case Some(JsNull)      => None
      case Some(JsString(x)) => Some(dxApi.analysis(x))
      case Some(other)       => throw new Exception(s"should be an analysis ${other}")
    }
    desc.copy(details = details, properties = props, parentJob = parentJob, analysis = analysis)
  }
}
