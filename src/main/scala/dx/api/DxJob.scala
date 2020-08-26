package dx.api

import spray.json._

case class DxJobDescribe(project: String,
                         id: String,
                         name: String,
                         created: Long,
                         modified: Long,
                         properties: Option[Map[String, String]],
                         details: Option[JsValue],
                         executableName: String,
                         parentJob: Option[DxJob],
                         analysis: Option[DxAnalysis],
                         output: Option[JsValue])
    extends DxObjectDescribe

case class DxJob(dxApi: DxApi, id: String, project: Option[DxProject] = None)
    extends CachingDxObject[DxJobDescribe]
    with DxExecution {
  def describeNoCache(fields: Set[Field.Value] = Set.empty): DxJobDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields = Set(Field.Project,
                            Field.Id,
                            Field.Name,
                            Field.Created,
                            Field.Modified,
                            Field.ExecutableName,
                            Field.ParentJob,
                            Field.Analysis)
    val allFields = fields ++ defaultFields
    val descJs =
      dxApi.jobDescribe(id, projSpec + ("fields" -> DxObject.requestFields(allFields)))
    DxJob.parseDescribeJson(descJs, dxApi)
  }
}

object DxJob {
  def parseDescribeJson(descJs: JsObject, dxApi: DxApi = DxApi.get): DxJobDescribe = {
    val desc =
      descJs.getFields("project", "id", "name", "created", "modified", "executableName") match {
        case Seq(JsString(project),
                 JsString(id),
                 JsString(name),
                 JsNumber(created),
                 JsNumber(modified),
                 JsString(executableName)) =>
          DxJobDescribe(project,
                        id,
                        name,
                        created.toLong,
                        modified.toLong,
                        None,
                        None,
                        executableName,
                        None,
                        None,
                        None)
        case _ =>
          throw new Exception(s"Malformed JSON ${descJs}")
      }
    // optional fields
    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    val parentJob: Option[DxJob] = descJs.fields.get("parentJob") match {
      case Some(JsString(x))   => Some(dxApi.job(x))
      case Some(JsNull) | None => None
      case Some(other)         => throw new Exception(s"should be a job ${other}")
    }
    val analysis = descJs.fields.get("analysis") match {
      case Some(JsString(x))   => Some(dxApi.analysis(x))
      case Some(JsNull) | None => None
      case Some(other)         => throw new Exception(s"should be an analysis ${other}")
    }
    val output = descJs.fields.get("output")
    desc.copy(details = details,
              properties = props,
              parentJob = parentJob,
              analysis = analysis,
              output = output)
  }
}
