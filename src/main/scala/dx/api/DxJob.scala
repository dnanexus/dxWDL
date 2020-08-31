package dx.api

import spray.json._

case class DxJobDescribe(id: String,
                         name: String,
                         project: DxProject,
                         created: Long,
                         modified: Long,
                         properties: Option[Map[String, String]],
                         details: Option[JsValue],
                         executableName: String,
                         parentJob: Option[DxJob],
                         analysis: Option[DxAnalysis],
                         executable: Option[DxExecutable],
                         output: Option[JsValue])
    extends DxObjectDescribe

case class DxJob(dxApi: DxApi, id: String, project: Option[DxProject] = None)
    extends CachingDxObject[DxJobDescribe]
    with DxExecution {
  def describeNoCache(fields: Set[Field.Value] = Set.empty): DxJobDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields = Set(Field.Id,
                            Field.Name,
                            Field.Project,
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
      descJs.getFields("id", "name", "project", "created", "modified", "executableName") match {
        case Seq(JsString(id),
                 JsString(name),
                 JsString(project),
                 JsNumber(created),
                 JsNumber(modified),
                 JsString(executableName)) =>
          DxJobDescribe(id,
                        name,
                        dxApi.project(project),
                        created.toLong,
                        modified.toLong,
                        None,
                        None,
                        executableName,
                        None,
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
    val executable = descJs.fields.get("executable") match {
      case Some(JsString(id))  => Some(dxApi.executable(id, Some(desc.project)))
      case Some(JsNull) | None => None
      case Some(other)         => throw new Exception(s"should be an executable ID ${other}")
    }
    val output = descJs.fields.get("output")
    desc.copy(details = details,
              properties = props,
              parentJob = parentJob,
              analysis = analysis,
              executable = executable,
              output = output)
  }
}
