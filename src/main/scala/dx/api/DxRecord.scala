package dx.api

import spray.json._

case class DxRecordDescribe(project: String,
                            id: String,
                            name: String,
                            folder: String,
                            created: Long,
                            modified: Long,
                            properties: Option[Map[String, String]],
                            details: Option[JsValue])
    extends DxObjectDescribe

case class DxRecord(dxApi: DxApi, id: String, project: Option[DxProject]) extends DxDataObject {
  def describe(fields: Set[Field.Value] = Set.empty): DxRecordDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields =
      Set(Field.Project, Field.Id, Field.Name, Field.Folder, Field.Created, Field.Modified)
    val allFields = fields ++ defaultFields
    val descJs =
      dxApi.recordDescribe(id, projSpec + ("fields" -> DxObject.requestFields(allFields)))
    val desc =
      descJs.getFields("project", "id", "name", "folder", "created", "modified") match {
        case Seq(JsString(projectId),
                 JsString(id),
                 JsString(name),
                 JsString(folder),
                 JsNumber(created),
                 JsNumber(modified)) =>
          DxRecordDescribe(projectId, id, name, folder, created.toLong, modified.toLong, None, None)
      }

    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    desc.copy(details = details, properties = props)
  }
}
