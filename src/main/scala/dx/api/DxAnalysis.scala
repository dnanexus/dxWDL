package dx.api

import spray.json.{JsNumber, JsObject, JsString, JsValue}

case class DxAnalysisDescribe(project: String,
                              id: String,
                              name: String,
                              folder: String,
                              created: Long,
                              modified: Long,
                              properties: Option[Map[String, String]],
                              details: Option[JsValue])
    extends DxObjectDescribe

case class DxAnalysis(dxApi: DxApi, id: String, project: Option[DxProject]) extends DxExecution {
  def describe(fields: Set[Field.Value] = Set.empty): DxAnalysisDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields =
      Set(Field.Project, Field.Id, Field.Name, Field.Folder, Field.Created, Field.Modified)
    val allFields = fields ++ defaultFields
    val request = projSpec + ("fields" -> DxObject.requestFields(allFields))
    val descJs = dxApi.analysisDescribe(id, request)
    val desc = descJs.getFields("project", "id", "name", "folder", "created", "modified") match {
      case Seq(JsString(project),
               JsString(id),
               JsString(name),
               JsString(folder),
               JsNumber(created),
               JsNumber(modified)) =>
        DxAnalysisDescribe(project, id, name, folder, created.toLong, modified.toLong, None, None)
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    val details = descJs.asJsObject.fields.get("details")
    val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
    desc.copy(details = details, properties = props)
  }

  def setProperties(props: Map[String, String]): Unit = {
    val request = Map(
        "properties" -> JsObject(props.view.mapValues(s => JsString(s)).toMap)
    )
    dxApi.analysisSetProperties(id, request)
  }
}
