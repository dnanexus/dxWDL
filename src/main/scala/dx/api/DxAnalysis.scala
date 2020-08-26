package dx.api

import spray.json.{JsNull, JsNumber, JsObject, JsString, JsValue}

case class DxAnalysisDescribe(project: String,
                              id: String,
                              name: String,
                              folder: String,
                              created: Long,
                              modified: Long,
                              executableName: Option[String],
                              properties: Option[Map[String, String]],
                              details: Option[JsValue],
                              output: Option[JsValue])
    extends DxObjectDescribe

case class DxAnalysis(dxApi: DxApi, id: String, project: Option[DxProject])
    extends CachingDxObject[DxAnalysisDescribe]
    with DxExecution {
  def describeNoCache(fields: Set[Field.Value] = Set.empty): DxAnalysisDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields =
      Set(Field.Project, Field.Id, Field.Name, Field.Folder, Field.Created, Field.Modified)
    val allFields = fields ++ defaultFields
    val request = projSpec + ("fields" -> DxObject.requestFields(allFields))
    val descJs = dxApi.analysisDescribe(id, request)
    DxAnalysis.parseDescribeJson(descJs)
  }

  def setProperties(props: Map[String, String]): Unit = {
    val request = Map(
        "properties" -> JsObject(props.view.mapValues(s => JsString(s)).toMap)
    )
    dxApi.analysisSetProperties(id, request)
  }
}

object DxAnalysis {
  def parseDescribeJson(descJs: JsObject): DxAnalysisDescribe = {
    val desc = descJs.getFields("project", "id", "name", "folder", "created", "modified") match {
      case Seq(JsString(project),
               JsString(id),
               JsString(name),
               JsString(folder),
               JsNumber(created),
               JsNumber(modified)) =>
        DxAnalysisDescribe(project,
                           id,
                           name,
                           folder,
                           created.toLong,
                           modified.toLong,
                           None,
                           None,
                           None,
                           None)
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    // optional fields
    val executableName = descJs.fields.get("executableName") match {
      case Some(JsString(x))   => Some(x)
      case Some(JsNull) | None => None
      case Some(other)         => throw new Exception(s"Invalid executable name ${other}")
    }
    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    val output = descJs.fields.get("output")

    desc.copy(executableName = executableName,
              details = details,
              properties = props,
              output = output)
  }
}
