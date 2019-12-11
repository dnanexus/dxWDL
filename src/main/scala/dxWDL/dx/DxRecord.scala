package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

case class DxRecordDescribe(project : String,
                            id  : String,
                            name : String,
                            folder: String,
                            created : Long,
                            modified : Long,
                            properties: Option[Map[String, String]],
                            details : Option[JsValue]) extends DxObjectDescribe

case class DxRecord(id : String,
                    project : Option[DxProject]) extends DxDataObject {
    def describe(fields : Set[Field.Value] = Set.empty) : DxRecordDescribe = {
        val projSpec = DxObject.maybeSpecifyProject(project)
        val defaultFields = Set(Field.Project,
                                Field.Id,
                                Field.Name,
                                Field.Folder,
                                Field.Created,
                                Field.Modified)
        val allFields = fields ++ defaultFields
        val request = JsObject(projSpec + "fields" -> DxObject.requestFields(allFields))
        val response = DXAPI.recordDescribe(id,
                                            DxUtils.jsonNodeOfJsValue(request),
                                            classOf[JsonNode],
                                            DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified") match {
            case Seq(JsString(projectId), JsString(id),
                     JsString(name), JsString(folder),
                     JsNumber(created), JsNumber(modified)) =>
                DxRecordDescribe(projectId,
                                 id,
                                 name,
                                 folder,
                                 created.toLong,
                                 modified.toLong,
                                 None,
                                 None)
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        desc.copy(details = details, properties = props)
    }
}

object DxRecord {
    def getInstance(id : String) : DxRecord = {
         DxDataObject.getInstance(id) match {
             case r : DxRecord => r
             case _ =>
                 throw new IllegalArgumentException(s"${id} isn't a record")
         }
    }
}
