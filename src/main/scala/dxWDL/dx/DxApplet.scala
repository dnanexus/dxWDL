package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

case class DxAppletDescribe(project: String,
                            id: String,
                            name: String,
                            folder: String,
                            created: Long,
                            modified: Long,
                            properties: Option[Map[String, String]],
                            details: Option[JsValue],
                            inputSpec: Option[Vector[IOParameter]],
                            outputSpec: Option[Vector[IOParameter]])
    extends DxObjectDescribe

case class DxApplet(id: String, project: Option[DxProject]) extends DxExecutable {
  def describe(fields: Set[Field.Value] = Set.empty): DxAppletDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields = Set(Field.Project,
                            Field.Id,
                            Field.Name,
                            Field.Folder,
                            Field.Created,
                            Field.Modified,
                            Field.InputSpec,
                            Field.OutputSpec)
    val allFields = fields ++ defaultFields
    val request = JsObject(projSpec + ("fields" -> DxObject.requestFields(allFields)))
    val response =
      DXAPI.appletDescribe(id, DxUtils.jsonNodeOfJsValue(request), classOf[JsonNode], DxUtils.dxEnv)
    val descJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    val desc = descJs.asJsObject.getFields("project",
                                           "id",
                                           "name",
                                           "folder",
                                           "created",
                                           "modified",
                                           "inputSpec",
                                           "outputSpec") match {
      case Seq(JsString(project),
               JsString(id),
               JsString(name),
               JsString(folder),
               JsNumber(created),
               JsNumber(modified),
               JsArray(inputSpec),
               JsArray(outputSpec)) =>
        DxAppletDescribe(
            project,
            id,
            name,
            folder,
            created.toLong,
            modified.toLong,
            None,
            None,
            Some(DxObject.parseIOSpec(inputSpec.toVector)),
            Some(DxObject.parseIOSpec(outputSpec.toVector))
        )
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    val details = descJs.asJsObject.fields.get("details")
    val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
    desc.copy(details = details, properties = props)
  }
}

object DxApplet {
  def getInstance(id: String): DxApplet = {
    DxObject.getInstance(id) match {
      case a: DxApplet => a
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't an applet")
    }
  }

  def getInstance(id: String, project: DxProject): DxApplet = {
    DxObject.getInstance(id, Some(project)) match {
      case a: DxApplet => a
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't an applet")
    }
  }
}
