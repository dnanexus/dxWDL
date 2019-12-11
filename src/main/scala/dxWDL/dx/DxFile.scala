package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

case class DxFilePart(state: String,
                      size: Long,
                      md5: String)

case class DxFileDescribe(project : String,
                          id : String,
                          name : String,
                          folder: String,
                          created : Long,
                          modified : Long,
                          size : Long,
                          properties : Option[Map[String, String]],
                          details : Option[JsValue],
                          parts : Option[Map[Int, DxFilePart]]) extends DxObjectDescribe

case class DxFile(id : String,
                  project : Option[DxProject]) extends DxDataObject {

    def describe(fields : Set[Field.Value] = Set.empty) : DxFileDescribe = {
        val projSpec = DxObject.maybeSpecifyProject(project)
        val baseFields = DxObject.requestFields(fields)
        val allFields = baseFields ++ Map("project" -> JsTrue,
                                          "folder"  -> JsTrue,
                                          "size" -> JsTrue)
        val request = JsObject(projSpec + ("fields" -> JsObject(allFields)))
        val response = DXAPI.fileDescribe(id,
                                          DxUtils.jsonNodeOfJsValue(request),
                                          classOf[JsonNode],
                                          DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified", "size") match {
            case Seq(JsString(project), JsString(id),
                     JsString(name), JsString(folder), JsNumber(created), JsNumber(modified), JsNumber(size)) =>
                DxFileDescribe(project,
                               id,
                               name,
                               folder,
                               created.toLong,
                               modified.toLong,
                               size.toLong,
                               None,
                               None,
                               None)
            case _ =>
                throw new Exception(s"Malformed JSON ${descJs}")
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        val parts = descJs.asJsObject.fields.get("parts").map(DxObject.parseFileParts)
        desc.copy(details = details, properties = props, parts = parts)
    }

    def getLinkAsJson : JsValue = {
        project match {
            case None =>
                JsObject("$dnanexus_link" -> JsString(id))
            case Some(p) =>
                JsObject( "$dnanexus_link" -> JsObject(
                             "project" -> JsString(p.id),
                             "id" -> JsString(id)
                         ))
        }
    }
}

object DxFile {
    def getInstance(id : String) : DxFile = {
        DxDataObject.getInstance(id) match {
             case f : DxFile => f
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't a file")
        }
    }

    def getInstance(id : String, project : DxProject) : DxFile = {
        DxDataObject.getInstance(id, Some(project)) match {
             case f : DxFile => f
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't a file")
        }
    }
}
