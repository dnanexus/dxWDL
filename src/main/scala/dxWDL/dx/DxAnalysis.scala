package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

import dxWDL.base.Utils

case class DxAnalysisDescribe(project : String,
                              id  : String,
                              name : String,
                              folder : String,
                              created : Long,
                              modified : Long,
                              properties: Option[Map[String, String]],
                              details : Option[JsValue]) extends DxObjectDescribe

case class DxAnalysis(id : String,
                      project : Option[DxProject]) extends DxObject with DxExecution {
    def describe(fields : Set[Field.Value] = Set.empty) : DxAnalysisDescribe = {
        val projSpec = DxObject.maybeSpecifyProject(project)
        val defaultFields = Set(Field.Project,
                                Field.Id,
                                Field.Name,
                                Field.Folder,
                                Field.Created,
                                Field.Modified)
        val allFields = fields ++ defaultFields
        val request = JsObject(projSpec +
                                   ("fields" -> DxObject.requestFields(allFields)))
        val response = DXAPI.analysisDescribe(id,
                                              DxUtils.jsonNodeOfJsValue(request),
                                              classOf[JsonNode],
                                              DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified") match {
            case Seq(JsString(project), JsString(id),
                     JsString(name), JsString(folder),
                     JsNumber(created), JsNumber(modified)) =>
                DxAnalysisDescribe(project,
                                   id,
                                   name,
                                   folder,
                                   created.toLong,
                                   modified.toLong,
                                   None,
                                   None)
            case _ =>
                throw new Exception(s"Malformed JSON ${descJs}")
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        desc.copy(details = details, properties = props)
    }

    def setProperties(props: Map[String, String]) : Unit = {
        val request = JsObject(
            "properties" -> JsObject(props.map{ case (k,v) =>
                                         k -> JsString(v)
                                     })
        )
        val response = DXAPI.analysisSetProperties(id,
                                                   DxUtils.jsonNodeOfJsValue(request),
                                                   classOf[JsonNode],
                                                   DxUtils.dxEnv)
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        Utils.ignore(repJs)
    }
}

object DxAnalysis {
    def getInstance(id : String) : DxAnalysis = {
        if (id.startsWith("analysis-"))
            return DxAnalysis(id, None)
        throw new IllegalArgumentException(s"${id} isn't an analysis")
    }
}
