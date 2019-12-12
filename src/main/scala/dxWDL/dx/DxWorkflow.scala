package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

case class DxWorkflowDescribe(project : String,
                              id  : String,
                              name : String,
                              folder : String,
                              created : Long,
                              modified : Long,
                              properties: Option[Map[String, String]],
                              details : Option[JsValue],
                              inputSpec : Option[Vector[IOParameter]],
                              outputSpec : Option[Vector[IOParameter]]) extends DxObjectDescribe

case class DxWorkflow(id : String,
                      project : Option[DxProject]) extends DxExecutable {
    def describe(fields : Set[Field.Value] = Set.empty) : DxWorkflowDescribe = {
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
        val response = DXAPI.workflowDescribe(id,
                                              DxUtils.jsonNodeOfJsValue(request),
                                              classOf[JsonNode],
                                              DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified",
                                               "inputSpec", "outputSpec") match {
            case Seq(JsString(projectId), JsString(id),
                     JsString(name), JsString(folder),
                     JsNumber(created), JsNumber(modified),
                     JsArray(inputSpec), JsArray(outputSpec)) =>
                DxWorkflowDescribe(projectId,
                                   id,
                                   name,
                                   folder,
                                   created.toLong,
                                   modified.toLong,
                                   None,
                                   None,
                                   Some(DxObject.parseIOSpec(inputSpec.toVector)),
                                   Some(DxObject.parseIOSpec(outputSpec.toVector)))
            case _ =>
                throw new Exception(s"Malformed JSON ${descJs}")
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        desc.copy(details = details, properties = props)
    }

    def close() : Unit = {
        DXAPI.workflowClose(id,
                            classOf[JsonNode],
                            DxUtils.dxEnv)
    }

    def newRun(input : JsValue, name : String) : DxAnalysis = {
        val request = JsObject(
            "name" -> JsString(name),
            "input" -> input.asJsObject)
        val response = DXAPI.workflowRun(id,
                                         DxUtils.jsonNodeOfJsValue(request),
                                         classOf[JsonNode],
                                         DxUtils.dxEnv)
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)
	repJs.asJsObject.fields.get("id") match {
            case None =>
                throw new Exception("id not returned in response")
            case Some(JsString(x)) =>
                DxAnalysis.getInstance(x)
            case Some(other) =>
                throw new Exception(s"malformed json response ${other}")
        }
    }
}

object DxWorkflow {
    def getInstance(id : String) : DxWorkflow = {
        DxDataObject.getInstance(id) match {
             case wf : DxWorkflow => wf
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't a workflow")
        }
    }
}
