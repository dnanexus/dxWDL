package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

case class DxWorkflowStageDesc(id: String, executable: String, name: String, input: JsValue)

case class DxWorkflowDescribe(project: String,
                              id: String,
                              name: String,
                              folder: String,
                              created: Long,
                              modified: Long,
                              properties: Option[Map[String, String]],
                              details: Option[JsValue],
                              inputSpec: Option[Vector[IOParameter]],
                              outputSpec: Option[Vector[IOParameter]],
                              stages: Option[Vector[DxWorkflowStageDesc]])
    extends DxObjectDescribe

case class DxWorkflow(id: String, project: Option[DxProject]) extends DxExecutable {
  private def parseStages(jsv: JsValue): Vector[DxWorkflowStageDesc] = {
    val jsVec = jsv match {
      case JsArray(a) => a
      case other      => throw new Exception(s"Malfored JSON ${other}")
    }
    jsVec.map {
      case jsv2 =>
        val stage = jsv2.asJsObject.getFields("id", "executable", "name", "input") match {
          case Seq(JsString(id), JsString(exec), JsString(name), input) =>
            DxWorkflowStageDesc(id, exec, name, input)
          case other =>
            throw new Exception(s"Malfored JSON ${other}")
        }
        stage
    }.toVector
  }

  def describe(fields: Set[Field.Value] = Set.empty): DxWorkflowDescribe = {
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
    val descJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    val desc = descJs.asJsObject.getFields("project",
                                           "id",
                                           "name",
                                           "folder",
                                           "created",
                                           "modified",
                                           "inputSpec",
                                           "outputSpec") match {
      case Seq(JsString(projectId),
               JsString(id),
               JsString(name),
               JsString(folder),
               JsNumber(created),
               JsNumber(modified),
               JsArray(inputSpec),
               JsArray(outputSpec)) =>
        DxWorkflowDescribe(
            projectId,
            id,
            name,
            folder,
            created.toLong,
            modified.toLong,
            None,
            None,
            Some(DxObject.parseIOSpec(inputSpec.toVector)),
            Some(DxObject.parseIOSpec(outputSpec.toVector)),
            None
        )
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    val details = descJs.asJsObject.fields.get("details")
    val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
    val stages = descJs.asJsObject.fields.get("stages").map(parseStages)
    desc.copy(details = details, properties = props, stages = stages)
  }

  def close(): Unit = {
    DXAPI.workflowClose(id, classOf[JsonNode], DxUtils.dxEnv)
  }

  def newRun(name: String,
             input: JsValue,
             delayWorkspaceDestruction: Option[Boolean] = None): DxAnalysis = {
    val req = Map("name" -> JsString(name), "input" -> input.asJsObject)
    val dwd = delayWorkspaceDestruction match {
      case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
      case _          => Map.empty
    }
    val request = JsObject(req ++ dwd)
    val response =
      DXAPI.workflowRun(id, DxUtils.jsonNodeOfJsValue(request), classOf[JsonNode], DxUtils.dxEnv)
    val repJs: JsValue = DxUtils.jsValueOfJsonNode(response)
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
  def getInstance(id: String): DxWorkflow = {
    DxObject.getInstance(id) match {
      case wf: DxWorkflow => wf
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't a workflow")
    }
  }

  def getInstance(id: String, project: DxProject): DxWorkflow = {
    DxObject.getInstance(id, Some(project)) match {
      case wf: DxWorkflow => wf
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't a workflow")
    }
  }
}
