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
                              stages: Option[Vector[DxWorkflowStageDesc]],
                              title: Option[String] = None,
                              summary: Option[String] = None,
                              description: Option[String] = None,
                              tags: Option[Vector[String]] = None,
                              types: Option[Vector[String]] = None,
                              inputs: Option[Vector[IOParameter]] = None,
                              outputs: Option[Vector[IOParameter]] = None)
    extends DxObjectDescribe

case class DxWorkflow(id: String, project: Option[DxProject]) extends DxExecutable {
  private def parseStages(jsv: JsValue): Vector[DxWorkflowStageDesc] = {
    val jsVec = jsv match {
      case JsArray(a) => a
      case other      => throw new Exception(s"Malfored JSON ${other}")
    }
    jsVec.map { jsv2 =>
      val stage = jsv2.asJsObject.getFields("id", "executable", "name", "input") match {
        case Seq(JsString(id), JsString(exec), JsString(name), input) =>
          DxWorkflowStageDesc(id, exec, name, input)
        case other =>
          throw new Exception(s"Malfored JSON ${other}")
      }
      stage
    }
  }

  def describe(fields: Set[Field.Value] = Set.empty): DxWorkflowDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    // TODO: working around an API bug where describing a workflow and requesting inputSpec
    // and outputSpec as part of fields results in a 500 error. Instead, request default fields,
    // which includes inputSpec and outputSpec.
    val defaultFields = Set(Field.Project,
                            Field.Id,
                            Field.Name,
                            Field.Folder,
                            Field.Created,
                            Field.Modified
                            //Field.InputSpec,
                            //Field.OutputSpec
    )
    val allFields = fields ++ defaultFields
    val request = JsObject(
        projSpec
          + ("fields" -> DxObject.requestFields(allFields))
          + ("defaultFields" -> JsBoolean(true))
    )
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
            Some(DxObject.parseIOSpec(inputSpec)),
            Some(DxObject.parseIOSpec(outputSpec)),
            None
        )
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    val descFields: Map[String, JsValue] = descJs.asJsObject.fields
    val details = descFields.get("details")
    val props = descFields.get("properties").map(DxObject.parseJsonProperties)
    val stages = descFields.get("stages").map(parseStages)
    val description = descFields.get("description").flatMap(unwrapString)
    val summary = descFields.get("summary").flatMap(unwrapString)
    val title = descFields.get("title").flatMap(unwrapString)
    val types = descFields.get("types").flatMap(unwrapStringArray)
    val tags = descFields.get("tags").flatMap(unwrapStringArray)
    val inputs = descFields.get("inputs") match {
      case Some(JsArray(inps)) => Some(DxObject.parseIOSpec(inps))
      case _                   => None
    }
    val outputs = descFields.get("outputs") match {
      case Some(JsArray(outs)) => Some(DxObject.parseIOSpec(outs))
      case _                   => None
    }
    desc.copy(
        details = details,
        properties = props,
        stages = stages,
        description = description,
        summary = summary,
        title = title,
        types = types,
        tags = tags,
        inputs = inputs,
        outputs = outputs
    )
  }

  def unwrapString(jsValue: JsValue): Option[String] = {
    jsValue match {
      case JsString(value) => Some(value)
      case _               => None
    }
  }

  def unwrapStringArray(jsValue: JsValue): Option[Vector[String]] = {
    jsValue match {
      case JsArray(array) => Some(array.flatMap(unwrapString))
      case _              => None
    }
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
