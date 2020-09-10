package dx.api

import spray.json._

case class DxWorkflowStageDesc(id: String, executable: String, name: String, input: JsValue)

// A stand in for the DxWorkflow.Stage inner class (we don't have a constructor for it)
case class DxWorkflowStage(id: String) {
  def getId: String = id

  def getInputReference(inputName: String): JsValue = {
    JsObject(
        DxUtils.DxLinkKey -> JsObject("stage" -> JsString(id), "inputField" -> JsString(inputName))
    )
  }

  def getOutputReference(outputName: String): JsValue = {
    JsObject(
        DxUtils.DxLinkKey -> JsObject("stage" -> JsString(id),
                                      "outputField" -> JsString(outputName))
    )
  }
}

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

case class DxWorkflow(dxApi: DxApi, id: String, project: Option[DxProject])
    extends DxExecutable
    with DxDataObject {
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
    val descJs = dxApi.workflowDescribe(
        id,
        projSpec
          + ("fields" -> DxObject.requestFields(allFields))
          + ("defaultFields" -> JsBoolean(true))
    )
    val desc = descJs.getFields("project",
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
            Some(IOParameter.parseIOSpec(dxApi, inputSpec)),
            Some(IOParameter.parseIOSpec(dxApi, outputSpec)),
            None
        )
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    val descFields: Map[String, JsValue] = descJs.fields
    val details = descFields.get("details")
    val props = descFields.get("properties").map(DxObject.parseJsonProperties)
    val stages = descFields.get("stages").map(parseStages)
    val description = descFields.get("description").flatMap(unwrapString)
    val summary = descFields.get("summary").flatMap(unwrapString)
    val title = descFields.get("title").flatMap(unwrapString)
    val types = descFields.get("types").flatMap(unwrapStringArray)
    val tags = descFields.get("tags").flatMap(unwrapStringArray)
    val inputs = descFields.get("inputs") match {
      case Some(JsArray(inps)) => Some(IOParameter.parseIOSpec(dxApi, inps))
      case _                   => None
    }
    val outputs = descFields.get("outputs") match {
      case Some(JsArray(outs)) => Some(IOParameter.parseIOSpec(dxApi, outs))
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
    dxApi.workflowClose(id)
  }

  def newRun(name: String,
             input: JsValue,
             details: Option[JsValue] = None,
             delayWorkspaceDestruction: Option[Boolean] = None): DxAnalysis = {
    val req = Map("name" -> JsString(name), "input" -> input.asJsObject)
    val detailsFields = details match {
      case Some(jsv) => Map("details" -> jsv)
      case None      => Map.empty
    }
    val dwd = delayWorkspaceDestruction match {
      case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
      case _          => Map.empty
    }
    val repJs = dxApi.workflowRun(id, req ++ detailsFields ++ dwd)
    repJs.fields.get("id") match {
      case None =>
        throw new Exception("id not returned in response")
      case Some(JsString(x)) =>
        dxApi.analysis(x)
      case Some(other) =>
        throw new Exception(s"malformed json response ${other}")
    }
  }
}
