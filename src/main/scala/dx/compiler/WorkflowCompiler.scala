package dx.compiler

import dx.api.{DxApi, DxUtils}
import dx.core.Constants
import dx.core.ir.{
  EmptyInput,
  ExecutableLink,
  IORef,
  Level,
  LinkInput,
  Parameter,
  ParameterLinkSerializer,
  ParameterLinkStage,
  ParameterLinkWorkflowInput,
  StageInput,
  StaticInput,
  Workflow,
  WorkflowInput
}
import wdlTools.util.CodecUtils
import dx.translator.CallableAttributes._
import dx.translator.Extras
import spray.json._
import wdlTools.util.Logger

import scala.collection.immutable.TreeMap

case class WorkflowCompiler(extras: Option[Extras],
                            parameterLinkSerializer: ParameterLinkSerializer,
                            dxApi: DxApi = DxApi.get,
                            logger: Logger = Logger.get)
    extends ExecutableCompiler(extras, parameterLinkSerializer, dxApi) {

  private def workflowInputParameterToNative(parameter: Parameter,
                                             stageInput: StageInput): Vector[JsValue] = {
    // The default value can come from the stageInput or the parameter
    val default = stageInput match {
      case StaticInput(value) => Some(value)
      case _                  => parameter.defaultValue
    }
    inputParameterToNative(parameter.copy(defaultValue = default))
  }

  // Note: a single WDL output can generate one or two JSON outputs.
  private def workflowOutputParameterToNative(parameter: Parameter,
                                              stageInput: StageInput): Vector[JsValue] = {
    val outputSpec: Map[String, Map[String, JsValue]] = outputParameterToNative(parameter).map {
      obj =>
        val name = obj.fields.get("name") match {
          case Some(JsString(name)) => name
          case other                => throw new Exception(s"Unexpected value for 'name' field: ${other}")
        }
        name -> obj.fields
    }.toMap

    val outputSources: Vector[(String, JsValue)] = stageInput match {
      case StaticInput(value) =>
        Vector(parameterLinkSerializer.createConstantField(value, parameter.dxName))
      case LinkInput(dxStage, paramName) =>
        val link = ParameterLinkStage(dxStage, IORef.Output, paramName, parameter.dxType)
        parameterLinkSerializer.createFieldsFromLink(link, parameter.dxName)
      case WorkflowInput(wfParam) =>
        // TODO: if the input has a non-static default, link to the value of the workflow input
        //  (either the user-specified value or the result of evaluting the expression)
        //  - right now this only links to the user-specified value
        val link = ParameterLinkWorkflowInput(wfParam.dxName, parameter.dxType)
        parameterLinkSerializer.createFieldsFromLink(link, parameter.dxName)
      case other =>
        throw new Exception(s"Bad value for stage input ${other}")
    }

    // merge the specification and the output sources
    outputSources.map {
      case (fieldName, output) =>
        JsObject(outputSpec(fieldName) ++ Map("outputSource" -> output))
    }
  }

  /**
    * Translates stage inputs to JSON.
    * @param inputs stage inputs
    * @return
    */
  private def stageInputToNative(inputs: Vector[(Parameter, StageInput)]): JsValue = {
    // sort the inputs, to make the request deterministic
    val jsInputs: TreeMap[String, JsValue] = inputs.foldLeft(TreeMap.empty[String, JsValue]) {
      case (accu, (parameter, stageInput)) =>
        stageInput match {
          case EmptyInput =>
            // We do not have a value for this input at compile time. For compulsory applet inputs,
            // the user will have to fill in a value at runtime.
            accu
          case StaticInput(value) =>
            val fields =
              parameterLinkSerializer.createFields(parameter.dxName, parameter.dxType, value)
            accu ++ fields.toMap
          case LinkInput(dxStage, paramname) =>
            val link = ParameterLinkStage(dxStage, IORef.Output, paramname, parameter.dxType)
            val fields = parameterLinkSerializer.createFieldsFromLink(link, parameter.dxName)
            accu ++ fields.toMap
          case WorkflowInput(wfParam) =>
            val link = ParameterLinkWorkflowInput(wfParam.dxName, parameter.dxType)
            val fields = parameterLinkSerializer.createFieldsFromLink(link, parameter.dxName)
            accu ++ fields.toMap
        }
    }
    JsObject(jsInputs)
  }

  private def workflowAttributesToNative(
      workflow: Workflow,
      defaultTags: Set[String]
  ): (Map[String, JsValue], Map[String, JsValue]) = {
    val (commonMeta, commonDetails) = callableAttributesToNative(workflow, defaultTags)
    val workflowMeta = workflow.attributes.collect {
      // These will be implemented in a future PR
      case CallNamesAttribute(_) =>
        throw new NotImplementedError()
      case RunOnSingleNodeAttribute(_) =>
        throw new NotImplementedError()
      // These are currently ignored because they only apply to apps
      //case VersionAttribute(text) => Some("version" -> JsString(text))
    }
    (commonMeta ++ workflowMeta, commonDetails)
  }

  def apply(
      workflow: Workflow,
      executableDict: Map[String, CompiledExecutable]
  ): (Map[String, JsValue], JsValue) = {
    logger.trace(s"Building /workflow/new request for ${workflow.name}")
    val stages =
      workflow.stages.foldLeft(Vector.empty[JsValue]) {
        case (stagesReq, stg) =>
          val CompiledExecutable(irApplet, dxExec, _, _) = executableDict(stg.calleeName)
          val linkedInputs = irApplet.inputVars zip stg.inputs
          val inputs = stageInputToNative(linkedInputs)
          // convert the per-stage metadata into JSON
          val stageReqDesc = JsObject(
              Map("id" -> JsString(stg.dxStage.id),
                  "executable" -> JsString(dxExec.id),
                  "name" -> JsString(stg.description),
                  "input" -> inputs)
          )
          stagesReq :+ stageReqDesc
      }
    // build the details JSON
    val defaultTags = Set(Constants.CompilerTag)
    val (wfMeta, wfMetaDetails) = workflowAttributesToNative(workflow, defaultTags)
    // compress and base64 encode the source code
    val sourceDetails = Map(
        Constants.SourceCode -> JsString(
            CodecUtils.gzipAndBase64Encode(workflow.document.toString)
        )
    )
    // links through applets that run workflow fragments
    val transitiveDependencies: Vector[ExecutableLink] =
      workflow.stages.foldLeft(Vector.empty[ExecutableLink]) {
        case (accu, stg) =>
          val CompiledExecutable(_, _, dependencies, _) = executableDict(stg.calleeName)
          accu ++ dependencies
      }
    // link to applets used by the fragments. This notifies the platform that they
    // need to be cloned when copying workflows.
    val dxLinks = transitiveDependencies.map {
      case ExecutableLink(name, _, _, dxExec) =>
        s"link_${name}" -> JsObject(DxUtils.DxLinkKey -> JsString(dxExec.id))
    }.toMap
    val delayDetails = delayWorkspaceDestructionToNative
    // generate the executable tree
    val execTree = ExecutableTree(executableDict).apply(workflow)
    val execTreeDetails = Map(
        "execTree" -> JsString(CodecUtils.gzipAndBase64Encode(execTree.toString))
    )
    val details = wfMetaDetails ++ sourceDetails ++ dxLinks ++ delayDetails ++ execTreeDetails
    // required arguments for API call
    val required = Map(
        "name" -> JsString(workflow.name),
        "stages" -> JsArray(stages),
        "details" -> JsObject(details),
        // Sub-workflow are compiled to hidden objects.
        "hidden" -> JsBoolean(workflow.level == Level.Sub)
    )
    // convert inputs and outputs to spec
    val wfInputOutput: Map[String, JsValue] =
      if (workflow.locked) {
        // Locked workflows have well defined inputs and outputs
        val wfInputSpec: Vector[JsValue] = workflow.inputs
          .sortWith(_._1.name < _._1.name)
          .flatMap {
            case (parameter, stageInput) => workflowInputParameterToNative(parameter, stageInput)
          }
        val wfOutputSpec: Vector[JsValue] = workflow.outputs
          .sortWith(_._1.name < _._1.name)
          .flatMap {
            case (parameter, stageInput) => workflowOutputParameterToNative(parameter, stageInput)
          }
        Map("inputs" -> JsArray(wfInputSpec), "outputs" -> JsArray(wfOutputSpec))
      } else {
        Map.empty
      }
    // look for ignoreReuse in runtime hints and in extras - the later overrides the former
    val ignoreReuse = extras.flatMap(_.ignoreReuse) match {
      case Some(true) => Map("ignoreReuse" -> JsArray(JsString("*")))
      case _          => Map.empty
    }
    // build API request
    val request = required ++ wfInputOutput ++ wfMeta ++ ignoreReuse
    (request, execTree)
  }
}
