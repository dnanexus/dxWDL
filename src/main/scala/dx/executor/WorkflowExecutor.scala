package dx.executor

import dx.core.ir.{Type, Value}
import dx.executor.WorkflowAction.WorkflowAction
import wdlTools.util.Enum

object WorkflowAction extends Enum {
  type WorkflowAction = Value
  val Inputs, Outputs, OutputReorg, CustomReorgOutputs, Run, Continue, Collect = Value
}

class WorkflowExecutor {
  val wfFragExecutor = executorFactories
    .collectFirst { factory =>
      factory.createWorkflowFragmentExecutor match {
        case Some(executor) => executor
      }
    }
    .getOrElse(
        throw new Exception("Cannot determine language/version from source code")
    )

  def reorganizeOutputsDefault(): Map[String, (Type, Value)] = {}

  def apply(action: WorkflowAction): String = {
    val outputs: Map[String, (Type, Value)] = action match {
      case ExecutorAction.WfInputs             =>
      case ExecutorAction.WfFragment           =>
      case ExecutorAction.WfScatterContinue    =>
      case ExecutorAction.WfScatterCollect     =>
      case ExecutorAction.WfOutputs            =>
      case ExecutorAction.WfCustomReorgOutputs =>
      case ExecutorAction.WfOutputReorg =>
        wfFragExecutor.reorganizeOutputsDefault()
      case _ =>
        throw new Exception(s"Illegal workflow fragment operation ${action}")
    }

    jobMeta.writeOutputs(outputs)
    s"success ${action}"
  }
}

trait WorkflowExecutorFactory {
  def create(): Option[WorkflowExecutor]
}
/**
private def workflowFragAction(
      action: ExecutorAction.Value,
      language: Option[Language],
      sourceCode: String,
      instanceTypeDB: InstanceTypeDB,
      executableDetails: Map[String, JsValue],
      jobInputPath: Path,
      jobOutputPath: Path,
      jobDesc: DxJobDescribe,
      dxPathConfig: DxWorkerPaths,
      fileResolver: FileSourceResolver,
      dxFileDescCache: DxFileDescCache,
      defaultRuntimeAttributes: Map[String, Value],
      delayWorkspaceDestruction: Option[Boolean],
      dxApi: DxApi
  ): Termination = {

    // Parse the inputs, convert to WDL values. Delay downloading files
    // from the platform, we may not need to access them.
    val inputsRaw: String = FileUtils.readFileContent(jobInputPath).parseJson
    val (wf, taskDir, typeAliases, document) =
      ParseSource(dxApi).parseWdlWorkflow(sourceCode)
    val wdlVarLinksConverter =
      WdlDxLinkSerde(dxApi, fileResolver, dxFileDescCache, typeAliases)
    val evaluator = createEvaluator(dxPathConfig, fileResolver, document.version.value)
    val jobInputOutput = JobInputOutput(dxPathConfig,
                                        fileResolver,
                                        dxFileDescCache,
                                        wdlVarLinksConverter,
                                        dxApi,
                                        evaluator)
    // setup the utility directories that the frag-runner employs
    val fragInputOutput = WfFragInputOutput(typeAliases, wdlVarLinksConverter, dxApi)

    // process the inputs
    val fragInputs = fragInputOutput.loadInputs(inputsRaw, executableDetails)
    val outputFields: Map[String, JsValue] =
      action match {
        case ExecutorAction.WfFragment | ExecutorAction.WfScatterContinue | ExecutorAction.WfScatterCollect =>

          val scatterSize = executableDetails.get(Native.ScatterChunkSize) match {
            case Some(JsNumber(n)) => n.toIntExact
            case None              => Native.JobPerScatterDefault
            case other =>
              throw new Exception(s"Invalid value ${other} for ${Native.ScatterChunkSize}")
          }
          val fragRunner = WfFragRunner(
              wf,
              taskDir,
              typeAliases,
              document,
              instanceTypeDB,
              fragInputs.execLinkInfo,
              dxPathConfig,
              fileResolver,
              wdlVarLinksConverter,
              jobInputOutput,
              inputsRaw,
              fragInputOutput,
              defaultRuntimeAttributes,
              delayWorkspaceDestruction,
              scatterSize,
              dxApi,
              evaluator
          )
          fragRunner.apply(fragInputs.blockPath, fragInputs.env, mode)
        case ExecutorAction.WfInputs =>
          val wfInputs = WfInputs(wf, document, wdlVarLinksConverter, dxApi, evaluator)
          wfInputs.apply(fragInputs.env)
        case ExecutorAction.WfOutputs =>
          val wfOutputs = WfOutputs(wf, document, wdlVarLinksConverter, dxApi, evaluator)
          wfOutputs.apply(fragInputs.env)

        case ExecutorAction.WfCustomReorgOutputs =>
          val wfCustomReorgOutputs = WfOutputs(
              wf,
              document,
              wdlVarLinksConverter,
              dxApi,
              evaluator
          )
          // add ___reconf_status as output.
          wfCustomReorgOutputs.apply(fragInputs.env, addStatus = true)

        case ExecutorAction.WfOutputReorg =>
          val wfReorg = WorkflowOutputReorg(dxApi)
          val refDxFiles = fragInputOutput.findRefDxFiles(inputsRaw, executableDetails)
          wfReorg.apply(refDxFiles)

        case _ =>
          throw new Exception(s"Illegal workflow fragment operation ${action}")
      }

  }

  */
