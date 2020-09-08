package dx.executor

import dx.core.{Native, getVersion}
import dx.core.ir.{ExecutableLink, Parameter, ParameterLink, Type, TypeSerde, Value}
import dx.executor.WorkflowAction.WorkflowAction
import dx.executor.wdl.WdlWorkflowSupportFactory
import spray.json.{JsArray, JsNumber, JsObject}
import wdlTools.util.Enum

object WorkflowAction extends Enum {
  type WorkflowAction = Value
  val Inputs, Outputs, OutputReorg, CustomReorgOutputs, Run, Continue, Collect = Value
}

abstract class WorkflowSupport(jobMeta: JobMeta) {
  def typeAliases: Map[String, Type]

  lazy val execLinkInfo: Map[String, ExecutableLink] =
    jobMeta.executableDetails.get("execLinkInfo") match {
      case Some(JsObject(fields)) =>
        fields.map {
          case (key, link) =>
            key -> ExecutableLink.deserialize(link, typeAliases, jobMeta.dxApi)
        }
      case None =>
        Map.empty
      case other =>
        throw new Exception(s"Bad value ${other}")
    }

  lazy val blockPath: Vector[Int] = jobMeta.executableDetails.get("blockPath") match {
    case Some(JsArray(arr)) =>
      arr.map {
        case JsNumber(n) => n.toInt
        case _           => throw new Exception("Bad value ${arr}")
      }
    case None =>
      Vector.empty
    case other =>
      throw new Exception(s"Bad value ${other}")
  }

  lazy val fqnDictTypes: Map[String, Type] =
    jobMeta.executableDetails.get(Native.WfFragmentInputs) match {
      case Some(JsObject(fields)) =>
        fields.map {
          case (key, value) =>
            // Transform back to a fully qualified name with dots
            val keyDecoded = Parameter.decodeDots(key)
            val wdlType = TypeSerde.deserialize(value, typeAliases)
            keyDecoded -> wdlType
          case other => throw new Exception(s"Bad value ${other}")
        }
      case other =>
        throw new Exception(s"Bad value ${other}")
    }
}

trait WorkflowSupportFactory {
  def create(jobMeta: JobMeta): Option[WorkflowSupport]
}

object WorkflowExecutor {
  val workflowSupportFactories: Vector[WorkflowSupportFactory] = Vector(
      WdlWorkflowSupportFactory()
  )

  def createWorkflowSupport(jobMeta: JobMeta): WorkflowSupport = {
    workflowSupportFactories
      .collectFirst { factory =>
        factory.create(jobMeta) match {
          case Some(executor) => executor
        }
      }
      .getOrElse(
          throw new Exception("Cannot determine language/version from source code")
      )
  }
}

case class WorkflowExecutor(jobMeta: JobMeta) {
  private val workflowSupport: WorkflowSupport = WorkflowExecutor.createWorkflowSupport(jobMeta)
  private val logger = jobMeta.logger

  def reorganizeOutputsDefault(): Map[String, (Type, Value)] = {
    logger.traceLimited(s"dxWDL version: ${getVersion}")

    val inputs = jobMeta.jsInputs.collect {
      case (name, jsValue) if !name.endsWith(ParameterLink.FlatFilesSuffix) =>
        val fqn = Parameter.decodeDots(name)
        val irType = workflowSupport.fqnDictTypes.getOrElse(
            fqn,
            throw new Exception(s"Did not find variable ${fqn} (${name}) in the block environment")
        )
        val irValue = jobMeta.inputDeserializer.deserializeInputWithType(jsValue, irType)
        fqn -> (irType, irValue)
    }

    val wfReorg = WorkflowOutputReorg(jobMeta.dxApi)
    val refDxFiles = fragInputOutput.findRefDxFiles(inputsRaw, executableDetails)
    wfReorg.apply(refDxFiles)

    Map.empty
  }

  def apply(action: WorkflowAction): String = {
    try {
      val outputs: Map[String, (Type, Value)] = action match {
        case WorkflowAction.Inputs             =>
        case WorkflowAction.Run                =>
        case WorkflowAction.Continue           =>
        case WorkflowAction.Collect            =>
        case WorkflowAction.Outputs            =>
        case WorkflowAction.CustomReorgOutputs =>
        case WorkflowAction.OutputReorg =>
          reorganizeOutputsDefault()
        case _ =>
          throw new Exception(s"Illegal workflow fragment operation ${action}")
      }
      jobMeta.writeOutputs(outputs)
      s"success ${action}"
    } catch {
      case e: Throwable =>
        workflowMeta.error(e)
        throw e
    }
  }
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
