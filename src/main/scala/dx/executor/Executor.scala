package dx.executor

import java.nio.file.{Path, Paths}

import dx.api.DxJob
import dx.core.getVersion
import dx.core.io.{DxWorkerPaths, DxdaManifest, DxfuseManifest}
import dx.executor.ExecutorAction.ExecutorAction
import dx.executor.wdl.WdlExecutorFactory
import spray.json._
import wdlTools.util.{Enum, FileSource, FileUtils, Logger, RealFileSource, SysUtils, TraceLevel}

object ExecutorAction extends Enum {
  type ExecutorAction = Value
  val WfFragment, WfInputs, WfOutputs, WfOutputReorg, WfCustomReorgOutputs, WfScatterContinue,
      WfScatterCollect =
    Value
  val WorkflowFragmentActions =
    Set(WfFragment,
        WfInputs,
        WfOutputs,
        WfOutputReorg,
        WfCustomReorgOutputs,
        WfScatterContinue,
        WfScatterCollect)
  val TaskCheckInstanceType, TaskProlog, TaskInstantiateCommand, TaskEpilog, TaskRelaunch = Value
  val TaskActions =
    Set(TaskCheckInstanceType, TaskProlog, TaskInstantiateCommand, TaskEpilog, TaskRelaunch)
}

abstract class TaskExecutor(jobMeta: JobMeta,
                            workerPaths: DxWorkerPaths,
                            fileUploader: FileUploader,
                            traceLengthLimit: Int = 10000) {
  protected val logger: Logger = jobMeta.logger

  protected def trace(msg: String, minLevel: Int = TraceLevel.Verbose): Unit = {
    logger.traceLimited(msg, traceLengthLimit, minLevel)
  }

  private def printDirTree(): Unit = {
    if (logger.traceLevel >= TraceLevel.VVerbose) {
      trace("Directory structure:", TraceLevel.VVerbose)
      val (_, stdout, _) = SysUtils.execCommand("ls -lR", None)
      trace(stdout + "\n", TraceLevel.VVerbose)
    }
  }

  def requiredInstanceType: String

  /**
    * Check if we are already on the correct instance type. This allows for avoiding
    * unnecessary relaunch operations.
    */
  def checkInstanceType: Boolean = {
    // calculate the required instance type
    val reqInstanceType: String = requiredInstanceType
    trace(s"required instance type: ${reqInstanceType}")
    val curInstanceType = jobMeta.jobDesc.instanceType.getOrElse(
        throw new Exception(s"Cannot get instance type for job ${jobMeta.jobDesc.id}")
    )
    trace(s"current instance type: ${curInstanceType}")
    val isSufficient =
      jobMeta.instanceTypeDb.lteqByResources(reqInstanceType, curInstanceType)
    trace(s"isSufficient? ${isSufficient}")
    isSufficient
  }

  /**
    * For any File- and Directory-typed inputs for which a value is provided, materialize
    * those files on the local file system. This could be via direct download, or by producing
    * dxda and/or dxfuse manifests.
    * @return
    */
  protected def localizeInputFiles
      : (Map[String, JsValue], Map[FileSource, Path], Option[DxdaManifest], Option[DxfuseManifest])

  // marshal localized inputs into json, and then to a string
  private def writeEnv(inputs: Map[String, JsValue],
                       fileSourceToPath: Map[FileSource, Path]): Unit = {
    val uriToPath: Map[String, JsValue] = fileSourceToPath.map {
      case (fileSource: RealFileSource, path) => fileSource.value -> JsString(path.toString)
      case (other, _) =>
        throw new RuntimeException(s"Can only serialize a RealFileSource, not ${other}")
    }
    val json = JsObject(
        "localizedInputs" -> JsObject(inputs),
        "dxUrl2path" -> JsObject(uriToPath)
    )
    FileUtils.writeFileContent(workerPaths.runnerTaskEnv, json.prettyPrint)
  }

  private def readEnv(): (Map[String, JsValue], Map[FileSource, Path]) = {
    val (inputJs, filesJs) = FileUtils.readFileContent(workerPaths.runnerTaskEnv).parseJson match {
      case JsObject(env) =>
        (env.get("localizedInputs"), env.get("dxUrl2path")) match {
          case (Some(JsObject(inputs)), Some(JsObject(paths))) => (inputs, paths)
          case _ =>
            throw new Exception("Malformed environment serialized to disk")
        }
      case _ => throw new Exception("Malformed environment serialized to disk")
    }
    val fileSourceToPath = filesJs.map {
      case (uri, JsString(path)) => jobMeta.fileResolver.resolve(uri) -> Paths.get(path)
    }
    (inputJs, fileSourceToPath)
  }

  /**
    * Evaluate inputs and prepare a file localization plan.
    */
  def prolog(): Unit = {
    if (logger.isVerbose) {
      trace(s"Prolog debugLevel=${logger.traceLevel}")
      trace(s"dxWDL version: ${getVersion}")
      printDirTree()
      trace(s"Task source code:\n${jobMeta.sourceCode}", traceLengthLimit)
    }
    val (localizedInputs, fileSourceToPath, dxdaManifest, dxfuseManifest) = localizeInputFiles
    // build a manifest for dxda, if there are files to download
    dxdaManifest.foreach {
      case DxdaManifest(manifestJs: JsObject) if manifestJs.fields.nonEmpty =>
        FileUtils.writeFileContent(workerPaths.dxdaManifest, manifestJs.prettyPrint)
    }
    // build a manifest for dxfuse, if there are files to stream
    dxfuseManifest.foreach {
      case DxfuseManifest(manifestJs: JsObject) if manifestJs.fields.nonEmpty =>
        FileUtils.writeFileContent(workerPaths.dxfuseManifest, manifestJs.prettyPrint)
    }
    writeEnv(localizedInputs, fileSourceToPath)
  }

  /**
    * Generates and writes command script(s) to disk.
    * @param localizedInputs task inputs with localized files
    * @return localizedInputs, updated with any additional (non-input) variables that
    *         may be required to evaluate the outputs.
    */
  protected def writeCommandScript(localizedInputs: Map[String, JsValue]): Map[String, JsValue]

  def instantiateCommand(): Unit = {
    val (localizedInputs, fileSourceToPath) = readEnv()
    logger.traceLimited(s"InstantiateCommand, env = ${localizedInputs}")
    val updatedInputs = writeCommandScript(localizedInputs)
    writeEnv(updatedInputs, fileSourceToPath)
  }

  /**
    * Evaluates the outputs of the task and write them to the meta file.
    * @param localizedInputs the execution context
    * @param fileSourceToPath mapping of file sources to local paths
    */
  protected def evaluateOutputs(localizedInputs: Map[String, JsValue],
                                fileSourceToPath: Map[FileSource, Path],
                                fileUploader: FileUploader): Unit

  def epilog(): Unit = {
    if (logger.isVerbose) {
      trace(s"Epilog debugLevel=${logger.traceLevel}")
      printDirTree()
    }
    val (localizedInputs, fileSourceToPath) = readEnv()
    evaluateOutputs(localizedInputs, fileSourceToPath, fileUploader)
  }

  /**
    * Creates JBORs for all the task outputs by linking them to the outputs of
    * a sub-job with the same signature as this (parent) job.
    * @param subjob DxJob
    */
  protected def linkOutputs(subjob: DxJob): Unit

  /**
    * Launches a sub-job with the same inputs and the dynamically calculated
    * instance type.
    */
  def relaunch(): Unit = {
    // Run a sub-job with the "body" entry point, and the required instance type
    val dxSubJob: DxJob =
      jobMeta.dxApi.runSubJob("body",
                              Some(requiredInstanceType),
                              JsObject(jobMeta.jsInputs),
                              Vector.empty,
                              jobMeta.delayWorkspaceDestruction)
    linkOutputs(dxSubJob)
  }
}

trait ExecutorFactory {
  def createTaskExecutor: Option[TaskExecutor]
}

case class Executor(jobMeta: JobMeta, workerPaths: DxWorkerPaths, fileUploader: FileUploader) {
  private val executorFactories: Vector[ExecutorFactory] = Vector(
      WdlExecutorFactory(jobMeta, workerPaths, fileUploader)
  )

  private def executeTask(action: ExecutorAction): String = {
    // setup the utility directories that the task-runner employs
    workerPaths.createCleanDirs()

    val taskExecutor = executorFactories
      .collectFirst { factory =>
        factory.createTaskExecutor match {
          case Some(executor) => executor
        }
      }
      .getOrElse(
          throw new Exception("Cannot determine language/version from source code")
      )

    if (action == ExecutorAction.TaskCheckInstanceType) {
      // special operation to check if this task is on the right instance type
      taskExecutor.checkInstanceType.toString
    } else {
      action match {
        case ExecutorAction.TaskProlog =>
          taskExecutor.prolog()
        case ExecutorAction.TaskInstantiateCommand =>
          taskExecutor.instantiateCommand()
        case ExecutorAction.TaskEpilog =>
          taskExecutor.epilog()
        case ExecutorAction.TaskRelaunch =>
          taskExecutor.relaunch()
        case _ =>
          throw new Exception(s"Invalid executor action ${action}")
      }
      s"success ${action}"
    }
  }

  private def executeWorkflowFragment(action: ExecutorAction): String = {
    val wfFragExecutor = executorFactories
      .collectFirst { factory =>
        factory.createWorkflowFragmentExecutor match {
          case Some(executor) => executor
        }
      }
      .getOrElse(
          throw new Exception("Cannot determine language/version from source code")
      )
  }

  def apply(action: ExecutorAction): String = {
    if (ExecutorAction.TaskActions.contains(action)) {
      executeTask(action)
    } else if (ExecutorAction.WorkflowFragmentActions.contains(action)) {
      executeWorkflowFragment(action)
    } else {
      throw new Exception(s"invalid action ${action}")
    }
  }
}

/**
  private def workflowFragAction(
      action: ExecutorAction.Value,
      language: Option[Language],
      sourceCode: String,
      instanceTypeDB: InstanceTypeDB,
      metaInfo: Map[String, JsValue],
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
    val fragInputs = fragInputOutput.loadInputs(inputsRaw, metaInfo)
    val outputFields: Map[String, JsValue] =
      action match {
        case ExecutorAction.WfFragment | ExecutorAction.WfScatterContinue | ExecutorAction.WfScatterCollect =>
          val mode = action match {
            case ExecutorAction.WfFragment        => RunnerWfFragmentMode.Launch
            case ExecutorAction.WfScatterContinue => RunnerWfFragmentMode.Continue
            case ExecutorAction.WfScatterCollect  => RunnerWfFragmentMode.Collect
          }
          val scatterSize = metaInfo.get(Native.ScatterChunkSize) match {
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
          val refDxFiles = fragInputOutput.findRefDxFiles(inputsRaw, metaInfo)
          wfReorg.apply(refDxFiles)

        case _ =>
          throw new Exception(s"Illegal workflow fragment operation ${action}")
      }

    // write outputs, ignore null values, these could occur for optional
    // values that were not specified.
    val json = JsObject(outputFields)
    FileUtils.writeFileContent(jobOutputPath, json.prettyPrint)
    Success(s"success ${action}")
  }

  */
