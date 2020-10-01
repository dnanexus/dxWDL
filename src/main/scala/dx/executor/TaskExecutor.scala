package dx.executor

import java.nio.file.{Path, Paths}

import dx.api.DxJob
import dx.core.getVersion
import dx.core.io.{DxdaManifest, DxfuseManifest}
import dx.executor.wdl.WdlTaskSupportFactory
import spray.json._
import wdlTools.util.{Enum, FileSource, FileUtils, RealDataSource, SysUtils, TraceLevel}

object TaskAction extends Enum {
  type TaskAction = Value
  val CheckInstanceType, Prolog, InstantiateCommand, Epilog, Relaunch = Value
}

trait TaskSupport {
  def getRequiredInstanceType: String

  /**
    * For any File- and Directory-typed inputs for which a value is provided, materialize
    * those files on the local file system. This could be via direct download, or by producing
    * dxda and/or dxfuse manifests.
    * @return
    */
  def localizeInputFiles(streamAllFiles: Boolean): (
      Map[String, JsValue],
      Map[FileSource, Path],
      Option[DxdaManifest],
      Option[DxfuseManifest]
  )

  /**
    * Generates and writes command script(s) to disk.
    * @param localizedInputs task inputs with localized files
    * @return localizedInputs, updated with any additional (non-input) variables that
    *         may be required to evaluate the outputs.
    */
  def writeCommandScript(localizedInputs: Map[String, JsValue]): Map[String, JsValue]

  /**
    * Evaluates the outputs of the task and write them to the meta file.
    * @param localizedInputs the execution context
    * @param fileSourceToPath mapping of file sources to local paths
    */
  def evaluateOutputs(localizedInputs: Map[String, JsValue],
                      fileSourceToPath: Map[FileSource, Path],
                      fileUploader: FileUploader): Unit

  /**
    * Creates JBORs for all the task outputs by linking them to the outputs of
    * a sub-job with the same signature as this (parent) job.
    * @param subjob DxJob
    */
  def linkOutputs(subjob: DxJob): Unit
}

trait TaskSupportFactory {
  def create(jobMeta: JobMeta, fileUploader: FileUploader): Option[TaskSupport]
}

object TaskExecutor {
  val taskSupportFactories: Vector[TaskSupportFactory] = Vector(
      WdlTaskSupportFactory()
  )

  def createTaskSupport(jobMeta: JobMeta, fileUploader: FileUploader): TaskSupport = {
    taskSupportFactories
      .collectFirst { factory =>
        factory.create(jobMeta, fileUploader) match {
          case Some(executor) => executor
        }
      }
      .getOrElse(
          throw new Exception("Cannot determine language/version from source code")
      )
  }
}

case class TaskExecutor(jobMeta: JobMeta,
                        streamAllFiles: Boolean,
                        fileUploader: FileUploader = SerialFileUploader(),
                        traceLengthLimit: Int = 10000) {
  private[executor] val taskSupport: TaskSupport =
    TaskExecutor.createTaskSupport(jobMeta, fileUploader)
  private val logger = jobMeta.logger

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

  /**
    * Check if we are already on the correct instance type. This allows for avoiding
    * unnecessary relaunch operations.
    */
  def checkInstanceType: Boolean = {
    // calculate the required instance type
    val reqInstanceType: String = taskSupport.getRequiredInstanceType
    trace(s"required instance type: ${reqInstanceType}")
    val curInstanceType = jobMeta.instanceType.getOrElse(
        throw new Exception(s"Cannot get instance type for job ${jobMeta.jobId}")
    )
    trace(s"current instance type: ${curInstanceType}")
    val isSufficient =
      try {
        jobMeta.instanceTypeDb.compareByResources(reqInstanceType, curInstanceType) <= 0
      } catch {
        case ex: Throwable =>
          logger.warning("error comparing current and required instance types",
                         exception = Some(ex))
          false
      }
    trace(s"isSufficient? ${isSufficient}")
    isSufficient
  }

  // marshal localized inputs into json, and then to a string
  private def writeEnv(inputs: Map[String, JsValue],
                       fileSourceToPath: Map[FileSource, Path]): Unit = {
    val uriToPath: Map[String, JsValue] = fileSourceToPath.map {
      case (fileSource: RealDataSource, path) => fileSource.value -> JsString(path.toString)
      case (other, _) =>
        throw new RuntimeException(s"Can only serialize a RealFileSource, not ${other}")
    }
    val json = JsObject(
        "localizedInputs" -> JsObject(inputs),
        "dxUrl2path" -> JsObject(uriToPath)
    )
    FileUtils.writeFileContent(jobMeta.workerPaths.getTaskEnvFile(), json.prettyPrint)
  }

  private def readEnv(): (Map[String, JsValue], Map[FileSource, Path]) = {
    val (inputJs, filesJs) =
      FileUtils.readFileContent(jobMeta.workerPaths.getTaskEnvFile()).parseJson match {
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
      case other                 => throw new Exception(s"unexpected path ${other}")
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
    val (localizedInputs, fileSourceToPath, dxdaManifest, dxfuseManifest) =
      taskSupport.localizeInputFiles(streamAllFiles)

    // build a manifest for dxda, if there are files to download
    dxdaManifest.foreach {
      case DxdaManifest(manifestJs) =>
        FileUtils.writeFileContent(jobMeta.workerPaths.getDxdaManifestFile(),
                                   manifestJs.prettyPrint)
    }
    // build a manifest for dxfuse, if there are files to stream
    dxfuseManifest.foreach {
      case DxfuseManifest(manifestJs) =>
        FileUtils.writeFileContent(jobMeta.workerPaths.getDxfuseManifestFile(),
                                   manifestJs.prettyPrint)
    }
    writeEnv(localizedInputs, fileSourceToPath)
  }

  def instantiateCommand(): Unit = {
    val (localizedInputs, fileSourceToPath) = readEnv()
    logger.traceLimited(s"InstantiateCommand, env = ${localizedInputs}")
    val updatedInputs = taskSupport.writeCommandScript(localizedInputs)
    writeEnv(updatedInputs, fileSourceToPath)
  }

  def epilog(): Unit = {
    if (logger.isVerbose) {
      trace(s"Epilog debugLevel=${logger.traceLevel}")
      printDirTree()
    }
    val (localizedInputs, fileSourceToPath) = readEnv()
    taskSupport.evaluateOutputs(localizedInputs, fileSourceToPath, fileUploader)
  }

  /**
    * Launches a sub-job with the same inputs and the dynamically calculated
    * instance type.
    */
  def relaunch(): Unit = {
    // Run a sub-job with the "body" entry point, and the required instance type
    val dxSubJob: DxJob =
      jobMeta.dxApi.runSubJob("body",
                              Some(taskSupport.getRequiredInstanceType),
                              JsObject(jobMeta.jsInputs),
                              Vector.empty,
                              jobMeta.delayWorkspaceDestruction)
    taskSupport.linkOutputs(dxSubJob)
  }

  def apply(action: TaskAction.TaskAction): String = {
    try {
      // setup the utility directories that the task-runner employs
      jobMeta.workerPaths.createCleanDirs()

      if (action == TaskAction.CheckInstanceType) {
        // special operation to check if this task is on the right instance type
        checkInstanceType.toString
      } else {
        action match {
          case TaskAction.Prolog =>
            prolog()
          case TaskAction.InstantiateCommand =>
            instantiateCommand()
          case TaskAction.Epilog =>
            epilog()
          case TaskAction.Relaunch =>
            relaunch()
          case _ =>
            throw new Exception(s"Invalid executor action ${action}")
        }
        s"success ${action}"
      }
    } catch {
      case e: Throwable =>
        jobMeta.error(e)
        throw e
    }
  }
}
