package dx.executor

import java.nio.file.{Path, Paths}

import dx.{AppException, AppInternalException}
import dx.api.{DxApi, DxExecutable, Field, InstanceTypeDB}
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache, DxPathConfig}
import dx.core.ir.Value
import dx.core.languages.wdl.ParseSource
import dx.core.util.MainUtils._
import dx.core.util.CompressionUtils
import wdlTools.util.{Enum, FileSourceResolver, FileUtils, JsUtils, TraceLevel}
import spray.json._

object Main {
  object ExecAction extends Enum {
    type ExecAction = Value
    val Collect, WfOutputs, WfInputs, WorkflowOutputReorg, WfCustomReorgOutputs, WfFragment,
        TaskCheckInstanceType, TaskProlog, TaskInstantiateCommand, TaskEpilog, TaskRelaunch = Value
  }

  private def taskAction(op: ExecAction.Value,
                         taskSourceCode: String,
                         instanceTypeDB: InstanceTypeDB,
                         jobInputPath: Path,
                         jobOutputPath: Path,
                         dxPathConfig: DxPathConfig,
                         fileResolver: FileSourceResolver,
                         dxFileDescCache: DxFileDescCache,
                         defaultRuntimeAttributes: Option[Map[String, Value]],
                         delayWorkspaceDestruction: Option[Boolean],
                         dxApi: DxApi): Termination = {
    // Parse the inputs, convert to WDL values. Delay downloading files
    // from the platform, we may not need to access them.
    val inputLines: String = FileUtils.readFileContent(jobInputPath)
    val originalInputs: JsValue = inputLines.parseJson

    val (task, typeAliases, document) = ParseSource(dxApi).parseWdlTask(taskSourceCode)

    // setup the utility directories that the task-runner employs
    dxPathConfig.createCleanDirs()

    val wdlVarLinksConverter =
      WdlDxLinkSerde(dxApi, fileResolver, dxFileDescCache, typeAliases)
    val evaluator = createEvaluator(dxPathConfig, fileResolver, document.version.value)
    val jobInputOutput =
      JobInputOutput(dxPathConfig,
                     fileResolver,
                     dxFileDescCache,
                     wdlVarLinksConverter,
                     dxApi,
                     evaluator)
    val inputs = jobInputOutput.loadInputs(originalInputs, task)
    System.err.println(s"""|Main processing inputs in taskAction
                           |originalInputs:
                           |${originalInputs.prettyPrint}
                           |
                           |processed inputs:
                           |${inputs.mkString("\n")}
                           |""".stripMargin)
    val taskRunner = dx.executor.TaskRunner(
        task,
        document,
        typeAliases,
        instanceTypeDB,
        dxPathConfig,
        fileResolver,
        wdlVarLinksConverter,
        jobInputOutput,
        defaultRuntimeAttributes,
        delayWorkspaceDestruction,
        dxApi,
        evaluator
    )

    // Running tasks
    op match {
      case ExecAction.TaskCheckInstanceType =>
        // special operation to check if this task is on the right instance type
        val correctInstanceType: Boolean = taskRunner.checkInstanceType(inputs)
        Success(correctInstanceType.toString)

      case ExecAction.TaskProlog =>
        val (localizedInputs, fileSourceToPath) = taskRunner.prolog(inputs)
        taskRunner.writeEnvToDisk(localizedInputs, fileSourceToPath)
        Success(s"success ${op}")

      case ExecAction.TaskInstantiateCommand =>
        val (localizedInputs, fileSourceToPath) = taskRunner.readEnvFromDisk()
        val env = taskRunner.instantiateCommand(localizedInputs)
        taskRunner.writeEnvToDisk(env, fileSourceToPath)
        Success(s"success ${op}")

      case ExecAction.TaskEpilog =>
        val (env, fileSourceToPath) = taskRunner.readEnvFromDisk()
        val outputFields: Map[String, JsValue] = taskRunner.epilog(env, fileSourceToPath)

        // write outputs, ignore null values, these could occur for optional
        // values that were not specified.
        val json = JsObject(outputFields.filter {
          case (_, jsValue) => jsValue != null && jsValue != JsNull
        })
        FileUtils.writeFileContent(jobOutputPath, json.prettyPrint)
        Success(s"success ${op}")

      case ExecAction.TaskRelaunch =>
        val outputFields: Map[String, JsValue] = taskRunner.relaunch(inputs, originalInputs)
        val json = JsObject(outputFields.filter {
          case (_, jsValue) => jsValue != null && jsValue != JsNull
        })
        FileUtils.writeFileContent(jobOutputPath, json.prettyPrint)
        Success(s"success ${op}")

      case _ =>
        Failure(s"Illegal task operation ${op}")
    }
  }

  // Execute a part of a workflow
  private def workflowFragAction(op: ExecAction.Value,
                                 wdlSourceCode: String,
                                 instanceTypeDB: InstanceTypeDB,
                                 metaInfo: JsValue,
                                 jobInputPath: Path,
                                 jobOutputPath: Path,
                                 dxPathConfig: DxPathConfig,
                                 fileResolver: FileSourceResolver,
                                 dxFileDescCache: DxFileDescCache,
                                 defaultRuntimeAttributes: Option[Map[String, Value]],
                                 delayWorkspaceDestruction: Option[Boolean],
                                 dxApi: DxApi): Termination = {
    // Parse the inputs, convert to WDL values. Delay downloading files
    // from the platform, we may not need to access them.
    val inputLines: String = FileUtils.readFileContent(jobInputPath)
    val inputsRaw: JsValue = inputLines.parseJson

    val (wf, taskDir, typeAliases, document) =
      ParseSource(dxApi).parseWdlWorkflow(wdlSourceCode)
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
      op match {
        case ExecAction.WfFragment =>
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
              dxApi,
              evaluator
          )
          fragRunner.apply(fragInputs.blockPath, fragInputs.env, RunnerWfFragmentMode.Launch)
        case ExecAction.Collect =>
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
              dxApi,
              evaluator
          )
          fragRunner.apply(fragInputs.blockPath, fragInputs.env, RunnerWfFragmentMode.Collect)
        case ExecAction.WfInputs =>
          val wfInputs = WfInputs(wf, document, wdlVarLinksConverter, dxApi, evaluator)
          wfInputs.apply(fragInputs.env)
        case ExecAction.WfOutputs =>
          val wfOutputs = WfOutputs(wf, document, wdlVarLinksConverter, dxApi, evaluator)
          wfOutputs.apply(fragInputs.env)

        case ExecAction.WfCustomReorgOutputs =>
          val wfCustomReorgOutputs = WfOutputs(
              wf,
              document,
              wdlVarLinksConverter,
              dxApi,
              evaluator
          )
          // add ___reconf_status as output.
          wfCustomReorgOutputs.apply(fragInputs.env, addStatus = true)

        case ExecAction.WorkflowOutputReorg =>
          val wfReorg = WorkflowOutputReorg(dxApi)
          val refDxFiles = fragInputOutput.findRefDxFiles(inputsRaw, metaInfo)
          wfReorg.apply(refDxFiles)

        case _ =>
          throw new Exception(s"Illegal workflow fragment operation ${op}")
      }

    // write outputs, ignore null values, these could occur for optional
    // values that were not specified.
    val json = JsObject(outputFields)
    FileUtils.writeFileContent(jobOutputPath, json.prettyPrint)
    Success(s"success ${op}")
  }

  // Report an error, since this is called from a bash script, we
  // can't simply raise an exception. Instead, we write the error to
  // a standard JSON file.
  def writeJobError(jobErrorPath: Path, e: Throwable): Unit = {
    val errType = e match {
      case _: AppException         => "AppError"
      case _: AppInternalException => "AppInternalError"
      case _: Throwable            => "AppInternalError"
    }
    // We are limited in what characters can be written to json, so we
    // provide a short description for json.
    //
    // Note: we sanitize this string, to be absolutely sure that
    // it does not contain problematic JSON characters.
    val errMsg = JsObject(
        "error" -> JsObject(
            "type" -> JsString(errType),
            "message" -> JsUtils.sanitizedString(e.getMessage)
        )
    ).prettyPrint
    FileUtils.writeFileContent(jobErrorPath, errMsg)

    // Write out a full stack trace to standard error.
    System.err.println(exceptionToString(e))
  }

  private val CommonOptions: OptionSpecs = Map(
      "streamAllFiles" -> FlagOptionSpec.Default
  )

  private[executor] def dispatchCommand(args: Vector[String]): Termination = {
    if (args.isEmpty) {
      return BadUsageTermination()
    }

    val action =
      try {
        ExecAction.withNameIgnoreCase(args.head.replaceAll("_", ""))
      } catch {
        case _: NoSuchElementException =>
          return BadUsageTermination()
      }

    val homeDir = args.headOption
      .map(Paths.get(_))
      .getOrElse(
          return BadUsageTermination(
              "Missing required positional argument <homedir>"
          )
      )

    val options =
      try {
        parseCommandLine(args, CommonOptions)
      } catch {
        case e: OptionParseException =>
          return BadUsageTermination("Error parsing command line options", Some(e))
      }

    val logger = initLogger(options)
    val dxApi = DxApi(logger)
    val streamAllFiles = options.getFlag("streamAllFiles")

    // Job input, output,  error, and info files are located relative to the home directory
    val jobInputPath = homeDir.resolve("job_input.json")
    val jobOutputPath = homeDir.resolve("job_output.json")
    val jobErrorPath = homeDir.resolve("job_error.json")
    val jobInfoPath = homeDir.resolve("dnanexus-job.json")

    try {
      // Setup the standard paths used for applets. These are used at runtime, not at compile time.
      // On the cloud instance running the job, the user is "dnanexus", and the home directory is
      // "/home/dnanexus".
      val dxPathConfig = DxPathConfig.apply(baseDNAxDir, streamAllFiles, logger)
      val inputs: JsValue = FileUtils.readFileContent(jobInputPath).parseJson
      val allFilesReferenced = inputs.asJsObject.fields.flatMap {
        case (_, jsElem) => dxApi.findFiles(jsElem)
      }.toVector

      // Describe all the files and build a lookup cache
      val dxFileDescCache = DxFileDescCache(dxApi.fileBulkDescribe(allFilesReferenced))
      val dxProtocol = DxFileAccessProtocol(dxApi, dxFileDescCache)
      val fileResolver =
        FileSourceResolver.create(localDirectories = Vector(dxPathConfig.homeDir),
                                  userProtocols = Vector(dxProtocol),
                                  logger = logger)

      // Get the source code, instance type DB, etc from the application's details
      val jobInfo = FileUtils.readFileContent(jobInfoPath).parseJson
      val executable: DxExecutable = jobInfo.asJsObject.fields.get("executable") match {
        case None =>
          dxApi.logger.trace(
              "executable field not found locally, performing an API call.",
              minLevel = TraceLevel.None
          )
          val dxJob = dxApi.currentJob
          dxJob.describe().executable
        case Some(JsString(x)) if x.startsWith("app-") =>
          dxApi.app(x)
        case Some(JsString(x)) if x.startsWith("applet-") =>
          dxApi.applet(x)
        case Some(other) =>
          throw new Exception(s"Malformed executable field ${other} in job info")
      }

      val details = executable.describe(Set(Field.Details)).details.get.asJsObject.fields
      val JsString(sourceCodeEncoded) = details.getOrElse("wdlSourceCode", details("womSourceCode"))
      val wdlSourceCode = CompressionUtils.base64DecodeAndGunzip(wdlSourceCodeEncoded)

      val JsString(instanceTypeDBEncoded) = details.asJsObject.fields("instanceTypeDB")
      val dbRaw = CompressionUtils.base64DecodeAndGunzip(instanceTypeDBEncoded)
      val instanceTypeDB = dbRaw.parseJson.convertTo[InstanceTypeDB]

      val runtimeAttrs: Option[Map[String, Value]] =
        details.asJsObject.fields.get("runtimeAttrs") match {
          case None         => None
          case Some(JsNull) => None
          case Some(x)      => Some(x.convertTo[Map[String, Value]])
        }

      val delayWorkspaceDestruction: Option[Boolean] =
        details.asJsObject.fields.get("delayWorkspaceDestruction") match {
          case Some(JsBoolean(flag)) => Some(flag)
          case None                  => None
        }

      action match {
        case ExecAction.Collect | ExecAction.WfFragment | ExecAction.WfInputs |
            ExecAction.WfOutputs | ExecAction.WorkflowOutputReorg |
            ExecAction.WfCustomReorgOutputs =>
          workflowFragAction(
              action,
              wdlSourceCode,
              instanceTypeDB,
              metaInfo,
              jobInputPath,
              jobOutputPath,
              dxPathConfig,
              fileResolver,
              dxFileDescCache,
              defaultRuntimeAttrs,
              delayWorkspaceDestruction,
              dxApi
          )
        case ExecAction.TaskCheckInstanceType | ExecAction.TaskProlog |
            ExecAction.TaskInstantiateCommand | ExecAction.TaskEpilog | ExecAction.TaskRelaunch =>
          taskAction(
              action,
              wdlSourceCode,
              instanceTypeDB,
              jobInputPath,
              jobOutputPath,
              dxPathConfig,
              fileResolver,
              dxFileDescCache,
              defaultRuntimeAttrs,
              delayWorkspaceDestruction,
              dxApi
          )
      }
    } catch {
      case e: Throwable =>
        writeJobError(jobErrorPath, e)
        Failure(s"failure running ${action}")
    }
  }

  private val usageMessage =
    s"""|java -jar dxWDL.jar internal <action> <homedir> [options]
        |
        |Options:
        |    -traceLevel [0,1,2] How much debug information to write to the
        |                        job log at runtime. Zero means write the minimum,
        |                        one is the default, and two is for internal debugging.
        |    -streamAllFiles     Mount all files with dxfuse, do not use the download agent
        |""".stripMargin

  def main(args: Seq[String]): Unit = {
    terminate(dispatchCommand(args.toVector), usageMessage)
  }
}
