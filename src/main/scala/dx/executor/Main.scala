package dx.executor

import java.nio.file.{Path, Paths}

import dx.{AppException, AppInternalException}
import dx.api.{DxApi, DxExecutable, Field, InstanceTypeDB}
import dx.core.NativeDetails
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache, DxPathConfig}
import dx.core.ir.Value
import dx.core.ir.ValueSerde.valueMapFormat
import dx.core.languages.Language
import dx.core.languages.Language.Language
import dx.core.languages.wdl.ParseSource
import dx.core.util.MainUtils._
import dx.core.util.CompressionUtils
import wdlTools.util.{Enum, FileSourceResolver, FileUtils, JsUtils, TraceLevel}
import spray.json._

object Main {
  private object ExecAction extends Enum {
    type ExecAction = Value
    val WfFragment, WfInputs, WfOutputs, WfOutputReorg, WfCustomReorgOutputs, WfScatterCollect =
      Value
    val WorkflowActions =
      Set(WfFragment, WfInputs, WfOutputs, WfOutputReorg, WfCustomReorgOutputs, WfScatterCollect)
    val TaskCheckInstanceType, TaskProlog, TaskInstantiateCommand, TaskEpilog, TaskRelaunch = Value
    val TaskActions =
      Set(TaskCheckInstanceType, TaskProlog, TaskInstantiateCommand, TaskEpilog, TaskRelaunch)
  }

  /*
   *
  // throw an exception if this WDL program is invalid
  def validateWdlCode(wdlWfSource: String, language: Option[Language.Value] = None): Unit = {
    val (tDoc, _) = parseWdlFromString(wdlWfSource)

    // Check that this is the correct language version
    language match {
      case None => ()
      case Some(requiredLang) =>
        val docLang = Language.fromWdlVersion(tDoc.version.value)
        if (docLang != requiredLang)
          throw new Exception(s"document has wrong version $docLang, should be $requiredLang")
    }
  }
   */

  private def taskAction(
      action: ExecAction.Value,
      language: Option[Language],
      taskSourceCode: String,
      instanceTypeDB: InstanceTypeDB,
      jobInputPath: Path,
      jobOutputPath: Path,
      dxPathConfig: DxPathConfig,
      fileResolver: FileSourceResolver,
      dxFileDescCache: DxFileDescCache,
      defaultRuntimeAttributes: Map[String, Value],
      delayWorkspaceDestruction: Option[Boolean],
      dxApi: DxApi
  ): Termination = {
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
    action match {
      case ExecAction.TaskCheckInstanceType =>
        // special operation to check if this task is on the right instance type
        val correctInstanceType: Boolean = taskRunner.checkInstanceType(inputs)
        Success(correctInstanceType.toString)

      case ExecAction.TaskProlog =>
        val (localizedInputs, fileSourceToPath) = taskRunner.prolog(inputs)
        taskRunner.writeEnvToDisk(localizedInputs, fileSourceToPath)
        Success(s"success ${action}")

      case ExecAction.TaskInstantiateCommand =>
        val (localizedInputs, fileSourceToPath) = taskRunner.readEnvFromDisk()
        val env = taskRunner.instantiateCommand(localizedInputs)
        taskRunner.writeEnvToDisk(env, fileSourceToPath)
        Success(s"success ${action}")

      case ExecAction.TaskEpilog =>
        val (env, fileSourceToPath) = taskRunner.readEnvFromDisk()
        val outputFields: Map[String, JsValue] = taskRunner.epilog(env, fileSourceToPath)

        // write outputs, ignore null values, these could occur for optional
        // values that were not specified.
        val json = JsObject(outputFields.filter {
          case (_, jsValue) => jsValue != null && jsValue != JsNull
        })
        FileUtils.writeFileContent(jobOutputPath, json.prettyPrint)
        Success(s"success ${action}")

      case ExecAction.TaskRelaunch =>
        val outputFields: Map[String, JsValue] = taskRunner.relaunch(inputs, originalInputs)
        val json = JsObject(outputFields.filter {
          case (_, jsValue) => jsValue != null && jsValue != JsNull
        })
        FileUtils.writeFileContent(jobOutputPath, json.prettyPrint)
        Success(s"success ${action}")

      case _ =>
        Failure(s"Illegal task operation ${action}")
    }
  }

  /**
    * Execute a part of a workflow.
    */
  private def workflowFragAction(
      op: ExecAction.Value,
      language: Option[Language],
      sourceCode: String,
      instanceTypeDB: InstanceTypeDB,
      metaInfo: Map[String, JsValue],
      jobInputPath: Path,
      jobOutputPath: Path,
      dxPathConfig: DxPathConfig,
      fileResolver: FileSourceResolver,
      dxFileDescCache: DxFileDescCache,
      defaultRuntimeAttributes: Map[String, Value],
      delayWorkspaceDestruction: Option[Boolean],
      dxApi: DxApi
  ): Termination = {

    // Parse the inputs, convert to WDL values. Delay downloading files
    // from the platform, we may not need to access them.
    val inputLines: String = FileUtils.readFileContent(jobInputPath)
    val inputsRaw: JsValue = inputLines.parseJson

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
        case ExecAction.WfScatterCollect =>
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

        case ExecAction.WfOutputReorg =>
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

  /**
    * Report an error, since this is called from a bash script, we can't simply
    * raise an exception. Instead, we write the error to a standard JSON file.
    * @param jobErrorPath path to the error file
    * @param e the exception
    */
  private def writeJobError(jobErrorPath: Path, e: Throwable): Unit = {
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

      val jobInfo = FileUtils.readFileContent(jobInfoPath).parseJson.asJsObject.fields
      val executable: DxExecutable = jobInfo.get("executable") match {
        case None =>
          dxApi.logger.trace(
              "executable field not found locally, performing an API call.",
              minLevel = TraceLevel.None
          )
          dxApi.currentJob.describe(Set(Field.Executable)).executable.get
        case Some(JsString(x)) if x.startsWith("app-") =>
          dxApi.app(x)
        case Some(JsString(x)) if x.startsWith("applet-") =>
          dxApi.applet(x)
        case Some(other) =>
          throw new Exception(s"Malformed executable field ${other} in job info")
      }

      // Get the source code, instance type DB, etc from the application's details
      val details = executable.describe(Set(Field.Details)).details.get.asJsObject.fields
      val language: Option[Language] = details.get(NativeDetails.Language) match {
        case Some(JsString(lang)) => Some(Language.withName(lang))
        case None =>
          logger.warning("This applet ws built with an old version of dxWDL - please rebuild")
          // we will attempt to detect the language/version later
          None
      }
      val sourceCodeEncoded = details.get(NativeDetails.SourceCode) match {
        case Some(JsString(s)) => s
        case None =>
          logger.warning("This applet ws built with an old version of dxWDL - please rebuild")
          val JsString(s) = details.getOrElse("wdlSourceCode", details("womSourceCode"))
          s
      }
      val wdlSourceCode = CompressionUtils.base64DecodeAndGunzip(sourceCodeEncoded)
      val instanceTypeDb = details(NativeDetails.InstanceTypeDb) match {
        case JsString(s) =>
          val js = CompressionUtils.base64DecodeAndGunzip(s)
          js.parseJson.convertTo[InstanceTypeDB]
      }
      val defaultRuntimeAttrs: Map[String, Value] =
        details.get("runtimeAttrs") match {
          case Some(x)             => x.convertTo[Map[String, Value]]
          case Some(JsNull) | None => Map.empty
        }
      val delayWorkspaceDestruction: Option[Boolean] =
        details.get("delayWorkspaceDestruction") match {
          case Some(JsBoolean(flag)) => Some(flag)
          case None                  => None
        }

      if (ExecAction.WorkflowActions.contains(action)) {
        workflowFragAction(
            action,
            language,
            wdlSourceCode,
            instanceTypeDb,
            details,
            jobInputPath,
            jobOutputPath,
            dxPathConfig,
            fileResolver,
            dxFileDescCache,
            defaultRuntimeAttrs,
            delayWorkspaceDestruction,
            dxApi
        )
      } else if (ExecAction.TaskActions.contains(action)) {
        taskAction(
            action,
            language,
            wdlSourceCode,
            instanceTypeDb,
            jobInputPath,
            jobOutputPath,
            dxPathConfig,
            fileResolver,
            dxFileDescCache,
            defaultRuntimeAttrs,
            delayWorkspaceDestruction,
            dxApi
        )
      } else {
        throw new Exception(s"Unknown action ${action}")
      }
    } catch {
      case e: Throwable =>
        writeJobError(jobErrorPath, e)
        Failure(s"failure running ${action}", Some(e))
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
