package dx.exec

import java.nio.file.{Path, Paths}

import dx.{AppException, AppInternalException}
import dx.api.{DxApi, DxApplet, DxFile, DxFileDescribe, Field, InstanceTypeDB}
import dx.compiler.WdlRuntimeAttrs
import dx.core.io.DxPathConfig
import dx.core.languages.wdl.{DxFileAccessProtocol, ParseSource}
import dx.core.util.MainUtils._
import dx.core.util.SysUtils
import dx.util.{JsUtils, Logger, TraceLevel}
import spray.json._

object Main extends App {
  object ExecAction extends Enumeration {
    type ExecAction = Value
    val Collect, WfOutputs, WfInputs, WorkflowOutputReorg, WfCustomReorgOutputs, WfFragment,
        TaskCheckInstanceType, TaskProlog, TaskInstantiateCommand, TaskEpilog, TaskRelaunch = Value
  }

  // Setup the standard paths used for applets. These are used at
  // runtime, not at compile time. On the cloud instance running the
  // job, the user is "dnanexus", and the home directory is
  // "/home/dnanexus".
  private def buildRuntimePathConfig(streamAllFiles: Boolean, logger: Logger): DxPathConfig = {
    DxPathConfig.apply(baseDNAxDir, streamAllFiles, logger)
  }

  private def taskAction(op: ExecAction.Value,
                         taskSourceCode: String,
                         instanceTypeDB: InstanceTypeDB,
                         jobInputPath: Path,
                         jobOutputPath: Path,
                         dxPathConfig: DxPathConfig,
                         dxIoFunctions: DxFileAccessProtocol,
                         defaultRuntimeAttributes: Option[WdlRuntimeAttrs],
                         delayWorkspaceDestruction: Option[Boolean],
                         dxApi: DxApi): Termination = {
    // Parse the inputs, convert to WDL values. Delay downloading files
    // from the platform, we may not need to access them.
    val inputLines: String = SysUtils.readFileContent(jobInputPath)
    val originalInputs: JsValue = inputLines.parseJson

    val (task, typeAliases, document) = ParseSource(dxApi.logger).parseWdlTask(taskSourceCode)

    // setup the utility directories that the task-runner employs
    dxPathConfig.createCleanDirs()

    val jobInputOutput =
      JobInputOutput(dxIoFunctions, typeAliases, document.version.value, dxApi)
    val inputs = jobInputOutput.loadInputs(originalInputs, task)
    System.err.println(s"""|Main processing inputs in taskAction
                           |originalInputs:
                           |${originalInputs.prettyPrint}
                           |
                           |processed inputs:
                           |${inputs.mkString("\n")}
                           |""".stripMargin)
    val taskRunner = dx.exec.TaskRunner(
        task,
        document,
        typeAliases,
        instanceTypeDB,
        dxPathConfig,
        dxIoFunctions,
        jobInputOutput,
        defaultRuntimeAttributes,
        delayWorkspaceDestruction,
        dxApi
    )

    // Running tasks
    op match {
      case ExecAction.TaskCheckInstanceType =>
        // special operation to check if this task is on the right instance type
        val correctInstanceType: Boolean = taskRunner.checkInstanceType(inputs)
        Success(correctInstanceType.toString)

      case ExecAction.TaskProlog =>
        val (localizedInputs, dxUrl2path) = taskRunner.prolog(inputs)
        taskRunner.writeEnvToDisk(localizedInputs, dxUrl2path)
        Success(s"success ${op}")

      case ExecAction.TaskInstantiateCommand =>
        val (localizedInputs, dxUrl2path) = taskRunner.readEnvFromDisk()
        val env = taskRunner.instantiateCommand(localizedInputs)
        taskRunner.writeEnvToDisk(env, dxUrl2path)
        Success(s"success ${op}")

      case ExecAction.TaskEpilog =>
        val (env, dxUrl2path) = taskRunner.readEnvFromDisk()
        val outputFields: Map[String, JsValue] = taskRunner.epilog(env, dxUrl2path)

        // write outputs, ignore null values, these could occur for optional
        // values that were not specified.
        val json = JsObject(outputFields.filter {
          case (_, jsValue) => jsValue != null && jsValue != JsNull
        })
        val ast_pp = json.prettyPrint
        SysUtils.writeFileContent(jobOutputPath, ast_pp)
        Success(s"success ${op}")

      case ExecAction.TaskRelaunch =>
        val outputFields: Map[String, JsValue] = taskRunner.relaunch(inputs, originalInputs)
        val json = JsObject(outputFields.filter {
          case (_, jsValue) => jsValue != null && jsValue != JsNull
        })
        val ast_pp = json.prettyPrint
        SysUtils.writeFileContent(jobOutputPath, ast_pp)
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
                                 dxIoFunctions: DxFileAccessProtocol,
                                 defaultRuntimeAttributes: Option[WdlRuntimeAttrs],
                                 delayWorkspaceDestruction: Option[Boolean],
                                 dxApi: DxApi): Termination = {
    val dxProject = dxApi.currentProject

    // Parse the inputs, convert to WDL values. Delay downloading files
    // from the platform, we may not need to access them.
    val inputLines: String = SysUtils.readFileContent(jobInputPath)
    val inputsRaw: JsValue = inputLines.parseJson

    val (wf, taskDir, typeAliases, document) =
      ParseSource(dxApi.logger).parseWdlWorkflow(wdlSourceCode)

    // setup the utility directories that the frag-runner employs
    val fragInputOutput =
      WfFragInputOutput(dxIoFunctions, dxProject, typeAliases, document.version.value, dxApi)

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
              dxIoFunctions,
              inputsRaw,
              fragInputOutput,
              defaultRuntimeAttributes,
              delayWorkspaceDestruction,
              dxApi
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
              dxIoFunctions,
              inputsRaw,
              fragInputOutput,
              defaultRuntimeAttributes,
              delayWorkspaceDestruction,
              dxApi
          )
          fragRunner.apply(fragInputs.blockPath, fragInputs.env, RunnerWfFragmentMode.Collect)
        case ExecAction.WfInputs =>
          val wfInputs =
            WfInputs(wf, document, typeAliases, dxPathConfig, dxIoFunctions, dxApi)
          wfInputs.apply(fragInputs.env)
        case ExecAction.WfOutputs =>
          val wfOutputs =
            WfOutputs(wf, document, typeAliases, dxPathConfig, dxIoFunctions, dxApi)
          wfOutputs.apply(fragInputs.env)

        case ExecAction.WfCustomReorgOutputs =>
          val wfCustomReorgOutputs = WfOutputs(
              wf,
              document,
              typeAliases,
              dxPathConfig,
              dxIoFunctions,
              dxApi
          )
          // add ___reconf_status as output.
          wfCustomReorgOutputs.apply(fragInputs.env, addStatus = true)

        case ExecAction.WorkflowOutputReorg =>
          val wfReorg =
            WorkflowOutputReorg(wf, document, typeAliases, dxPathConfig, dxIoFunctions, dxApi)
          val refDxFiles = fragInputOutput.findRefDxFiles(inputsRaw, metaInfo)
          wfReorg.apply(refDxFiles)

        case _ =>
          throw new Exception(s"Illegal workflow fragment operation ${op}")
      }

    // write outputs, ignore null values, these could occur for optional
    // values that were not specified.
    val json = JsObject(outputFields)
    val ast_pp = json.prettyPrint
    SysUtils.writeFileContent(jobOutputPath, ast_pp)

    Success(s"success ${op}")
  }

  private def parseStreamAllFiles(s: String): Boolean = {
    s.toLowerCase match {
      case "true"  => true
      case "false" => false
      case other =>
        throw new Exception(
            s"""|the streamAllFiles flag must be a boolean (true,false).
                |Value ${other} is illegal.""".stripMargin
              .replaceAll("\n", " ")
        )
    }
  }

  // Make a list of all the files cloned for access by this applet.
  // Bulk describe all the them.
  private def runtimeBulkFileDescribe(dxApi: DxApi,
                                      jobInputPath: Path): Map[String, (DxFile, DxFileDescribe)] = {
    val inputs: JsValue = SysUtils.readFileContent(jobInputPath).parseJson

    val allFilesReferenced = inputs.asJsObject.fields.flatMap {
      case (_, jsElem) => dxApi.findFiles(jsElem)
    }.toVector

    // Describe all the files, in one go
    val descAll = dxApi.fileBulkDescribe(allFilesReferenced)
    descAll.map {
      case (file, desc) => file.id -> (file, desc)
    }
  }

  private def getWdlSourceCodeFromDetails(details: JsValue): String = {
    val fields = details.asJsObject.fields
    val JsString(wdlSourceCode) = fields.getOrElse("wdlSourceCode", fields("womSourceCode"))
    wdlSourceCode
  }

  // Get the WDL source code, and the instance type database from the
  // details field stored on the platform
  private def retrieveFromDetails(
      dxApi: DxApi,
      jobInfoPath: Path
  ): (String, InstanceTypeDB, JsValue, Option[WdlRuntimeAttrs], Option[Boolean]) = {
    val jobInfo = SysUtils.readFileContent(jobInfoPath).parseJson
    val applet: DxApplet = jobInfo.asJsObject.fields.get("applet") match {
      case None =>
        dxApi.logger.trace(
            s"""|applet field not found locally, performing
                |an API call.
                |""".stripMargin,
            minLevel = TraceLevel.None
        )
        val dxJob = dxApi.currentJob
        dxJob.describe().applet
      case Some(JsString(x)) =>
        dxApi.applet(x)
      case Some(other) =>
        throw new Exception(s"malformed applet field ${other} in job info")
    }

    val details: JsValue = applet.describe(Set(Field.Details)).details.get
    val wdlSourceCodeEncoded = getWdlSourceCodeFromDetails(details)
    val wdlSourceCode = SysUtils.base64DecodeAndGunzip(wdlSourceCodeEncoded)

    val JsString(instanceTypeDBEncoded) = details.asJsObject.fields("instanceTypeDB")
    val dbRaw = SysUtils.base64DecodeAndGunzip(instanceTypeDBEncoded)
    val instanceTypeDB = dbRaw.parseJson.convertTo[InstanceTypeDB]

    val runtimeAttrs: Option[WdlRuntimeAttrs] =
      details.asJsObject.fields.get("runtimeAttrs") match {
        case None         => None
        case Some(JsNull) => None
        case Some(x)      => Some(x.convertTo[WdlRuntimeAttrs])
      }

    val delayWorkspaceDestruction: Option[Boolean] =
      details.asJsObject.fields.get("delayWorkspaceDestruction") match {
        case Some(JsBoolean(flag)) => Some(flag)
        case None                  => None
      }
    (wdlSourceCode, instanceTypeDB, details, runtimeAttrs, delayWorkspaceDestruction)
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
            "message" -> JsString(JsUtils.sanitize(e.getMessage))
        )
    ).prettyPrint
    SysUtils.writeFileContent(jobErrorPath, errMsg)

    // Write out a full stack trace to standard error.
    System.err.println(exceptionToString(e))
  }

  def dispatchCommand(args: Seq[String]): Termination = {
    val operation = ExecAction.values.find(x => normKey(x.toString) == normKey(args.head))
    operation match {
      case None =>
        Failure(s"unknown internal action ${args.head}")
      case Some(op) if args.length == 4 =>
        val homeDir = Paths.get(args(1))
        val logger = Logger(quiet = false, traceLevel = getTraceLevel(Some(args(2))))
        val dxApi = DxApi(logger)
        val streamAllFiles = parseStreamAllFiles(args(3))
        val (jobInputPath, jobOutputPath, jobErrorPath, jobInfoPath) = jobFilesOfHomeDir(homeDir)
        val dxPathConfig = buildRuntimePathConfig(streamAllFiles, logger)
        val fileInfoDir = runtimeBulkFileDescribe(dxApi, jobInputPath)
        val dxIoFunctions = DxFileAccessProtocol(dxApi, fileInfoDir, dxPathConfig)

        // Get the WDL source code (currently WDL, could be also CWL in the future)
        // Parse the inputs, convert to WDL values.
        val (wdlSourceCode,
             instanceTypeDB,
             metaInfo,
             defaultRuntimeAttrs,
             delayWorkspaceDestruction) =
          retrieveFromDetails(dxApi, jobInfoPath)

        try {
          op match {
            case ExecAction.Collect | ExecAction.WfFragment | ExecAction.WfInputs |
                ExecAction.WfOutputs | ExecAction.WorkflowOutputReorg |
                ExecAction.WfCustomReorgOutputs =>
              workflowFragAction(
                  op,
                  wdlSourceCode,
                  instanceTypeDB,
                  metaInfo,
                  jobInputPath,
                  jobOutputPath,
                  dxPathConfig,
                  dxIoFunctions,
                  defaultRuntimeAttrs,
                  delayWorkspaceDestruction,
                  dxApi
              )
            case ExecAction.TaskCheckInstanceType | ExecAction.TaskProlog |
                ExecAction.TaskInstantiateCommand | ExecAction.TaskEpilog |
                ExecAction.TaskRelaunch =>
              taskAction(op,
                         wdlSourceCode,
                         instanceTypeDB,
                         jobInputPath,
                         jobOutputPath,
                         dxPathConfig,
                         dxIoFunctions,
                         defaultRuntimeAttrs,
                         delayWorkspaceDestruction,
                         dxApi)
          }
        } catch {
          case e: Throwable =>
            writeJobError(jobErrorPath, e)
            Failure(s"failure running ${op}")
        }
      case Some(_) =>
        BadUsageTermination(s"""|Bad arguments to internal operation
                                |  ${args}
                                |Usage:
                                |  java -jar dxWDL.jar internal <action> <home dir> <debug level>
                                |""".stripMargin)
    }
  }

  terminate(dispatchCommand(args.toSeq))
}
