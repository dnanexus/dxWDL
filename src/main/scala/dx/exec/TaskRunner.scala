/** Run a wdl task.

In the example below, we want to run the Add task in a dx:applet.

task Add {
    Int a
    Int b

    command {
        echo $((a + b))
    }
    output {
        Int sum = read_int(stdout())
    }
}

  */
package dx.exec

import java.lang.management._
import java.nio.file.{Files, Path}

import dx.api.{DxApi, DxJob, InstanceTypeDB}
import dx.compiler.WdlRuntimeAttrs
import dx.exec
import dx.core.io.{DxPathConfig, DxdaManifest, DxfuseManifest}
import dx.core.languages.wdl._
import dx.core.getVersion
import spray.json._
import wdlTools.eval.{Eval, WdlValues, Context => EvalContext}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{FileSourceResolver, FileUtils, SysUtils, TraceLevel}

case class TaskRunner(task: TAT.Task,
                      document: TAT.Document,
                      typeAliases: Map[String, WdlTypes.T],
                      instanceTypeDb: InstanceTypeDB,
                      dxPathConfig: DxPathConfig,
                      fileResolver: FileSourceResolver,
                      wdlVarLinksConverter: WdlVarLinksConverter,
                      jobInputOutput: JobInputOutput,
                      defaultRuntimeAttrs: Option[WdlRuntimeAttrs],
                      delayWorkspaceDestruction: Option[Boolean],
                      dxApi: DxApi,
                      evaluator: Eval) {
  private def printDirStruct(): Unit = {
    dxApi.logger.traceLimited("Directory structure:", minLevel = TraceLevel.VVerbose)
    val (stdout, _) = SysUtils.execCommand("ls -lR", None)
    dxApi.logger.traceLimited(stdout + "\n", 10000, minLevel = TraceLevel.VVerbose)
  }

  // Figure how much memory is available for this container/image
  //
  // We may need to check the cgroup itself. Not sure how portable that is.
  //val totalAvailableMemoryBytes = Utils.readFileContent("/sys/fs/cgroup/memory/memory.limit_in_bytes").toInt
  //
  private def availableMemory(): Long = {
    val mbean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    mbean.getTotalPhysicalMemorySize
  }

  // Write the core bash script into a file. In some cases, we
  // need to run some shell setup statements before and after this
  // script.
  private def writeBashScript(command: String): Unit = {
    // This is based on Cromwell code from
    // [BackgroundAsyncJobExecutionActor.scala].  Generate a bash
    // script that captures standard output, and standard
    // error. We need to be careful to pipe stdout/stderr to the
    // parent stdout/stderr, and not lose the result code of the
    // shell command. Notes on bash magic symbols used here:
    //
    //  Symbol  Explanation
    //    >     redirect stdout
    //    2>    redirect stderr
    //    <     redirect stdin
    //
    val script =
      if (command.isEmpty) {
        s"""|#!/bin/bash
            |echo 0 > ${dxPathConfig.rcPath}
            |""".stripMargin.trim + "\n"
      } else {
        // It would have been easier to have one long
        // section bracketed with triple quotes (`"""`). However,
        // some characters are dropped from [command] this way, for
        // example pipe ('|').
        val part1 =
          s"""|#!/bin/bash
              |(
              |    cd ${dxPathConfig.homeDir.toString}
              |""".stripMargin
        val part2 =
          s"""|) \\
              |  > >( tee ${dxPathConfig.stdout} ) \\
              |  2> >( tee ${dxPathConfig.stderr} >&2 )
              |
              |echo $$? > ${dxPathConfig.rcPath}
              |
              |# make sure the files are on stable storage
              |# before leaving. This helps with stdin and stdout
              |# characters that may be in the fifo queues.
              |sync
              |""".stripMargin
        Vector(part1, command, part2).mkString("\n")
      }
    dxApi.logger.traceLimited(s"writing bash script to ${dxPathConfig.script}")
    FileUtils.writeFileContent(dxPathConfig.script, script)
    dxPathConfig.script.toFile.setExecutable(true)
  }

  private def writeDockerSubmitBashScript(imgName: String, dxfuseRunning: Boolean): Unit = {
    // The user wants to use a docker container with the
    // image [imgName].
    //
    // Map the home directory into the container, so that
    // we can reach the result files, and upload them to
    // the platform.
    //

    // Limit the docker container to leave some memory for the rest of the
    // ongoing system services, for example, dxfuse.
    //
    // This doesn't work with our current docker installation, unfortunately.
    // I am leaving the code here, so we could fix this in the future.
    val totalAvailableMemoryBytes = availableMemory()
    val memCap =
      if (dxfuseRunning)
        totalAvailableMemoryBytes - exec.DXFUSE_MAX_MEMORY_CONSUMPTION
      else
        totalAvailableMemoryBytes

    val dockerRunScript =
      s"""|#!/bin/bash -x
          |
          |# make sure there is no preexisting Docker CID file
          |rm -f ${dxPathConfig.dockerCid}
          |
          |# Run the container under a priviliged user, so it will have
          |# permissions to read/write files in the home directory. This
          |# is required in cases where the container uses a different
          |# user.
          |extraFlags="--user $$(id -u):$$(id -g) --hostname $$(hostname)"
          |
          |# run as in the original configuration
          |docker run \\
          |  --memory=${memCap} \\
          |  --cidfile ${dxPathConfig.dockerCid} \\
          |  $${extraFlags} \\
          |  --entrypoint /bin/bash \\
          |  -v ${dxPathConfig.homeDir}:${dxPathConfig.homeDir} \\
          |  ${imgName} ${dxPathConfig.script.toString}
          |
          |# get the return code (working even if the container was detached)
          |rc=$$(docker wait `cat ${dxPathConfig.dockerCid.toString}`)
          |
          |# remove the container after waiting
          |docker rm `cat ${dxPathConfig.dockerCid.toString}`
          |
          |# return exit code
          |exit $$rc
          |""".stripMargin

    //  -v ${dxPathConfig.dxfuseMountpoint}:${dxPathConfig.dxfuseMountpoint}

    dxApi.logger.traceLimited(s"writing docker run script to ${dxPathConfig.dockerSubmitScript}")
    FileUtils.writeFileContent(dxPathConfig.dockerSubmitScript, dockerRunScript)
    dxPathConfig.dockerSubmitScript.toFile.setExecutable(true)
  }

  /**
    * Check if we are already on the correct instance type. This allows for avoiding unnecessary
    * relaunch operations.
    */
  def checkInstanceType(runnerEval: RunnerEval): Boolean = {
    // evaluate the runtime attributes
    // determine the instance type
    val requiredInstanceType: String = runnerEval.calcInstanceType(instanceTypeDb)
    dxApi.logger.traceLimited(s"required instance type: ${requiredInstanceType}")

    // Figure out which instance we are on right now
    val dxJob = dxApi.currentJob
    val descFieldReq = Map("fields" -> JsObject("instanceType" -> JsBoolean(true)))
    val retval = dxApi.jobDescribe(dxJob.id, descFieldReq)
    val crntInstanceType: String = retval.fields.get("instanceType") match {
      case Some(JsString(x)) => x
      case _                 => throw new Exception(s"wrong type for instanceType ${retval}")
    }
    dxApi.logger.traceLimited(s"current instance type: ${crntInstanceType}")

    val isSufficient = instanceTypeDb.lteqByResources(requiredInstanceType, crntInstanceType)
    dxApi.logger.traceLimited(s"isSufficient? ${isSufficient}")
    isSufficient
  }

  // Calculate the input variables for the task, download the input files,
  // and build a shell script to run the command.
  private def prolog(inputs: Map[TAT.InputDefinition, WdlValues.V]): LocalizedRunnerInputs = {
    if (dxApi.logger.isVerbose) {
      dxApi.logger.traceLimited(s"Prolog debugLevel=${dxApi.logger.traceLevel}")
      dxApi.logger.traceLimited(s"dxWDL version: ${getVersion}")
      if (dxApi.logger.traceLevel >= TraceLevel.VVerbose) {
        printDirStruct()
      }
      dxApi.logger.traceLimited(s"Task source code:")
      dxApi.logger.traceLimited(document.source.readString, 10000)
      dxApi.logger.traceLimited(s"inputs: ${inputs}")
    }

    val parameterMeta: Map[String, TAT.MetaValue] = task.parameterMeta match {
      case None                                   => Map.empty
      case Some(TAT.ParameterMetaSection(kvs, _)) => kvs
    }

    // Download/stream all input files.
    //
    // Note: this may be overly conservative,
    // because some of the files may not actually be accessed.
    val (localizedInputs, fileSourceToPath, dxdaManifest, dxfuseManifest) =
      jobInputOutput.localizeFiles(parameterMeta, inputs, dxPathConfig.inputFilesDir)

    // build a manifest for dxda, if there are files to download
    val DxdaManifest(manifestJs) = dxdaManifest
    if (manifestJs.asJsObject.fields.nonEmpty) {
      FileUtils.writeFileContent(dxPathConfig.dxdaManifest, manifestJs.prettyPrint)
    }

    // build a manifest for dxfuse
    val DxfuseManifest(manifest2Js) = dxfuseManifest
    if (manifest2Js != JsNull) {
      FileUtils.writeFileContent(dxPathConfig.dxfuseManifest, manifest2Js.prettyPrint)
    }

    val inputsWithTypes: Map[String, (WdlTypes.T, WdlValues.V)] =
      localizedInputs.map {
        case (inpDfn, value) =>
          inpDfn.name -> (inpDfn.wdlType, value)
      }

    dxApi.logger.traceLimited(s"Epilog: complete, inputsWithTypes = ${localizedInputs}")
    LocalizedRunnerInputs(inputsWithTypes, fileSourceToPath, task, evaluator)
  }

  def prologStep(inputs: Map[TAT.InputDefinition, WdlValues.V]): Unit = {
    val localizedRunnerEnv = prolog(inputs)
    localizedRunnerEnv.writeToDisk(dxPathConfig.runnerTaskEnv, typeAliases)
  }

  // instantiate the bash command
  private def instantiateCommand(runnerEnv: LocalizedRunnerInputs): Unit = {
    dxApi.logger.traceLimited(s"InstantiateCommand, env = ${runnerEnv}")
    val runnerEval = RunnerEval(task, runnerEnv, defaultRuntimeAttrs, dxApi.logger, evaluator)
    // Write shell script to a file. It will be executed by the dx-applet shell code.
    writeBashScript(runnerEval.command)
    runnerEval.dockerImage match {
      case Some(img) =>
        // write a script that launches the actual command inside a docker image.
        writeDockerSubmitBashScript(img, Files.exists(dxPathConfig.dxfuseManifest))
      case None => ()
    }
  }

  def instantiateCommandStep(): Unit = {
    instantiateCommand(LocalizedRunnerInputs(dxPathConfig.runnerTaskEnv, typeAliases, fileResolver))
  }

  // Run docker and/or the bash script
  private def epilog(
      runnerEnv: LocalizedRunnerInputs
  ): Map[String, JsValue] = {
    dxApi.logger.traceLimited(s"Epilog debugLevel=${dxApi.logger.traceLevel}")
    if (dxApi.logger.traceLevel >= TraceLevel.VVerbose) {
      printDirStruct()
    }

    // Evaluate the output declarations. Add outputs evaluated to
    // the environment, so they can be referenced by expressions in the next
    // lines.
    val outputsLocal: Map[String, (WdlTypes.T, WdlValues.V)] =
      task.outputs
        .foldLeft(Map.empty[String, (WdlTypes.T, WdlValues.V)]) {
          case (env, outDef: TAT.OutputDefinition) =>
            val value =
              evaluator.applyExprAndCoerce(outDef.expr,
                                           outDef.wdlType,
                                           EvalContext.createFromEnv(env ++ runnerEnv.env))
            env + (outDef.name -> (outDef.wdlType, value))
        }

    val outputs: Map[String, (WdlTypes.T, WdlValues.V)] =
      // Upload output files to the platform.
      jobInputOutput.delocalizeFiles(outputsLocal, runnerEnv.fileSourceToPath)

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = outputs
      .map {
        case (outputVarName, (wdlType, wdlValue)) =>
          val wvl = wdlVarLinksConverter.importFromWDL(wdlType, wdlValue)
          wdlVarLinksConverter.genFields(wvl, outputVarName)
      }
      .toVector
      .flatten
      .toMap
    outputFields
  }

  def epilogStep(): Map[String, JsValue] = {
    val runnerEnv = LocalizedRunnerInputs(dxPathConfig.runnerTaskEnv, typeAliases, fileResolver)
    epilog(runnerEnv)
  }

  def runTask(inputs: Map[TAT.InputDefinition, WdlValues.V]): Map[String, JsValue] = {
    // prolog
    val runnerEnv = prolog(inputs)

    // instantiate the command
    instantiateCommand(runnerEnv)

    // execute the shell script in a child job
    val script: Path = dxPathConfig.script
    if (Files.exists(script)) {
      SysUtils.execScript(script, None)
    }

    // epilog
    epilog(runnerEnv)
  }

  /**
    * The runtime attributes need to be calculated at runtime. Evaluate them,
    * determine the instance type [xxxx], and relaunch the job on [xxxx]
    */
  def relaunch(runnerEval: RunnerEval, originalInputs: JsValue): Map[String, JsValue] = {
    dxApi.logger.traceLimited(s"inputs: ${runnerEval}")

    // evaluate the runtime attributes
    // determine the instance type
    val instanceType: String = runnerEval.calcInstanceType(instanceTypeDb)

    // Run a sub-job with the "body" entry point, and the required instance type
    val dxSubJob: DxJob =
      dxApi.runSubJob("body",
                      Some(instanceType),
                      originalInputs,
                      Vector.empty,
                      delayWorkspaceDestruction)

    // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
    // is exactly the same as the parent, we can immediately exit the parent job.
    val outputs: Map[String, JsValue] = task.outputs.flatMap { outDef: TAT.OutputDefinition =>
      val wvl = WdlVarLinks(outDef.wdlType, DxlExec(dxSubJob, outDef.name))
      wdlVarLinksConverter.genFields(wvl, outDef.name)
    }.toMap
    outputs
  }
}
