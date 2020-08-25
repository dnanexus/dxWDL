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
import java.nio.file.{Files, Path, Paths}

import dx.AppInternalException
import dx.api.{DxApi, DxJob, InstanceTypeDB}
import dx.compiler.WdlRuntimeAttrs
import dx.exec
import dx.core.io.{DxPathConfig, DxdaManifest, DxfuseManifest}
import dx.core.languages.wdl._
import dx.core.getVersion
import dx.core.ir.ParameterLinkExec
import spray.json._
import wdlTools.eval.{Eval, WdlValues, Context => EvalContext}
import wdlTools.exec.DockerUtils
import wdlTools.syntax.SourceLocation
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{
  FileSource,
  FileSourceResolver,
  FileUtils,
  RealFileSource,
  SysUtils,
  TraceLevel
}

case class TaskRunner(task: TAT.Task,
                      document: TAT.Document,
                      typeAliases: Map[String, WdlTypes.T],
                      instanceTypeDB: InstanceTypeDB,
                      dxPathConfig: DxPathConfig,
                      fileResolver: FileSourceResolver,
                      wdlVarLinksConverter: ParameterLinkSerde,
                      jobInputOutput: JobInputOutput,
                      defaultRuntimeAttrs: Option[WdlRuntimeAttrs],
                      delayWorkspaceDestruction: Option[Boolean],
                      dxApi: DxApi,
                      evaluator: Eval) {
  private val dockerUtils = DockerUtils(fileResolver, dxApi.logger)

  // serialize the task inputs to json, and then write to a file.
  def writeEnvToDisk(localizedInputs: Map[String, (WdlTypes.T, WdlValues.V)],
                     fileSourceToPath: Map[FileSource, Path]): Unit = {
    val locInputsM: Map[String, JsValue] = localizedInputs.map {
      case (name, (t, v)) =>
        val wdlTypeRepr = TypeSerialization(typeAliases).toString(t)
        val value = WdlValueSerialization(typeAliases).toJSON(t, v)
        (name, JsArray(JsString(wdlTypeRepr), value))
    }
    val dxUrlM: Map[String, JsValue] = fileSourceToPath.map {
      case (fileSource: RealFileSource, path) => fileSource.value -> JsString(path.toString)
      case (other, _) =>
        throw new RuntimeException(s"Can only serialize a RealFileSource, not ${other}")
    }

    // marshal into json, and then to a string
    val json = JsObject("localizedInputs" -> JsObject(locInputsM), "dxUrl2path" -> JsObject(dxUrlM))
    FileUtils.writeFileContent(dxPathConfig.runnerTaskEnv, json.prettyPrint)
  }

  def readEnvFromDisk(): (Map[String, (WdlTypes.T, WdlValues.V)], Map[FileSource, Path]) = {
    val buf = FileUtils.readFileContent(dxPathConfig.runnerTaskEnv)
    val json: JsValue = buf.parseJson
    val (localPathToJs, dxUriToJs) = json match {
      case JsObject(m) =>
        (m.get("localizedInputs"), m.get("dxUrl2path")) match {
          case (Some(JsObject(env_m)), Some(JsObject(path_m))) =>
            (env_m, path_m)
          case (_, _) =>
            throw new Exception("Malformed environment serialized to disk")
        }
      case _ => throw new Exception("Malformed environment serialized to disk")
    }
    val localizedInputs = localPathToJs.map {
      case (key, JsArray(Vector(JsString(wdlTypeRepr), jsVal))) =>
        val t = TypeSerialization(typeAliases).fromString(wdlTypeRepr)
        val value = WdlValueSerialization(typeAliases).fromJSON(jsVal)
        key -> (t, value)
      case (_, other) =>
        throw new Exception(s"Bad deserialization value ${other}")
    }
    val fileSourceToPath = dxUriToJs.map {
      case (uri, JsString(path)) => fileResolver.resolve(uri) -> Paths.get(path)
      case other                 => throw new Exception(s"Invalid map item ${other}")
    }
    (localizedInputs, fileSourceToPath)
  }

  private def printDirStruct(): Unit = {
    dxApi.logger.traceLimited("Directory structure:", minLevel = TraceLevel.VVerbose)
    val (_, stdout, _) = SysUtils.execCommand("ls -lR", None)
    dxApi.logger.traceLimited(stdout + "\n", 10000, minLevel = TraceLevel.VVerbose)
  }

  // Figure out if a docker image is specified. If so, return it as a string.
  private def dockerImageEval(env: Map[String, WdlValues.V]): Option[String] = {
    val attributes: Map[String, TAT.Expr] = task.runtime match {
      case None                             => Map.empty
      case Some(TAT.RuntimeSection(kvs, _)) => kvs
    }
    val dImg: Option[WdlValues.V] = attributes.get("docker") match {
      case None =>
        defaultRuntimeAttrs match {
          case None      => None
          case Some(dra) => dra.m.get("docker")
        }
      case Some(expr) =>
        val value = evaluator.applyExprAndCoerce(expr, WdlTypes.T_String, EvalContext(env))
        Some(value)
    }
    dImg match {
      case None                        => None
      case Some(WdlValues.V_String(s)) => Some(s)
      case Some(other) =>
        throw new AppInternalException(s"docker is not a string expression ${other}")
    }
  }

  private def dockerImage(env: Map[String, WdlValues.V], loc: SourceLocation): Option[String] = {
    dockerImageEval(env) match {
      case Some(nameOrUrl) => Some(dockerUtils.getImage(nameOrUrl, loc))
      case other           => other
    }
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

  private def inputsDbg(inputs: Map[TAT.InputDefinition, WdlValues.V]): String = {
    inputs
      .map {
        case (inp, value) =>
          s"${inp.name} -> (${inp.wdlType}, ${value})"
      }
      .mkString("\n")
  }

  private def stripTypesFromEnv(
      env: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, WdlValues.V] = {
    env.map { case (name, (_, v)) => name -> v }
  }

  private def evalInputsAndDeclarations(
      inputsWithTypes: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, (WdlTypes.T, WdlValues.V)] = {
    // evaluate the declarations using the inputs
    val env: Map[String, (WdlTypes.T, WdlValues.V)] =
      task.declarations.foldLeft(inputsWithTypes) {
        case (env, TAT.Declaration(name, wdlType, Some(expr), _)) =>
          val wdlValue =
            evaluator.applyExprAndCoerce(expr, wdlType, EvalContext(stripTypesFromEnv(env)))
          env + (name -> (wdlType, wdlValue))
        case (_, TAT.Declaration(name, _, None, _)) =>
          throw new Exception(s"Declaration ${name} has no expression")
      }
    env
  }

  // Calculate the input variables for the task, download the input files,
  // and build a shell script to run the command.
  def prolog(
      taskInputs: Map[TAT.InputDefinition, WdlValues.V]
  ): (Map[String, (WdlTypes.T, WdlValues.V)], Map[FileSource, Path]) = {
    if (dxApi.logger.isVerbose) {
      dxApi.logger.traceLimited(s"Prolog debugLevel=${dxApi.logger.traceLevel}")
      dxApi.logger.traceLimited(s"dxWDL version: ${getVersion}")
      if (dxApi.logger.traceLevel >= TraceLevel.VVerbose) {
        printDirStruct()
      }
      dxApi.logger.traceLimited(s"Task source code:")
      dxApi.logger.traceLimited(document.source.readString, 10000)
      dxApi.logger.traceLimited(s"inputs: ${inputsDbg(taskInputs)}")
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
      jobInputOutput.localizeFiles(parameterMeta, taskInputs, dxPathConfig.inputFilesDir)

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
    (inputsWithTypes, fileSourceToPath)
  }

  // HERE we download all the inputs, or mount them with dxfuse

  // instantiate the bash command
  def instantiateCommand(
      localizedInputs: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, (WdlTypes.T, WdlValues.V)] = {
    dxApi.logger.traceLimited(s"InstantiateCommand, env = ${localizedInputs}")

    val env = evalInputsAndDeclarations(localizedInputs)
    val docker = dockerImage(stripTypesFromEnv(env), task.command.loc)

    // instantiate the command
    val command = evaluator.applyCommand(task.command, EvalContext(stripTypesFromEnv(env)))

    // Write shell script to a file. It will be executed by the dx-applet shell code.
    writeBashScript(command)
    docker match {
      case Some(img) =>
        // write a script that launches the actual command inside a docker image.
        writeDockerSubmitBashScript(img, Files.exists(dxPathConfig.dxfuseManifest))
      case None => ()
    }

    // Record the localized inputs, we need them in the ilog
    env
  }

  // HERE we run docker and/or the bash script

  def epilog(localizedInputs: Map[String, (WdlTypes.T, WdlValues.V)],
             fileSourceToPath: Map[FileSource, Path]): Map[String, JsValue] = {
    dxApi.logger.traceLimited(s"Epilog debugLevel=${dxApi.logger.traceLevel}")
    if (dxApi.logger.traceLevel >= TraceLevel.VVerbose) {
      printDirStruct()
    }

    // Evaluate the output declarations. Add outputs evaluated to the environment,
    // so they can be referenced by expressions in the next lines.
    val outputsLocal: Map[String, (WdlTypes.T, WdlValues.V)] =
      task.outputs
        .foldLeft(Map.empty[String, (WdlTypes.T, WdlValues.V)]) {
          case (env, outDef: TAT.OutputDefinition) =>
            val inputsNoTypes = stripTypesFromEnv(localizedInputs)
            val envNoTypes = stripTypesFromEnv(env)
            val value = evaluator.applyExprAndCoerce(outDef.expr,
                                                     outDef.wdlType,
                                                     EvalContext(envNoTypes ++ inputsNoTypes))
            env + (outDef.name -> (outDef.wdlType, value))
        }

    val outputs: Map[String, (WdlTypes.T, WdlValues.V)] =
      // Upload output files to the platform.
      jobInputOutput.delocalizeFiles(outputsLocal, fileSourceToPath)

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = outputs
      .map {
        case (outputVarName, (wdlType, wdlValue)) =>
          val wvl = wdlVarLinksConverter.createLink(wdlType, wdlValue)
          wdlVarLinksConverter.createFields(wvl, outputVarName)
      }
      .toVector
      .flatten
      .toMap
    outputFields
  }

  // Evaluate the runtime expressions, and figure out which instance type
  // this task requires.
  //
  // Do not download the files, if there are any. We may be
  // calculating the instance type in the workflow runner, outside
  // the task.
  def calcInstanceType(taskInputs: Map[TAT.InputDefinition, WdlValues.V]): String = {
    dxApi.logger.traceLimited("calcInstanceType", minLevel = TraceLevel.VVerbose)
    dxApi.logger.traceLimited(s"inputs: ${inputsDbg(taskInputs)}", minLevel = TraceLevel.VVerbose)

    val inputsWithTypes: Map[String, (WdlTypes.T, WdlValues.V)] =
      taskInputs.map {
        case (inpDfn, value) =>
          inpDfn.name -> (inpDfn.wdlType, value)
      }
    val env = evalInputsAndDeclarations(inputsWithTypes)
    val runtimeAttrs: Map[String, TAT.Expr] = task.runtime match {
      case None                             => Map.empty
      case Some(TAT.RuntimeSection(kvs, _)) => kvs
    }

    def evalAttr(attrName: String): Option[WdlValues.V] = {
      runtimeAttrs.get(attrName) match {
        case None =>
          // try the defaults
          defaultRuntimeAttrs match {
            case None      => None
            case Some(dra) => dra.m.get(attrName)
          }
        case Some(expr) =>
          Some(
              evaluator
                .applyExprAndCoerce(expr, WdlTypes.T_String, EvalContext(stripTypesFromEnv(env)))
          )
      }
    }

    val dxInstanceType = evalAttr("dx_instance_type")
    val memory = evalAttr("memory")
    val diskSpace = evalAttr("disks")
    val cores = evalAttr("cpu")
    val gpu = evalAttr("gpu")
    val iTypeRaw = InstanceTypes.parse(dxInstanceType, memory, diskSpace, cores, gpu)
    val iType = instanceTypeDB.apply(iTypeRaw)
    dxApi.logger.traceLimited(
        s"""|calcInstanceType memory=${memory} disk=${diskSpace}
            |cores=${cores} instancetype=${iType}""".stripMargin
          .replaceAll("\n", " ")
    )
    iType
  }

  /** Check if we are already on the correct instance type. This allows for avoiding unnecessary
    * relaunch operations.
    */
  def checkInstanceType(inputs: Map[TAT.InputDefinition, WdlValues.V]): Boolean = {
    // evaluate the runtime attributes
    // determine the instance type
    dxApi.logger.traceLimited(s"inputs: ${inputsDbg(inputs)}")

    val requiredInstanceType: String = calcInstanceType(inputs)
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

    val isSufficient = instanceTypeDB.lteqByResources(requiredInstanceType, crntInstanceType)
    dxApi.logger.traceLimited(s"isSufficient? ${isSufficient}")
    isSufficient
  }

  /** The runtime attributes need to be calculated at runtime. Evaluate them,
    *  determine the instance type [xxxx], and relaunch the job on [xxxx]
    */
  def relaunch(inputs: Map[TAT.InputDefinition, WdlValues.V],
               originalInputs: JsValue): Map[String, JsValue] = {
    dxApi.logger.traceLimited(s"inputs: ${inputsDbg(inputs)}")

    // evaluate the runtime attributes
    // determine the instance type
    val instanceType: String = calcInstanceType(inputs)

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
      val wvl = WdlDxLink(outDef.wdlType, ParameterLinkExec(dxSubJob, outDef.name))
      wdlVarLinksConverter.createFields(wvl, outDef.name)
    }.toMap
    outputs
  }
}
