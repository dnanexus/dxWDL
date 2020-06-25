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
import dx.api.{DxApi, DxJob, DxPath, InstanceTypeDB}
import dx.compiler.WdlRuntimeAttrs
import dx.exec
import dx.core.io.{DxPathConfig, DxdaManifest, DxfuseManifest, Furl}
import dx.core.languages.wdl._
import dx.core.util.SysUtils
import dx.util.{TraceLevel, getVersion}
import spray.json._
import wdlTools.eval.{WdlValues, Context => EvalContext}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}

// This object is used to allow easy testing of complex
// methods internal to the TaskRunner.
object TaskRunnerUtils {
  // Read the manifest file from a docker tarball, and get the repository name.
  //
  // A manifest could look like this:
  // [
  //    {"Config":"4b778ee055da936b387080ba034c05a8fad46d8e50ee24f27dcd0d5166c56819.json",
  //     "RepoTags":["ubuntu_18_04_minimal:latest"],
  //     "Layers":[
  //          "1053541ae4c67d0daa87babb7fe26bf2f5a3b29d03f4af94e9c3cb96128116f5/layer.tar",
  //          "fb1542f1963e61a22f9416077bf5f999753cbf363234bf8c9c5c1992d9a0b97d/layer.tar",
  //          "2652f5844803bcf8615bec64abd20959c023d34644104245b905bb9b08667c8d/layer.tar",
  //          ]}
  // ]
  def readManifestGetDockerImageName(buf: String): String = {
    val jso = buf.parseJson
    val elem = jso match {
      case JsArray(elements) if elements.nonEmpty => elements.head
      case other =>
        throw new Exception(s"bad value ${other} for manifest, expecting non empty array")
    }
    val repo: String = elem.asJsObject.fields.get("RepoTags") match {
      case None =>
        throw new Exception("The repository is not specified for the image")
      case Some(JsString(repo)) =>
        repo
      case Some(JsArray(elements)) =>
        if (elements.isEmpty)
          throw new Exception("RepoTags has an empty array")
        elements.head match {
          case JsString(repo) => repo
          case other          => throw new Exception(s"bad value ${other} in RepoTags manifest field")
        }
      case other =>
        throw new Exception(s"bad value ${other} in RepoTags manifest field")
    }
    repo
  }
}

// We can't use the name Task, because that would confuse it with the
// WDL language definition.
case class TaskRunner(task: TAT.Task,
                      document: TAT.Document,
                      typeAliases: Map[String, WdlTypes.T],
                      instanceTypeDB: InstanceTypeDB,
                      dxPathConfig: DxPathConfig,
                      dxIoFunctions: DxFileAccessProtocol,
                      jobInputOutput: JobInputOutput,
                      defaultRuntimeAttrs: Option[WdlRuntimeAttrs],
                      delayWorkspaceDestruction: Option[Boolean],
                      dxApi: DxApi) {
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(dxApi, dxIoFunctions.fileInfoDir, typeAliases)
  private val DOCKER_TARBALLS_DIR = "/tmp/docker-tarballs"

  // build an object capable of evaluating WDL expressions
  private val evaluator = Evaluator.make(dxIoFunctions, document.version.value)

  // serialize the task inputs to json, and then write to a file.
  def writeEnvToDisk(localizedInputs: Map[String, (WdlTypes.T, WdlValues.V)],
                     dxUrl2path: Map[Furl, Path]): Unit = {
    val locInputsM: Map[String, JsValue] = localizedInputs.map {
      case (name, (t, v)) =>
        val wdlTypeRepr = TypeSerialization(typeAliases).toString(t)
        val value = WdlValueSerialization(typeAliases).toJSON(t, v)
        (name, JsArray(JsString(wdlTypeRepr), value))
    }
    val dxUrlM: Map[String, JsValue] = dxUrl2path.map {
      case (furl, path) => furl.toString -> JsString(path.toString)
    }

    // marshal into json, and then to a string
    val json = JsObject("localizedInputs" -> JsObject(locInputsM), "dxUrl2path" -> JsObject(dxUrlM))
    SysUtils.writeFileContent(dxPathConfig.runnerTaskEnv, json.prettyPrint)
  }

  def readEnvFromDisk(): (Map[String, (WdlTypes.T, WdlValues.V)], Map[Furl, Path]) = {
    val buf = SysUtils.readFileContent(dxPathConfig.runnerTaskEnv)
    val json: JsValue = buf.parseJson
    val (locInputsM, dxUrlM) = json match {
      case JsObject(m) =>
        (m.get("localizedInputs"), m.get("dxUrl2path")) match {
          case (Some(JsObject(env_m)), Some(JsObject(path_m))) =>
            (env_m, path_m)
          case (_, _) =>
            throw new Exception("Malformed environment serialized to disk")
        }
      case _ => throw new Exception("Malformed environment serialized to disk")
    }
    val localizedInputs = locInputsM.map {
      case (key, JsArray(Vector(JsString(wdlTypeRepr), jsVal))) =>
        val t = TypeSerialization(typeAliases).fromString(wdlTypeRepr)
        val value = WdlValueSerialization(typeAliases).fromJSON(jsVal)
        key -> (t, value)
      case (_, other) =>
        throw new Exception(s"sanity: bad deserialization value ${other}")
    }
    val dxUrl2path = dxUrlM.map {
      case (key, JsString(path)) => Furl.fromUrl(key, dxApi) -> Paths.get(path)
      case (_, _)                => throw new Exception("Sanity")
    }
    (localizedInputs, dxUrl2path)
  }

  private def printDirStruct(): Unit = {
    dxApi.logger.appletLog("Directory structure:", minLevel = TraceLevel.VVerbose)
    val (stdout, _) = SysUtils.execCommand("ls -lR", None)
    dxApi.logger.appletLog(stdout + "\n", 10000, minLevel = TraceLevel.VVerbose)
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

  private def pullImage(dImg: String): Option[String] = {
    var retry_count = 5
    while (retry_count > 0) {
      try {
        val (outstr, errstr) = SysUtils.execCommand(s"docker pull ${dImg}")

        dxApi.logger.appletLog(
            s"""|output:
                |${outstr}
                |stderr:
                |${errstr}""".stripMargin
        )
        return Some(dImg)
      } catch {
        // ideally should catch specific exception.
        case _: Throwable =>
          retry_count = retry_count - 1
          dxApi.logger.appletLog(
              s"""Failed to pull docker image:
                 |${dImg}. Retrying... ${5 - retry_count}
                    """.stripMargin
          )
          Thread.sleep(1000)
      }
    }
    throw new RuntimeException(s"Unable to pull docker image: ${dImg} after 5 tries")
  }

  private def dockerImage(env: Map[String, WdlValues.V]): Option[String] = {
    val dImg = dockerImageEval(env)
    dImg match {
      case Some(url) if url.startsWith(DxPath.DX_URL_PREFIX) =>
        // a tarball created with "docker save".
        // 1. download it
        // 2. open the tar archive
        // 2. load into the local docker cache
        // 3. figure out the image name
        dxApi.logger.appletLog(s"looking up dx:url ${url}")
        val dxFile = dxApi.resolveDxUrlFile(url)
        val fileName = dxFile.describe().name
        val tarballDir = Paths.get(DOCKER_TARBALLS_DIR)
        SysUtils.safeMkdir(tarballDir)
        val localTar: Path = tarballDir.resolve(fileName)

        dxApi.logger.appletLog(s"downloading docker tarball to ${localTar}")
        dxApi.downloadFile(localTar, dxFile)

        dxApi.logger.appletLog("figuring out the image name")
        val (mContent, _) = SysUtils.execCommand(s"tar --to-stdout -xf ${localTar} manifest.json")
        dxApi.logger.appletLog(
            s"""|manifest content:
                |${mContent}
                |""".stripMargin
        )
        val repo = TaskRunnerUtils.readManifestGetDockerImageName(mContent)
        dxApi.logger.appletLog(s"repository is ${repo}")

        dxApi.logger.appletLog(s"load tarball ${localTar} to docker", minLevel = TraceLevel.None)
        val (outstr, errstr) = SysUtils.execCommand(s"docker load --input ${localTar}")
        dxApi.logger.appletLog(
            s"""|output:
                |${outstr}
                |stderr:
                |${errstr}""".stripMargin
        )
        Some(repo)

      case Some(dImg) =>
        pullImage(dImg)

      case _ =>
        dImg
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
        List(part1, command, part2).mkString("\n")
      }
    dxApi.logger.appletLog(s"writing bash script to ${dxPathConfig.script}")
    SysUtils.writeFileContent(dxPathConfig.script, script)
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

    dxApi.logger.appletLog(s"writing docker run script to ${dxPathConfig.dockerSubmitScript}")
    SysUtils.writeFileContent(dxPathConfig.dockerSubmitScript, dockerRunScript)
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
          throw new Exception(s"sanity: declaration ${name} has no expression")
      }
    env
  }

  // Calculate the input variables for the task, download the input files,
  // and build a shell script to run the command.
  def prolog(
      taskInputs: Map[TAT.InputDefinition, WdlValues.V]
  ): (Map[String, (WdlTypes.T, WdlValues.V)], Map[Furl, Path]) = {
    dxApi.logger.appletLog(s"Prolog debugLevel=${dxApi.logger.traceLevel}")
    dxApi.logger.appletLog(s"dxWDL version: ${getVersion}")
    if (dxApi.logger.traceLevel >= TraceLevel.VVerbose) {
      printDirStruct()
    }
    dxApi.logger.appletLog(s"Task source code:")
    dxApi.logger.appletLog(document.sourceCode, 10000)
    dxApi.logger.appletLog(s"inputs: ${inputsDbg(taskInputs)}")

    val parameterMeta: Map[String, TAT.MetaValue] = task.parameterMeta match {
      case None                                   => Map.empty
      case Some(TAT.ParameterMetaSection(kvs, _)) => kvs
    }

    // Download/stream all input files.
    //
    // Note: this may be overly conservative,
    // because some of the files may not actually be accessed.
    val (localizedInputs, dxUrl2path, dxdaManifest, dxfuseManifest) =
      jobInputOutput.localizeFiles(parameterMeta, taskInputs, dxPathConfig.inputFilesDir)

    // build a manifest for dxda, if there are files to download
    val DxdaManifest(manifestJs) = dxdaManifest
    if (manifestJs.asJsObject.fields.nonEmpty) {
      SysUtils.writeFileContent(dxPathConfig.dxdaManifest, manifestJs.prettyPrint)
    }

    // build a manifest for dxfuse
    val DxfuseManifest(manifest2Js) = dxfuseManifest
    if (manifest2Js != JsNull) {
      SysUtils.writeFileContent(dxPathConfig.dxfuseManifest, manifest2Js.prettyPrint)
    }

    val inputsWithTypes: Map[String, (WdlTypes.T, WdlValues.V)] =
      localizedInputs.map {
        case (inpDfn, value) =>
          inpDfn.name -> (inpDfn.wdlType, value)
      }

    dxApi.logger.appletLog(s"Epilog: complete, inputsWithTypes = ${localizedInputs}")
    (inputsWithTypes, dxUrl2path)
  }

  // HERE we download all the inputs, or mount them with dxfuse

  // instantiate the bash command
  def instantiateCommand(
      localizedInputs: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, (WdlTypes.T, WdlValues.V)] = {
    dxApi.logger.appletLog(s"InstantiateCommand, env = ${localizedInputs}")

    val env = evalInputsAndDeclarations(localizedInputs)
    val docker = dockerImage(stripTypesFromEnv(env))

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
             dxUrl2path: Map[Furl, Path]): Map[String, JsValue] = {
    dxApi.logger.appletLog(s"Epilog debugLevel=${dxApi.logger.traceLevel}")
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
            val inputsNoTypes = stripTypesFromEnv(localizedInputs)
            val envNoTypes = stripTypesFromEnv(env)
            val value = evaluator.applyExprAndCoerce(outDef.expr,
                                                     outDef.wdlType,
                                                     EvalContext(envNoTypes ++ inputsNoTypes))
            env + (outDef.name -> (outDef.wdlType, value))
        }

    val outputs: Map[String, (WdlTypes.T, WdlValues.V)] =
      // Upload output files to the platform.
      jobInputOutput.delocalizeFiles(outputsLocal, dxUrl2path)

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = outputs
      .map {
        case (outputVarName, (wdlType, wdlValue)) =>
          val wvl = wdlVarLinksConverter.importFromWDL(wdlType, wdlValue)
          wdlVarLinksConverter.genFields(wvl, outputVarName)
      }
      .toList
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
    dxApi.logger.appletLog("calcInstanceType", minLevel = TraceLevel.VVerbose)
    dxApi.logger.appletLog(s"inputs: ${inputsDbg(taskInputs)}", minLevel = TraceLevel.VVerbose)

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
    dxApi.logger.appletLog(
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
    dxApi.logger.appletLog(s"inputs: ${inputsDbg(inputs)}")

    val requiredInstanceType: String = calcInstanceType(inputs)
    dxApi.logger.appletLog(s"required instance type: ${requiredInstanceType}")

    // Figure out which instance we are on right now
    val dxJob = dxApi.currentJob
    val descFieldReq = Map("fields" -> JsObject("instanceType" -> JsBoolean(true)))
    val retval = dxApi.jobDescribe(dxJob.id, descFieldReq)
    val crntInstanceType: String = retval.fields.get("instanceType") match {
      case Some(JsString(x)) => x
      case _                 => throw new Exception(s"wrong type for instanceType ${retval}")
    }
    dxApi.logger.appletLog(s"current instance type: ${crntInstanceType}")

    val isSufficient = instanceTypeDB.lteqByResources(requiredInstanceType, crntInstanceType)
    dxApi.logger.appletLog(s"isSufficient? ${isSufficient}")
    isSufficient
  }

  /** The runtime attributes need to be calculated at runtime. Evaluate them,
    *  determine the instance type [xxxx], and relaunch the job on [xxxx]
    */
  def relaunch(inputs: Map[TAT.InputDefinition, WdlValues.V],
               originalInputs: JsValue): Map[String, JsValue] = {
    dxApi.logger.appletLog(s"inputs: ${inputsDbg(inputs)}")

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
      val wvl = WdlVarLinks(outDef.wdlType, DxlExec(dxSubJob, outDef.name))
      wdlVarLinksConverter.genFields(wvl, outDef.name)
    }.toMap
    outputs
  }
}
