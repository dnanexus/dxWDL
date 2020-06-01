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
package dxWDL.exec

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import java.lang.management._
import java.nio.file.{Path, Paths}
import spray.json._
import wdlTools.eval.{Context => EvalContext, Eval => WdlExprEval, EvalConfig, WdlValues}
import wdlTools.types.{TypedAbstractSyntax => TAT, TypeOptions, WdlTypes}

import dxWDL.base._
import dxWDL.dx._
import dxWDL.util._

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
      case JsArray(elements) if elements.size >= 1 => elements.head
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
                      dxIoFunctions: DxIoFunctions,
                      jobInputOutput: JobInputOutput,
                      defaultRuntimeAttrs: Option[WdlRuntimeAttrs],
                      delayWorkspaceDestruction: Option[Boolean],
                      runtimeDebugLevel: Int) {
  private val verbose = (runtimeDebugLevel >= 1)
  private val maxVerboseLevel = (runtimeDebugLevel == 2)
  private val utlVerbose = Verbose(runtimeDebugLevel >= 1, false, Set.empty)
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(utlVerbose, dxIoFunctions.fileInfoDir, typeAliases)
  private val DOCKER_TARBALLS_DIR = "/tmp/docker-tarballs"

  val evaluator: WdlExprEval = {
    val evalOpts = TypeOptions(typeChecking = wdlTools.util.TypeCheckingRegime.Strict,
                               antlr4Trace = false,
                               localDirectories = Vector.empty,
                               verbosity = wdlTools.util.Verbosity.Quiet)
    val evalCfg = EvalConfig(dxIoFunctions.config.homeDir,
                             dxIoFunctions.config.tmpDir,
                             dxIoFunctions.config.stdout,
                             dxIoFunctions.config.stderr)
    new WdlExprEval(evalOpts, evalCfg, document.version.value, None)
  }

  // serialize the task inputs to json, and then write to a file.
  def writeEnvToDisk(localizedInputs: Map[String, (WdlTypes.T, WdlValues.V)],
                     dxUrl2path: Map[Furl, Path]): Unit = {
    val locInputsM: Map[String, JsValue] = localizedInputs.map {
      case (name, (t, v)) =>
        (name, WomValueSerialization(typeAliases).toJSON(t, v))
    }.toMap
    val dxUrlM: Map[String, JsValue] = dxUrl2path.map {
      case (FurlLocal(url), path) =>
        url -> JsString(path.toString)
      case (FurlDx(value, _, _), path) =>
        value -> JsString(path.toString)
    }

    // marshal into json, and then to a string
    val json = JsObject("localizedInputs" -> JsObject(locInputsM), "dxUrl2path" -> JsObject(dxUrlM))
    Utils.writeFileContent(dxPathConfig.runnerTaskEnv, json.prettyPrint)
  }

  def readEnvFromDisk(): (Map[String, WdlValues.V], Map[Furl, Path]) = {
    val buf = Utils.readFileContent(dxPathConfig.runnerTaskEnv)
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
      case (key, jsVal) =>
        key -> WomValueSerialization(typeAliases).fromJSON(jsVal)
    }.toMap
    val dxUrl2path = dxUrlM.map {
      case (key, JsString(path)) => Furl.parse(key) -> Paths.get(path)
      case (_, _)                => throw new Exception("Sanity")
    }.toMap
    (localizedInputs, dxUrl2path)
  }

  private def printDirStruct(): Unit = {
    Utils.appletLog(maxVerboseLevel, "Directory structure:")
    val (stdout, stderr) = Utils.execCommand("ls -lR", None)
    Utils.appletLog(maxVerboseLevel, stdout + "\n", 10000)
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

    var retry_count = 5;
    while (retry_count > 0) {
      try {
        val (outstr, errstr) = Utils.execCommand(s"docker pull ${dImg}")

        Utils.appletLog(
            verbose,
            s"""|output:
                |${outstr}
                |stderr:
                |${errstr}""".stripMargin
        )
        return Some(dImg)
      } catch {
        // ideally should catch specific exception.
        case e: Throwable =>
          retry_count = retry_count - 1
          Utils.appletLog(
              verbose,
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
      case Some(url) if url.startsWith(Utils.DX_URL_PREFIX) =>
        // a tarball created with "docker save".
        // 1. download it
        // 2. open the tar archive
        // 2. load into the local docker cache
        // 3. figure out the image name
        Utils.appletLog(verbose, s"looking up dx:url ${url}")
        val dxFile = DxPath.resolveDxURLFile(url)
        val fileName = dxFile.describe().name
        val tarballDir = Paths.get(DOCKER_TARBALLS_DIR)
        Utils.safeMkdir(tarballDir)
        val localTar: Path = tarballDir.resolve(fileName)

        Utils.appletLog(verbose, s"downloading docker tarball to ${localTar}")
        DxUtils.downloadFile(localTar, dxFile, verbose)

        Utils.appletLog(verbose, "figuring out the image name")
        val (mContent, _) = Utils.execCommand(s"tar --to-stdout -xf ${localTar} manifest.json")
        Utils.appletLog(
            verbose,
            s"""|manifest content:
                |${mContent}
                |""".stripMargin
        )
        val repo = TaskRunnerUtils.readManifestGetDockerImageName(mContent)
        Utils.appletLog(verbose, s"repository is ${repo}")

        Utils.appletLog(true, s"load tarball ${localTar} to docker")
        val (outstr, errstr) = Utils.execCommand(s"docker load --input ${localTar}")
        Utils.appletLog(
            verbose,
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
    val mbean = ManagementFactory
      .getOperatingSystemMXBean()
      .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    mbean.getTotalPhysicalMemorySize()
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
    Utils.appletLog(verbose, s"writing bash script to ${dxPathConfig.script}")
    Utils.writeFileContent(dxPathConfig.script, script)
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

    val totalAvailableMemoryBytes = availableMemory()
    val memCap =
      if (dxfuseRunning)
        totalAvailableMemoryBytes - Utils.DXFUSE_MAX_MEMORY_CONSUMPTION
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

    Utils.appletLog(verbose, s"writing docker run script to ${dxPathConfig.dockerSubmitScript}")
    Utils.writeFileContent(dxPathConfig.dockerSubmitScript, dockerRunScript)
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

  // Calculate the input variables for the task, download the input files,
  // and build a shell script to run the command.
  def prolog(
      taskInputs: Map[TAT.InputDefinition, WdlValues.V]
  ): (Map[String, (WdlTypes.T, WdlValues.V)], Map[Furl, Path]) = {
    Utils.appletLog(verbose, s"Prolog  debugLevel=${runtimeDebugLevel}")
    Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
    if (maxVerboseLevel)
      printDirStruct()

    Utils.appletLog(verbose, s"Task source code:")
    Utils.appletLog(verbose, document.sourceCode, 10000)
    Utils.appletLog(verbose, s"inputs: ${inputsDbg(taskInputs)}")

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
    if (manifestJs.asJsObject.fields.size > 0) {
      Utils.writeFileContent(dxPathConfig.dxdaManifest, manifestJs.prettyPrint)
    }

    // build a manifest for dxfuse
    val DxfuseManifest(manifest2Js) = dxfuseManifest
    if (manifest2Js != JsNull) {
      Utils.writeFileContent(dxPathConfig.dxfuseManifest, manifest2Js.prettyPrint)
    }

    val inputsWithTypes: Map[String, (WdlTypes.T, WdlValues.V)] =
      localizedInputs.map {
        case (inpDfn, value) =>
          inpDfn.name -> (inpDfn.wdlType, value)
      }.toMap
    val inputs: Map[String, WdlValues.V] =
      inputsWithTypes.map {
        case (k, (t, v)) => k -> v
      }

    val docker = dockerImage(inputs)

    // instantiate the command
    val command = evaluator.applyCommand(task.command, EvalContext(inputs))

    // Write shell script to a file. It will be executed by the dx-applet shell code.
    writeBashScript(command)
    docker match {
      case Some(img) =>
        // write a script that launches the actual command inside a docker image.
        writeDockerSubmitBashScript(img, manifest2Js != JsNull)
      case None => ()
    }

    // Record the localized inputs, we need them in the epilog
    (inputsWithTypes, dxUrl2path)
  }

  def epilog(localizedInputs: Map[String, WdlValues.V],
             dxUrl2path: Map[Furl, Path]): Map[String, JsValue] = {
    Utils.appletLog(verbose, s"Epilog  debugLevel=${runtimeDebugLevel}")
    if (maxVerboseLevel)
      printDirStruct()

    // Evaluate the output declarations. Add outputs evaluated to
    // the environment, so they can be referenced by expressions in the next
    // lines.
    val outputsLocal: Map[String, (WdlTypes.T, WdlValues.V)] =
      task.outputs
        .foldLeft(Map.empty[String, (WdlTypes.T, WdlValues.V)]) {
          case (env, outDef: TAT.OutputDefinition) =>
            val envNoTypes = env.map { case (k, (t, v)) => k -> v }
            val value = evaluator.applyExprAndCoerce(outDef.expr,
                                                     outDef.wdlType,
                                                     EvalContext(envNoTypes ++ localizedInputs))
            env + (outDef.name -> (outDef.wdlType, value))
        }
        .toMap

    val outputs: Map[String, (WdlTypes.T, WdlValues.V)] =
      // Upload output files to the platform.
      jobInputOutput.delocalizeFiles(outputsLocal, dxUrl2path)

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = outputs
      .map {
        case (outputVarName, (wdlType, womValue)) =>
          val wvl = wdlVarLinksConverter.importFromWDL(wdlType, womValue)
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
    // evaluate the declarations
    val inputs = taskInputs.map {
      case (inpDfn, value) =>
        inpDfn.name -> value
    }.toMap

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
          Some(evaluator.applyExprAndCoerce(expr, WdlTypes.T_String, EvalContext(inputs)))
      }
    }

    val dxInstanceType = evalAttr("dx_instance_type")
    val memory = evalAttr("memory")
    val diskSpace = evalAttr("disks")
    val cores = evalAttr("cpu")
    val gpu = evalAttr("gpu")
    val iTypeRaw = InstanceTypeDB.parse(dxInstanceType, memory, diskSpace, cores, gpu)
    val iType = instanceTypeDB.apply(iTypeRaw)
    Utils.appletLog(
        verbose,
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
    Utils.appletLog(verbose, s"inputs: ${inputsDbg(inputs)}")

    val requiredInstanceType: String = calcInstanceType(inputs)
    Utils.appletLog(verbose, s"required instance type: ${requiredInstanceType}")

    // Figure out which instance we are on right now
    val dxJob = DxJob(DxUtils.dxEnv.getJob())

    val descFieldReq = JsObject("fields" -> JsObject("instanceType" -> JsBoolean(true)))
    val retval: JsValue =
      DxUtils.jsValueOfJsonNode(
          DXAPI.jobDescribe(dxJob.id, DxUtils.jsonNodeOfJsValue(descFieldReq), classOf[JsonNode])
      )
    val crntInstanceType: String = retval.asJsObject.fields.get("instanceType") match {
      case Some(JsString(x)) => x
      case _                 => throw new Exception(s"wrong type for instanceType ${retval}")
    }
    Utils.appletLog(verbose, s"current instance type: ${crntInstanceType}")

    val isSufficient = instanceTypeDB.lteqByResources(requiredInstanceType, crntInstanceType)
    Utils.appletLog(verbose, s"isSufficient? ${isSufficient}")
    isSufficient
  }

  /** The runtime attributes need to be calculated at runtime. Evaluate them,
    *  determine the instance type [xxxx], and relaunch the job on [xxxx]
    */
  def relaunch(inputs: Map[TAT.InputDefinition, WdlValues.V],
               originalInputs: JsValue): Map[String, JsValue] = {
    Utils.appletLog(verbose, s"inputs: ${inputsDbg(inputs)}")

    // evaluate the runtime attributes
    // determine the instance type
    val instanceType: String = calcInstanceType(inputs)

    // Run a sub-job with the "body" entry point, and the required instance type
    val dxSubJob: DxJob =
      DxUtils.runSubJob("body",
                        Some(instanceType),
                        originalInputs,
                        Vector.empty,
                        delayWorkspaceDestruction,
                        maxVerboseLevel)

    // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
    // is exactly the same as the parent, we can immediately exit the parent job.
    val outputs: Map[String, JsValue] = task.outputs
      .map {
        case (outDef: TAT.OutputDefinition) =>
          val wvl = WdlVarLinks(outDef.wdlType, DxlExec(dxSubJob, outDef.name))
          wdlVarLinksConverter.genFields(wvl, outDef.name)
      }
      .flatten
      .toMap
    outputs
  }
}
