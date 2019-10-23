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

import cats.data.Validated.{Invalid, Valid}
import com.dnanexus.{DXAPI, DXJob}
import com.fasterxml.jackson.databind.JsonNode
import common.validation.ErrorOr.ErrorOr
import java.lang.management._
import java.nio.file.{Path, Paths}
import spray.json._
import wom.callable.{CallableTaskDefinition, RuntimeEnvironment}
import wom.callable.Callable.{InputDefinition, OutputDefinition}
import wom.core.WorkflowSource
import wom.types.WomType
import wom.values._

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
    def readManifestGetDockerImageName(buf: String) : String = {
        val jso = buf.parseJson
        val elem = jso match {
            case JsArray(elements) if elements.size >= 1 => elements.head
            case other => throw new Exception(s"bad value ${other} for manifest, expecting non empty array")
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
                    case other => throw new Exception(s"bad value ${other} in RepoTags manifest field")
                }
            case other =>
                throw new Exception(s"bad value ${other} in RepoTags manifest field")
        }
        repo
    }
}

// We can't use the name Task, because that would confuse it with the
// WDL language definition.
case class TaskRunner(task: CallableTaskDefinition,
                      taskSourceCode: WorkflowSource,  // for debugging/informational purposes only
                      typeAliases: Map[String, WomType],
                      instanceTypeDB : InstanceTypeDB,
                      dxPathConfig : DxPathConfig,
                      dxIoFunctions : DxIoFunctions,
                      jobInputOutput : JobInputOutput,
                      defaultRuntimeAttrs : Option[WdlRuntimeAttrs],
                      runtimeDebugLevel: Int) {
    private val verbose = (runtimeDebugLevel >= 1)
    private val maxVerboseLevel = (runtimeDebugLevel == 2)
    private val utlVerbose = Verbose(runtimeDebugLevel >= 1, false, Set.empty)
    private val wdlVarLinksConverter = WdlVarLinksConverter(utlVerbose, dxIoFunctions.fileInfoDir, typeAliases)
    private val DOCKER_TARBALLS_DIR = "/tmp/docker-tarballs"

    // check if the command section is empty
    val commandSectionEmpty: Boolean = {
        try {
            val commandTemplate = task.commandTemplateString(Map.empty[InputDefinition, WomValue])
            commandTemplate.trim.isEmpty
        } catch {
            case _: Throwable =>
                false
        }
    }

    def getErrorOr[A](value: ErrorOr[A]) : A = {
        value match {
            case Valid(x) => x
            case Invalid(s) => throw new AppInternalException(s"Invalid ${s}")
        }
    }

    // serialize the task inputs to json, and then write to a file.
    def writeEnvToDisk(localizedInputs: Map[String, WomValue],
                       dxUrl2path: Map[Furl, Path]) : Unit = {
        val locInputsM : Map[String, JsValue] = localizedInputs.map{ case(name, v) =>
            (name, WomValueSerialization(typeAliases).toJSON(v))
        }.toMap
        val dxUrlM : Map[String, JsValue] = dxUrl2path.map{
            case (FurlLocal(url), path) =>
                url -> JsString(path.toString)
            case (FurlDx(value, _, _), path) =>
                value -> JsString(path.toString)
        }

        // marshal into json, and then to a string
        val json = JsObject("localizedInputs" -> JsObject(locInputsM),
                            "dxUrl2path" -> JsObject(dxUrlM))
        Utils.writeFileContent(dxPathConfig.runnerTaskEnv, json.prettyPrint)
    }

    def readEnvFromDisk() : (Map[String, WomValue], Map[Furl, Path]) = {
        val buf = Utils.readFileContent(dxPathConfig.runnerTaskEnv)
        val json : JsValue = buf.parseJson
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
        val localizedInputs = locInputsM.map{ case (key, jsVal) =>
            key -> WomValueSerialization(typeAliases).fromJSON(jsVal)
        }.toMap
        val dxUrl2path = dxUrlM.map{
            case (key, JsString(path)) => Furl.parse(key) -> Paths.get(path)
            case (_, _) => throw new Exception("Sanity")
        }.toMap
        (localizedInputs, dxUrl2path)
    }

    private def printDirStruct() : Unit = {
        Utils.appletLog(maxVerboseLevel, "Directory structure:")
        val (stdout, stderr) = Utils.execCommand("ls -lR", None)
        Utils.appletLog(maxVerboseLevel, stdout + "\n", 10000)
    }

    // Figure out if a docker image is specified. If so, return it as a string.
    private def dockerImageEval(env: Map[String, WomValue]) : Option[String] = {
        val dImg : Option[WomValue] = task.runtimeAttributes.attributes.get("docker") match {
            case None =>
                defaultRuntimeAttrs match {
                    case None => None
                    case Some(dra) => dra.m.get("docker")
                }
            case Some(expr) =>
                val result: ErrorOr[WomValue] = expr.evaluateValue(env, dxIoFunctions)
                result match {
                    case Valid(x) => Some(x)
                    case Invalid(_) =>
                        throw new AppInternalException(s"Invalid wom expression ${expr}")
                }
        }
        dImg match {
            case None => None
            case Some(WomString(s)) => Some(s)
            case Some(other) =>
                throw new AppInternalException(s"docker is not a string expression ${other}")
        }
    }

    private def pullImage(dImg: String): Option[String] = {

        var retry_count = 5;
        while (retry_count > 0){
            try {
                val (outstr, errstr) = Utils.execCommand(s"docker pull ${dImg}")

                Utils.appletLog(verbose, s"""|output:
                                             |${outstr}
                                             |stderr:
                                             |${errstr}""".stripMargin)
                return Some(dImg)
            } catch {
                // ideally should catch specific exception.
                case e: Throwable =>
                    retry_count = retry_count - 1
                    Utils.appletLog(verbose,
                        s"""Failed to pull docker image:
                           |${dImg}. Retrying... ${5 - retry_count}
                    """.stripMargin)
                    Thread.sleep(1000)
            }
        }
        throw new RuntimeException(s"Unable to pull docker image: ${dImg} after 5 tries")
    }

    private def dockerImage(env: Map[String, WomValue]) : Option[String] = {
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
                val fileName = dxFile.describe().getName
                val tarballDir = Paths.get(DOCKER_TARBALLS_DIR)
                Utils.safeMkdir(tarballDir)
                val localTar : Path = tarballDir.resolve(fileName)

                Utils.appletLog(verbose, s"downloading docker tarball to ${localTar}")
                DxUtils.downloadFile(localTar, dxFile, verbose)

                Utils.appletLog(verbose, "figuring out the image name")
                val (mContent, _) = Utils.execCommand(s"tar --to-stdout -xf ${localTar} manifest.json")
                Utils.appletLog(verbose, s"""|manifest content:
                                             |${mContent}
                                             |""".stripMargin)
                val repo = TaskRunnerUtils.readManifestGetDockerImageName(mContent)
                Utils.appletLog(verbose, s"repository is ${repo}")

                Utils.appletLog(true, s"load tarball ${localTar} to docker")
                val (outstr,errstr) = Utils.execCommand(s"docker load --input ${localTar}")
                Utils.appletLog(verbose, s"""|output:
                                             |${outstr}
                                             |stderr:
                                             |${errstr}""".stripMargin)
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
    private def availableMemory() : Long = {
        val mbean = ManagementFactory.getOperatingSystemMXBean()
            .asInstanceOf[com.sun.management.OperatingSystemMXBean]
        mbean.getTotalPhysicalMemorySize()
    }

    private def getRuntimeEnvironment() : RuntimeEnvironment = {
        val physicalMemorySize = availableMemory()
        val numCores = Runtime.getRuntime().availableProcessors()

        import eu.timepit.refined._
        import eu.timepit.refined.numeric._
        val numCoresPositiveInt = refineV[Positive](numCores).right.get

        RuntimeEnvironment(
            dxPathConfig.outputFilesDir.toAbsolutePath().toString,
            dxPathConfig.tmpDir.toAbsolutePath().toString,
            numCoresPositiveInt,
            physicalMemorySize,

            // TODO
            1000, //outputPathSize: Long,
            1000) //tempPathSize: Long)
    }

    // Write the core bash script into a file. In some cases, we
    // need to run some shell setup statements before and after this
    // script.
    private def writeBashScript(command: String) : Unit = {
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

    private def writeDockerSubmitBashScript(imgName: String,
                                            dxfuseRunning: Boolean) : Unit = {
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

        Utils.appletLog(verbose, s"writing docker run script to ${dxPathConfig.dockerSubmitScript}")
        Utils.writeFileContent(dxPathConfig.dockerSubmitScript, dockerRunScript)
        dxPathConfig.dockerSubmitScript.toFile.setExecutable(true)
    }

/*
    private def evalEnvironment(localizedInputs: Map[InputDefinition, WomValue]) : Map[String, WomValue] = {
        val inputs = localizedInputs.map{ case (inpDfn, value) =>
            inpDfn.name -> value
        }.toMap

        // evaluate the declarations.
        //
        // It isn't clear that this is actually correct, because I'm not sure what's in the
        // environment.
        val env = task.environmentExpressions.map{
            case (varName, expr) =>
                val result: ErrorOr[WomValue] =
                    expr.evaluateValue(inputs, dxIoFunctions)
                val v = result match {
                    case Valid(value) => value
                    case _ =>
                        throw new AppInternalException(s"Error evaluating expression ${expr}")
                }
                varName -> v
        }.toMap

        inputs ++ env
    }
 */

    private def inputsDbg(inputs: Map[InputDefinition, WomValue]) : String = {
        inputs.map{ case (inp, value) =>
            val i = WomPrettyPrint.apply(inp)
            s"${i} -> ${value}"
        }.mkString("\n")
    }

    // Calculate the input variables for the task, download the input files,
    // and build a shell script to run the command.
    def prolog(taskInputs: Map[InputDefinition, WomValue]) :
            (Map[String, WomValue], Map[Furl, Path]) =
    {
        Utils.appletLog(verbose, s"Prolog  debugLevel=${runtimeDebugLevel}")
        Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
        if (maxVerboseLevel)
            printDirStruct()

        Utils.appletLog(verbose, s"Task source code:")
        Utils.appletLog(verbose, taskSourceCode, 10000)
        Utils.appletLog(verbose, s"inputs: ${inputsDbg(taskInputs)}")

        if (commandSectionEmpty) {
            // The command section is empty, there is no need to setup docker,
            // or download files.
            val inputs = taskInputs.map{ case (inpDfn, value) =>
                inpDfn.name -> value
            }.toMap
            // write an empty bash script
            writeBashScript("")
            return (inputs, Map.empty)
        }

        // Download/stream all input files.
        //
        // Note: this may be overly conservative,
        // because some of the files may not actually be accessed.
        val (localizedInputs, dxUrl2path, dxdaManifest, dxfuseManifest) =
            jobInputOutput.localizeFiles(task.parameterMeta, taskInputs, dxPathConfig.inputFilesDir)

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

        val inputs = localizedInputs.map{ case (inpDfn, value) =>
            inpDfn.name -> value
        }.toMap
        val docker = dockerImage(inputs)

        // instantiate the command
        val runtimeEnvironment = getRuntimeEnvironment()
        val womInstantiation = getErrorOr(task.instantiateCommand(localizedInputs,
                                                                  dxIoFunctions,
                                                                  identity,
                                                                  runtimeEnvironment))
        val command = womInstantiation.commandString

        // Write shell script to a file. It will be executed by the dx-applet shell code.
        writeBashScript(command)
        docker match {
            case Some(img) =>
                // write a script that launches the actual command inside a docker image.
                writeDockerSubmitBashScript(img, manifest2Js != JsNull)
            case None => ()
        }

        // Record the localized inputs, we need them in the epilog
        (inputs, dxUrl2path)
    }

    def epilog(localizedInputValues: Map[String, WomValue],
               dxUrl2path: Map[Furl, Path]) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"Epilog  debugLevel=${runtimeDebugLevel}")
        if (maxVerboseLevel)
            printDirStruct()

        // Evaluate the output declarations. Add outputs evaluated to
        // the environment, so they can be referenced by expressions in the next
        // lines.
        var envFull = localizedInputValues
        val outputsLocal: Map[String, WomValue] = task.outputs.map{
            case (outDef: OutputDefinition) =>
                val result: ErrorOr[WomValue] =
                    outDef.expression.evaluateValue(envFull, dxIoFunctions)
                val valueRaw = result match {
                    case Valid(value) =>
                        value
                    case Invalid(errors) =>
                        Utils.error(errors.toList.mkString)
                        throw new AppInternalException(s"Error evaluating output expression ${outDef.expression}")
                }

                // cast the result value to the correct type
                // For example, an expression like:
                //   Float x = "3.2"
                // requires casting from string to float
                val value = outDef.womType.coerceRawValue(valueRaw).get
                envFull += (outDef.name -> value)
                outDef.name -> value
        }.toMap

        val outputs : Map[String, WomValue] =
            if (commandSectionEmpty) {
                // the command section is empty, so there is no manipulation of local files.
                // we do not upload or download files.
                outputsLocal
            } else {
                // Upload output files to the platform.
                jobInputOutput.delocalizeFiles(outputsLocal, dxUrl2path)
            }

        // convert the WDL values to JSON
        val outputFields:Map[String, JsValue] = outputs.map {
            case (outputVarName, womValue) =>
                val wvl = wdlVarLinksConverter.importFromWDL(womValue.womType, womValue)
                wdlVarLinksConverter.genFields(wvl, outputVarName)
        }.toList.flatten.toMap
        outputFields
    }


    // Evaluate the runtime expressions, and figure out which instance type
    // this task requires.
    //
    // Do not download the files, if there are any. We may be
    // calculating the instance type in the workflow runner, outside
    // the task.
    def calcInstanceType(taskInputs: Map[InputDefinition, WomValue]) : String = {
        // evaluate the declarations
        val inputs = taskInputs.map{ case (inpDfn, value) =>
            inpDfn.name -> value
        }.toMap

        def evalAttr(attrName: String) : Option[WomValue] = {
            task.runtimeAttributes.attributes.get(attrName) match {
                case None =>
                    // try the defaults
                    defaultRuntimeAttrs match {
                        case None => None
                        case Some(dra) => dra.m.get(attrName)
                    }
                case Some(expr) =>
                    Some(getErrorOr(expr.evaluateValue(inputs, dxIoFunctions)))
            }
        }

        val dxInstanceType = evalAttr("dx_instance_type")
        val memory = evalAttr("memory")
        val diskSpace = evalAttr("disks")
        val cores = evalAttr("cpu")
        val gpu = evalAttr("gpu")
        val iTypeRaw = InstanceTypeDB.parse(dxInstanceType, memory, diskSpace, cores, gpu)
        val iType = instanceTypeDB.apply(iTypeRaw)
        Utils.appletLog(verbose, s"""|calcInstanceType memory=${memory} disk=${diskSpace}
                                     |cores=${cores} instancetype=${iType}"""
                            .stripMargin.replaceAll("\n", " "))
        iType
    }

    /** Check if we are already on the correct instance type. This allows for avoiding unnecessary
      * relaunch operations.
      */
    def checkInstanceType(inputs: Map[InputDefinition, WomValue]) : Boolean = {
        // evaluate the runtime attributes
        // determine the instance type
        Utils.appletLog(verbose, s"inputs: ${inputsDbg(inputs)}")

        val requiredInstanceType:String = calcInstanceType(inputs)
        Utils.appletLog(verbose, s"required instance type: ${requiredInstanceType}")

        // Figure out which instance we are on right now
        val dxJob = DxUtils.dxEnv.getJob()

        val descFieldReq = JsObject("fields" -> JsObject("instanceType" -> JsBoolean(true)))
        val retval: JsValue =
            DxUtils.jsValueOfJsonNode(
                DXAPI.jobDescribe(dxJob.getId,
                                  DxUtils.jsonNodeOfJsValue(descFieldReq),
                                  classOf[JsonNode]))
        val crntInstanceType:String = retval.asJsObject.fields.get("instanceType") match {
            case Some(JsString(x)) => x
            case _ => throw new Exception(s"wrong type for instanceType ${retval}")
        }
        Utils.appletLog(verbose, s"current instance type: ${crntInstanceType}")

        val isSufficient = instanceTypeDB.lteqByResources(requiredInstanceType, crntInstanceType)
        Utils.appletLog(verbose, s"isSufficient? ${isSufficient}")
        isSufficient
    }

    /** The runtime attributes need to be calculated at runtime. Evaluate them,
      *  determine the instance type [xxxx], and relaunch the job on [xxxx]
      */
    def relaunch(inputs: Map[InputDefinition, WomValue], originalInputs : JsValue) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"inputs: ${inputsDbg(inputs)}")

        // evaluate the runtime attributes
        // determine the instance type
        val instanceType:String = calcInstanceType(inputs)

        // Run a sub-job with the "body" entry point, and the required instance type
        val dxSubJob : DXJob = DxUtils.runSubJob("body", Some(instanceType), originalInputs,
                                                 Vector.empty, maxVerboseLevel)

        // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
        // is exactly the same as the parent, we can immediately exit the parent job.
        val outputs: Map[String, JsValue] = task.outputs.map {
            case (outDef: OutputDefinition) =>
                val wvl = WdlVarLinks(outDef.womType,
                                      DxlExec(dxSubJob, outDef.name))
                wdlVarLinksConverter.genFields(wvl, outDef.name)
        }.flatten.toMap
        outputs
    }
}
