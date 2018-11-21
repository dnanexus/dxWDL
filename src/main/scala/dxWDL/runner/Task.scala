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

package dxWDL.runner

import cats.data.Validated.{Invalid, Valid}
import com.dnanexus.{DXAPI, DXJob}
import com.fasterxml.jackson.databind.JsonNode
import common.validation.ErrorOr.ErrorOr
import java.lang.management._
import java.nio.file.{Path, Paths}
import spray.json._
import dxWDL.util._
import wom.callable.CallableTaskDefinition
import wom.callable.Callable.{InputDefinition, OutputDefinition}
import wom.core.WorkflowSource
import wom.expression.{NoIoFunctionSet}
import wom.values._

case class Task(task: CallableTaskDefinition,
                taskSourceCode: WorkflowSource,  // for debugging/informational purposes only
                instanceTypeDB : InstanceTypeDB,
                runtimeDebugLevel: Int) {
    private val verbose = (runtimeDebugLevel >= 1)
    private val maxVerboseLevel = (runtimeDebugLevel == 2)
    private val RUNNER_TASK_ENV_FILE = "taskEnv.json"
    private val DX_HOME = "/home/dnanexus"

    def getErrorOr[A](value: ErrorOr[A]) : A = {
        value match {
            case Valid(x) => x
            case Invalid(s) => throw new AppInternalException(s"Invalid ${s}")
        }
    }


    lazy val metaDirPath : Path = {
        val currentDir = System.getProperty("user.dir")
        val p = Paths.get(currentDir, "meta")
        Utils.safeMkdir(p)
        p
    }

    lazy val execDirPath : Path = {
        val currentDir = System.getProperty("user.dir")
        val p = Paths.get(currentDir, "execution")
        Utils.safeMkdir(p)
        p
    }

    // Running applets download files from the platform to
    // this location
    lazy val inputFilesDirPath : Path = {
        val p = execDirPath.resolve("inputs")
        Utils.safeMkdir(p)
        p
    }

    // Running applets download files from the platform to
    // this location
    lazy val outputFilesDirPath : Path = {
        val p = execDirPath.resolve("outputs")
        Utils.safeMkdir(p)
        p
    }

    // This directory has to reside under the user home directory. If
    // a task runs under docker, the container will need access to
    // temporary files created with stdlib calls like "write_lines".
    lazy val tmpDirPath : Path = {
        val currentDir = System.getProperty("user.dir")
        val p = Paths.get(currentDir, "job_scratch_space")
        Utils.safeMkdir(p)
        p
    }


    // serialize the task inputs to json, and then write to a file.
    def writeEnvToDisk(env: Map[String, WomValue],
                       dxUrl2path: Map[DxURL, Path]) : Unit = {
        val env_m : Map[String, JsValue] = env.map{ case(varName, v) =>
            (varName, WomValueSerialization.toJSON(v))
        }.toMap
        val url_m : Map[String, JsValue] = dxUrl2path.map{ case(dxURL, path) =>
            dxURL.value -> JsString(path.toString)
        }

        // marshal into json, and then to a string
        val json = JsObject("env" -> JsObject(env_m),
                           "dxUrl2path" -> JsObject(url_m))
        Utils.writeFileContent(metaDirPath.resolve(RUNNER_TASK_ENV_FILE),
                               json.prettyPrint)
    }

    def readEnvFromDisk() : (Map[String, WomValue], Map[DxURL, Path]) = {
        val buf = Utils.readFileContent(metaDirPath.resolve(RUNNER_TASK_ENV_FILE))
        val json : JsValue = buf.parseJson
        val (env_m, path_m) = json match {
            case JsObject(m) =>
                (m.get("env"), m.get("dxUrl2path")) match {
                    case (Some(JsObject(env_m)), Some(JsObject(path_m))) => (env_m, path_m)
                    case (_, _) => throw new Exception("Malformed environment serialized to disk")
                }
            case _ => throw new Exception("Malformed environment serialized to disk")
        }
        val env = env_m.map{ case (key, jsVal) =>
            key -> WomValueSerialization.fromJSON(jsVal)
        }.toMap
        val dxUrl2path = path_m.map{
            case (key, JsString(path)) => DxURL(key) -> Paths.get(path)
            case (_, _) => throw new Exception("Sanity")
        }.toMap
        (env, dxUrl2path)
    }

    private def printDirStruct() : Unit = {
        Utils.appletLog(maxVerboseLevel, "Directory structure:")
        val (stdout, stderr) = Utils.execCommand("ls -lR", None)
        Utils.appletLog(maxVerboseLevel, stdout + "\n", 10000)
    }

    // Figure out if a docker image is specified. If so, return it as a string.
    private def dockerImageEval(env: Map[String, WomValue]) : Option[String] = {
        task.runtimeAttributes.attributes.get("docker") match {
            case None => None
            case Some(expr) =>
                val result: ErrorOr[WomValue] =
                    expr.evaluateValue(env, NoIoFunctionSet)
                result match {
                    case Valid(WomString(s)) => Some(s)
                    case _ =>
                        throw new AppInternalException(s"docker is not a string expression ${expr}")
                }
        }
    }

    private def dockerImage(env: Map[String, WomValue]) : Option[String] = {
        val dImg = dockerImageEval(env)
        dImg match {
            case Some(url) if url.startsWith(Utils.DX_URL_PREFIX) =>
                // This is a record on the platform, created with
                // dx-docker. Describe it with an API call, and get
                // the docker image name.
                Utils.appletLog(verbose, s"looking up dx:url ${url}")
                val dxRecord = DxPath.lookupDxURLRecord(url)
                Utils.appletLog(verbose, s"Found record ${dxRecord}")
                val imageName = dxRecord.describe().getName
                Utils.appletLog(verbose, s"Image name is ${imageName}")
                Some(imageName)
            case _ => dImg
        }
    }

    // Write the core bash script into a file. In some cases, we
    // need to run some shell setup statements before and after this
    // script.
    private def writeBashScript(inputEnv: Map[InputDefinition, WomValue]) : Unit = {
        val mbean = ManagementFactory.getOperatingSystemMXBean().asInstanceOf[com.sun.management.OperatingSystemMXBean]
        val physicalMemorySize = mbean.getTotalPhysicalMemorySize()
        val numCores = Runtime.getRuntime().availableProcessors()

        import eu.timepit.refined._
        //import eu.timepit.refined.api.Refined
        import eu.timepit.refined.auto._
        import eu.timepit.refined.numeric._
        val numCoresPositiveInt = refineV[Positive](numCores).right.get

        val runtimeEnvironment = wom.callable.RuntimeEnvironment(
            outputFilesDirPath.toAbsolutePath().toString,
            tmpDirPath.toAbsolutePath().toString,
            numCoresPositiveInt,
            physicalMemorySize,

            // TODO
            1000, //outputPathSize: Long,
            1000) //tempPathSize: Long)

        // instantiate the command
        // TODO: [env] has to be of type:
        //   type WomEvaluatedCallInputs = Map[InputDefinition, WomValue]
        val womInstantiation = getErrorOr(task.instantiateCommand(inputEnv,
                                                                  NoIoFunctionSet,
                                                                  identity,
                                                                  runtimeEnvironment))

        //val command = womInstantiation.head.commandString
        val command = womInstantiation.commandString

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
        val scriptPath = metaDirPath.resolve("script")
        val stdoutPath = metaDirPath.resolve("stdout")
        val stderrPath = metaDirPath.resolve("stderr")
        val rcPath = metaDirPath.resolve("rc")
        val script =
            if (command.isEmpty) {
                s"""|#!/bin/bash
                    |echo 0 > ${rcPath}
                    |""".stripMargin.trim + "\n"
            } else {
                s"""|#!/bin/bash
                    |(
                    |    cd ${DX_HOME}
                    |    ${command}
                    |) \\
                    |  > >( tee ${stdoutPath} ) \\
                    |  2> >( tee ${stderrPath} >&2 )
                    |echo $$? > ${rcPath}
                    |""".stripMargin.trim + "\n"
            }
        Utils.appletLog(verbose, s"writing bash script to ${scriptPath}")
        Utils.writeFileContent(scriptPath, script)
    }

    private def evalEnvironment(inputs: Map[String, WomValue]) : Map[String, WomValue] = {
        // evaluate the declarations
        task.environmentExpressions.foldLeft(inputs){
            case (env, (varName, expr)) =>
                val result: ErrorOr[WomValue] =
                    expr.evaluateValue(env, DxIoFunctions)
                val v = result match {
                    case Valid(value) => value
                    case _ =>
                        throw new AppInternalException(s"Error evaluating expression ${expr}")
                }
                env + (varName -> v)
        }.toMap
    }

    private def writeDockerSubmitBashScript(env: Map[String, WomValue],
                                            imgName: String) : Unit = {
        // The user wants to use a docker container with the
        // image [imgName]. We implement this with dx-docker.
        // There may be corner cases where the image will run
        // into permission limitations due to security.
        //
        // Map the home directory into the container, so that
        // we can reach the result files, and upload them to
        // the platform.
        val dockerCmd = s"""|dx-docker run --entrypoint /bin/bash
                            |-v ${DX_HOME}:${DX_HOME}
                            |${imgName}
                            |$${HOME}/execution/meta/script""".stripMargin.replaceAll("\n", " ")
        val dockerRunPath = metaDirPath.resolve("script.submit")
        val dockerRunScript =
            s"""|#!/bin/bash -ex
                |${dockerCmd}""".stripMargin
        Utils.appletLog(verbose, s"writing docker run script to ${dockerRunPath}")
        Utils.writeFileContent(dockerRunPath, dockerRunScript)
        dockerRunPath.toFile.setExecutable(true)
    }

    // Calculate the input variables for the task, download the input files,
    // and build a shell script to run the command.
    def prolog(inputs: Map[String, WomValue]) : (Map[String, WomValue], Map[DxURL, Path]) = {
        Utils.appletLog(verbose, s"Prolog  debugLevel=${runtimeDebugLevel}")
        Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
        if (maxVerboseLevel)
            printDirStruct()

        Utils.appletLog(verbose, s"Task source code:")
        Utils.appletLog(verbose, taskSourceCode, 10000)

        // Download all input files.
        //
        // Note: this may be overly conservative,
        // because some of the files may not actually be accessed.
        val (localInputs, dxUrl2path) = JobInputOutput.localizeFiles(inputs, inputFilesDirPath)

        // evaluate the declarations
        val env: Map[String, WomValue] = evalEnvironment(inputs)
        val docker = dockerImage(env)

        // We need a value of type
        //    type WomEvaluatedCallInputs = Map[InputDefinition, WomValue]
        // to be able to instantiate the command. It is not clear to me
        // how the environment variables are passed.
        val inputEnv : Map[InputDefinition, WomValue] = task.inputs.map{ inpDef =>
            inputs.get(inpDef.name) match {
                case Some(value) => inpDef -> value
                case None => throw new Exception(s"Sanity: input ${inpDef.localName} is unspecified")
            }
        }.toMap

        // Write shell script to a file. It will be executed by the dx-applet shell code.
        writeBashScript(inputEnv)
        docker match {
            case Some(img) =>
                // write a script that launches the actual command inside a docker image.
                writeDockerSubmitBashScript(env, img)
            case None => ()
        }

        // Record the environment, we need it in the epilog
        (env, dxUrl2path)
    }

    def epilog(env: Map[String, WomValue],
               dxUrl2path: Map[DxURL, Path]) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"Epilog  debugLevel=${runtimeDebugLevel}")
        if (maxVerboseLevel)
            printDirStruct()

        // Evaluate the output declarations. Add outputs evaluated to
        // the environment, so they can be referenced by expressions in the next
        // lines.
        var envFull = env
        val outputsLocal: Map[String, WomValue] = task.outputs.map{
            case (outDef: OutputDefinition) =>
                val result: ErrorOr[WomValue] =
                    outDef.expression.evaluateValue(env, DxIoFunctions)
                result match {
                    case Valid(value) =>
                        envFull += (outDef.name -> value)
                        outDef.name -> value
                    case _ =>
                        throw new AppInternalException(s"Error evaluating output expression ${outDef.expression}")
                }
        }.toMap

        // Upload output files to the platform.
        val outputs : Map[String, WomValue] = JobInputOutput.delocalizeFiles(outputsLocal, dxUrl2path)

        // convert the WDL values to JSON
        val outputFields:Map[String, JsValue] = outputs.map {
            case (outputVarName, womValue) =>
                val wvl = WdlVarLinks.importFromWDL(womValue.womType, DeclAttrs.empty, womValue)
                WdlVarLinks.genFields(wvl, outputVarName)
        }.toList.flatten.toMap
        outputFields
    }


    // Evaluate the runtime expressions, and figure out which instance type
    // this task requires.
    //
    // Do not download the files, if there are any. We may be
    // calculating the instance type in the workflow runner, outside
    // the task.
    def calcInstanceType(taskInputs: Map[String, WomValue]) : String = {
        // evaluate the declarations
        val env: Map[String, WomValue] = evalEnvironment(taskInputs)

        def evalAttr(attrName: String) : Option[WomValue] = {
            task.runtimeAttributes.attributes.get(attrName) match {
                case None => None
                case Some(expr) => Some(getErrorOr(expr.evaluateValue(env, DxIoFunctions)))
            }
        }

        val dxInstaceType = evalAttr(Extras.DX_INSTANCE_TYPE_ATTR)
        val memory = evalAttr("memory")
        val diskSpace = evalAttr("disks")
        val cores = evalAttr("cpu")
        val iTypeRaw = InstanceTypeDB.parse(dxInstaceType, memory, diskSpace, cores)
        val iType = instanceTypeDB.apply(iTypeRaw)
        Utils.appletLog(verbose, s"""|calcInstanceType memory=${memory} disk=${diskSpace}
                                     |cores=${cores} instancetype=${iType}"""
                            .stripMargin.replaceAll("\n", " "))
        iType
    }

    /** Check if we are already on the correct instance type. This allows for avoiding unnecessary
      * relaunch operations.
      */
    def checkInstanceType(inputs: Map[String, WomValue]) : Boolean = {
        // evaluate the runtime attributes
        // determine the instance type
        val requiredInstanceType:String = calcInstanceType(inputs)
        Utils.appletLog(verbose, s"required instance type: ${requiredInstanceType}")

        // Figure out which instance we are on right now
        val dxJob = Utils.dxEnv.getJob()

        val descFieldReq = JsObject("fields" -> JsObject("instanceType" -> JsBoolean(true)))
        val retval: JsValue =
            Utils.jsValueOfJsonNode(
                DXAPI.jobDescribe(dxJob.getId,
                                  Utils.jsonNodeOfJsValue(descFieldReq),
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
    def relaunch(inputs: Map[String, WomValue], originalInputs : JsValue) : Map[String, JsValue] = {
        // evaluate the runtime attributes
        // determine the instance type
        val instanceType:String = calcInstanceType(inputs)

        // Run a sub-job with the "body" entry point, and the required instance type
        val dxSubJob : DXJob = Utils.runSubJob("body", Some(instanceType), originalInputs, Vector.empty)

        // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
        // is exactly the same as the parent, we can immediately exit the parent job.
        val outputs: Map[String, JsValue] = task.outputs.map {
            case (outDef: OutputDefinition) =>
                val wvl = WdlVarLinks(outDef.womType,
                                      DeclAttrs.empty,
                                      DxlExec(dxSubJob, outDef.name))
                WdlVarLinks.genFields(wvl, outDef.name)
        }.flatten.toMap
        outputs
    }
}
