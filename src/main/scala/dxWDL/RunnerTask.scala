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

package dxWDL

import com.dnanexus.{DXAPI, DXApplet, DXEnvironment, DXFile, DXJob, DXJSON, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.io.PrintStream
import java.nio.file.{Path, Paths, Files}
import scala.collection.mutable.HashMap
import scala.collection.JavaConverters._
import spray.json._
import spray.json.DefaultJsonProtocol
import wdl4s.AstTools._
import wdl4s.expression.{WdlStandardLibraryFunctionsType, WdlStandardLibraryFunctions}
import wdl4s.types._
import wdl4s.values._
import wdl4s.{Call, Declaration, WdlNamespaceWithWorkflow, Task, TaskOutput, WdlExpression, WdlNamespace, Workflow}
import wdl4s.WdlExpression.AstForExpressions

object RunnerTask {
    // Stream where to emit debugging information. By default,
    // goes to stderr on the instance. Requires reconfiguration
    // in unit test environments
    var errStream: PrintStream = System.err
    def setErrStream(err: PrintStream) = {
        errStream = err
    }

    def getMetaDir() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.safeMkdir(metaDir)
        metaDir
    }

    def evalDeclarations(task : Task,
                         taskInputs : Map[String, WdlValue]) : Map[Declaration, WdlValue] = {
        var env = taskInputs
        def lookup(varName : String) : WdlValue =
            env.get(varName) match {
                case Some(x) => x
                case None => throw new UnboundVariableException(varName)
            }

        def evalDecl(decl : Declaration) : Option[WdlValue] = {
            (decl.wdlType, decl.expression) match {
                // optional input
                case (WdlOptionalType(_), None) =>
                    taskInputs.get(decl.unqualifiedName) match {
                        case None => None
                        case Some(x) => Some(x)
                    }

                    // compulsory input.
                    // Here, we fail if an input has not been provided. This
                    // is not the Cromwell behavior, which allows unbound variables,
                    // as long as they are not accessed.
                case (_, None) =>
                    taskInputs.get(decl.unqualifiedName) match {
                        case None => throw new UnboundVariableException(decl.unqualifiedName)
                        case Some(x) => Some(x)
                    }

                // declaration to evaluate, not an input
                case (_, Some(expr)) =>
                    val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
                    Some(v)
            }
        }

        // evaluate the declarations, and discard any optionals that did not have an
        // input
        task.declarations.map{ decl =>
            evalDecl(decl) match {
                case None => None
                case Some(v) => Some(decl, v)
            }
        }.flatten.toMap
    }

    // evaluate Task output expressions
    def evalTaskOutputs(task : Task,
                        inputs : Map[String, WdlValue]) : Seq[(String, WdlType, WdlValue)] = {
        def lookup(varName : String) : WdlValue = {
            inputs.get(varName) match {
                case Some(x) => x
                case None => throw new AppInternalException(s"No value found for variable ${varName}")
            }
        }
        def evalTaskOutput(tso: TaskOutput, expr: WdlExpression) : (String, WdlType, WdlValue) = {
            val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
            (tso.unqualifiedName, tso.wdlType, v)
        }
        task.outputs.map { case outdef =>
            outdef.expression match {
                case Some(expr) if Utils.isOptional(outdef.wdlType) =>
                    // An optional output, it could be missing
                    try {
                        Some(evalTaskOutput(outdef, expr))
                    } catch {
                        case e: Throwable => None
                    }
                case Some(expr) =>
                    // Compulsory output
                    Some(evalTaskOutput(outdef, expr))
                case None =>
                    throw new AppInternalException("Output has no evaluating expression")
            }
        }.flatten
    }

    // serialize the task inputs to json, and then write to a file.
    def writeEnvToDisk(env : Map[String, WdlValue]) : Unit = {
        val m : Map[String, JsValue] = env.map{ case(varName, wdlValue) =>
            (varName, JsString(Utils.marshal(wdlValue)))
        }.toMap
        val buf = (JsObject(m)).prettyPrint
        val inputVarsPath = getMetaDir().resolve("inputVars.json")
        Utils.writeFileContent(inputVarsPath, buf)
    }

    def readTaskDeclarationsFromDisk() : Map[String, WdlValue] = {
        val inputVarsPath = getMetaDir().resolve("inputVars.json")
        val buf = Utils.readFileContent(inputVarsPath)
        val json : JsValue = buf.parseJson
        val m = json match {
            case JsObject(m) => m
            case _ => throw new Exception("Serialized task inputs not a json object")
        }
        m.map { case (key, jsVal) =>
            val wdlValue = jsVal match {
                case JsString(s) => Utils.unmarshal(s)
                case _ => throw new Exception("Serialized task inputs not a json object")
            }
            key -> wdlValue
        }.toMap
    }

    // Upload output files as a consequence
    def writeJobOutputs(jobOutputPath : Path,
                        outputs : Seq[(String, WdlType, WdlValue)]) : Unit = {
        // convert the WDL values to JSON
        val jsOutputs : Seq[(String, JsValue)] = outputs.map {
            case (key,wdlType,wdlValue) =>
                val wvl = WdlVarLinks.apply(wdlType, wdlValue)
                val l = WdlVarLinks.genFields(wvl, key)
                l.map{ case (x,y) => (x, Utils.jsValueOfJsonNode(y)) }
        }.flatten
        val json = JsObject(jsOutputs.toMap)
        val ast_pp = json.prettyPrint
        errStream.println(s"writeJobOutputs ${ast_pp}")
        // write to the job outputs
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }

    def writeSubmitBashScript(task: Task,
                              env: Map[String, WdlValue]) : Unit = {
        def lookup(varName : String) : WdlValue = {
            env.get(varName) match {
                case Some(x) => x
                case None => throw new AppInternalException(s"No value found for variable ${varName}")
            }
        }
        def evalStringExpr(expr: WdlExpression) : String = {
            val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
            v match {
                case WdlString(s) => s
                case _ => throw new AppInternalException(
                    s"docker is not a string expression ${v.toWdlString}")
            }
        }
        // Figure out if docker is used. If so, it is specifed by an
        // expression that requires evaluation.
        val docker: Option[String] =
            task.runtimeAttributes.attrs.get("docker") match {
                case None => None
                case Some(expr) => Some(evalStringExpr(expr))
            }
        docker match {
            case None => ()
            case Some(imgName) =>
                // The user wants to use a docker container with the
                // image [imgName]. We implement this with dx-docker.
                // There may be corner cases where the image will run
                // into permission limitations due to security.
                //
                // Map the home directory into the container, so that
                // we can reach the result files, and upload them to
                // the platform.
                val DX_HOME = Utils.DX_HOME
                val dockerRunPath = getMetaDir().resolve("script.submit")
                val dockerRunScript =
                    s"""|#!/bin/bash -ex
                        |dx-docker run --entrypoint /bin/bash -v ${DX_HOME}:${DX_HOME} ${imgName} $${HOME}/execution/meta/script""".stripMargin.trim
                errStream.println(s"writing docker run script to ${dockerRunPath}")
                Utils.writeFileContent(dockerRunPath, dockerRunScript)
                dockerRunPath.toFile.setExecutable(true)
        }
    }

    def writeBashScript(task: Task,
                        inputs: Map[Declaration, WdlValue]) {
        val metaDir = getMetaDir()
        val scriptPath = metaDir.resolve("script")
        val stdoutPath = metaDir.resolve("stdout")
        val stderrPath = metaDir.resolve("stderr")
        val rcPath = metaDir.resolve("rc")

        // instantiate the command
        val shellCmd : String = task.instantiateCommand(inputs, DxFunctions).get

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
            if (shellCmd.isEmpty) {
                s"""|#!/bin/bash
                    |echo 0 > ${rcPath}
                    |""".stripMargin.trim + "\n"
            } else {
                s"""|#!/bin/bash
                    |(
                    |if [ -d ${Utils.DX_HOME} ]; then
                    |  cd ${Utils.DX_HOME}
                    |fi
                    |${shellCmd}
                    |) \\
                    |  > >( tee ${stdoutPath} ) \\
                    |  2> >( tee ${stderrPath} >&2 )
                    |echo $$? > ${rcPath}
                    |""".stripMargin.trim + "\n"
            }
        errStream.println(s"writing bash script to ${scriptPath}")
        Utils.writeFileContent(scriptPath, script)
    }

    def prologCore(task: Task, inputs: Map[String, WdlValue]) : Unit = {
        val topDecls = evalDeclarations(task, inputs)

        // Write shell script to a file. It will be executed by the dx-applet code
        writeBashScript(task, topDecls)

        // write the script that launches the shell script. It could be a docker
        // image.
        writeSubmitBashScript(task, inputs)

        // serialize the environment, so we don't have to calculate it again in
        // the epilog
        val env = topDecls.map{ case (decl, wdlValue) => decl.unqualifiedName -> wdlValue}.toMap
        writeEnvToDisk(env)
    }

    // Calculate the input variables for the task, download the input files,
    // and build a shell script to run the command.
    def prolog(task: Task,
               jobInputPath : Path,
               jobOutputPath : Path,
               jobInfoPath: Path) : Unit = {
        // Extract types for the inputs
        val (inputTypes,_) = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        errStream.println(s"WdlType mapping =${inputTypes}")

        // Read the job input file
        val inputLines : String = Utils.readFileContent(jobInputPath)
        var inputWvls = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputTypes)

        // convert to WDL values.
        //
        // Download any files now, because we do not know what will be
        // accessed in the shell command. It will not be possible to
        // download on-demand when the shell command runs.
        val inputs = inputWvls.map{ case (key, wvl) =>
            key -> WdlVarLinks.eval(wvl, true)
        }.toMap
        prologCore(task, inputs)
    }

    def epilogCore(task: Task) : Seq[(String, WdlType, WdlValue)]  = {
        val taskInputs : Map[String, WdlValue] = readTaskDeclarationsFromDisk()
        // evaluate outputs
        evalTaskOutputs(task, taskInputs)
    }

    def epilog(task: Task,
               jobInputPath : Path,
               jobOutputPath : Path,
               jobInfoPath: Path) : Unit = {
        val outputs : Seq[(String, WdlType, WdlValue)] = epilogCore(task)
        writeJobOutputs(jobOutputPath, outputs)
    }



    // Evaluate the runtime expressions, and figure out which instance type
    // this task requires.
    def calcInstanceType(task: Task,
                         taskInputs: Map[String, WdlVarLinks],
                         instanceTypeDB: InstanceTypeDB) : String = {
        // input variables that were already calculated
        val env = HashMap.empty[String, WdlValue]
        def lookup(varName : String) : WdlValue = {
            env.get(varName) match {
                case Some(x) => x
                case None =>
                    // value not evaluated yet, calculate and keep in cache
                    taskInputs.get(varName) match {
                        case Some(wvl) =>
                            env(varName) = WdlVarLinks.eval(wvl, true)
                            env(varName)
                        case None => throw new UnboundVariableException(varName)
                    }
            }
        }
        def evalAttr(attrName: String) : Option[WdlValue] = {
            task.runtimeAttributes.attrs.get(attrName) match {
                case None => None
                case Some(expr) =>
                    Some(expr.evaluate(lookup, DxFunctions).get)
            }
        }

        val dxInstaceType = evalAttr(Utils.DX_INSTANCE_TYPE_ATTR)
        val memory = evalAttr("memory")
        val diskSpace = evalAttr("disks")
        val cores = evalAttr("cpu")
        val iType = instanceTypeDB.apply(dxInstaceType, memory, diskSpace, cores)
        errStream.println(s"""|calcInstanceType memory=${memory} disk=${diskSpace}
                              |cores=${cores} instancetype=${iType}"""
                              .stripMargin.replaceAll("\n", " "))
        iType
    }

    def relaunchBuildInputs(inputWvls: Map[String, WdlVarLinks]) : ObjectNode = {
        var builder : DXJSON.ObjectBuilder = DXJSON.getObjectBuilder()
        inputWvls.foreach{ case (varName, wvl) =>
            WdlVarLinks.genFields(wvl, varName).foreach{ case (fieldName, jsNode) =>
                builder = builder.put(fieldName, jsNode)
            }
        }
        builder.build()
    }

    def runSubJob(entryPoint:String, instanceType:String, inputs:ObjectNode) : DXJob = {
        val req: ObjectNode = DXJSON.getObjectBuilder()
            .put("function", entryPoint)
            .put("input", inputs)
            .put("systemRequirements",
                 DXJSON.getObjectBuilder().put(entryPoint,
                                               DXJSON.getObjectBuilder()
                                                   .put("instanceType", instanceType)
                                                   .build())
                     .build())
            .build()
        val retval: JsonNode = DXAPI.jobNew(req, classOf[JsonNode])
        val info: JsValue =  Utils.jsValueOfJsonNode(retval)
        val id:String = info.asJsObject.fields.get("id") match {
            case Some(JsString(x)) => x
            case _ => throw new AppInternalException(
                s"Bad format returned from jobNew ${info.prettyPrint}")
        }
        DXJob.getInstance(id)
    }

    /** The runtime attributes need to be calculated at runtime. Evaluate them,
      *  determine the instance type [xxxx], and relaunch the job on [xxxx]
      */
    def relaunch(task: Task,
                 jobInputPath : Path,
                 jobOutputPath : Path,
                 jobInfoPath: Path) : Unit = {
        // Extract types for the inputs
        val (inputTypes,_) = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        errStream.println(s"WdlType mapping =${inputTypes}")

        val dxEnv: DXEnvironment = DXEnvironment.create()
        val dxJob = dxEnv.getJob()
        val dxProject = dxEnv.getProjectContext()

        // Read the job input file, and load the inputs without downloading
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val inputWvls = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputTypes)

        // Figure out the available instance types, and their prices,
        // by reading the file
        val dbRaw = Utils.readFileContent(Paths.get("/" + Utils.INSTANCE_TYPE_DB_FILENAME))
        val instanceTypeDB = dbRaw.parseJson.convertTo[InstanceTypeDB]

        // evaluate the runtime attributes
        // determine the instance type
        val instanceType:String = calcInstanceType(task, inputWvls, instanceTypeDB)

        // relaunch the applet on the correct instance type
        val inputs = relaunchBuildInputs(inputWvls)

        // Run a sub-job with the "body" entry point, and the required instance type
        val dxSubJob : DXJob = runSubJob("body", instanceType, inputs)

        // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
        // is exactly the same as the parent, we can immediately exit the parent job.
        val outputs: Map[String, JsonNode] = task.outputs.map { tso =>
            val wvl = WdlVarLinks(tso.wdlType, DxlJob(dxSubJob, IORef.Output, tso.unqualifiedName))
            WdlVarLinks.genFields(wvl, tso.unqualifiedName)
        }.flatten.toMap


        // write the outputs to the job_output.json file
        val json = JsObject(
            outputs.map{ case (key, json) => key -> Utils.jsValueOfJsonNode(json) }.toMap
        )
        val ast_pp = json.prettyPrint
        errStream.println(s"outputs = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
