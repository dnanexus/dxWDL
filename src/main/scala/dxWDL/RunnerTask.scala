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
import wdl4s.{Call, Declaration,
    Task, TaskOutput, WdlExpression,
    WdlNamespace, WdlNamespaceWithWorkflow, Workflow}
import wdl4s.WdlExpression.AstForExpressions

case class RunnerTask(task:Task,
                      cef: CompilerErrorFormatter) {
    def getMetaDir() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.safeMkdir(metaDir)
        metaDir
    }

    // TODO: merge this method with the corresponding method in RunnerEval.
    // The differences are:
    // 1) use of the task to get declaration attributes
    // 2) Dealing with file streams
    private def evalDeclarations(declarations: Seq[Declaration],
                                 inputs : Map[String, WdlVarLinks]) : Map[String, BValue] = {
        // Environment that includes a cache for values that have
        // already been evaluated.  It is more efficient to make the
        // conversion once, however, that is not the main point
        // here. There are types that require special care, for
        // example files. We need to make sure we download files
        // exactly once, and later, we want to be able to delete them.
        var env: Map[String, (WdlVarLinks, Option[WdlValue])] =
            inputs.map{ case (key, wvl) => key -> (wvl, None) }.toMap

        def evalAndCache(key: String, wvl: WdlVarLinks) : WdlValue = {
            val v: WdlValue =
                if (wvl.attrs.stream) {
                    WdlVarLinks.eval(wvl, false)
                } else {
                    WdlVarLinks.eval(wvl, true)
                }
            env = env + (key -> (wvl, Some(v)))
            v
        }

        def lookup(varName : String) : WdlValue =
            env.get(varName) match {
                case Some((wvl, None)) =>
                    // Make a value from a dx-links structure. This also causes any file
                    // to be downloaded. Keep the result cached.
                    evalAndCache(varName, wvl)
                case Some((_, Some(v))) =>
                    // We have already evalulated this structure
                    v
                case None =>
                    throw new AppInternalException(s"Accessing unbound variable ${varName}")
            }

        def evalDecl(decl : Declaration) : Option[(WdlVarLinks, WdlValue)] = {
            val attrs = DeclAttrs.get(task, decl.unqualifiedName, cef)
            (decl.wdlType, decl.expression) match {
                // optional input
                case (WdlOptionalType(_), None) =>
                    inputs.get(decl.unqualifiedName) match {
                        case None => None
                        case Some(wvl) =>
                            val v: WdlValue = evalAndCache(decl.unqualifiedName, wvl)
                            Some((wvl, v))
                    }

                // compulsory input
                case (_, None) =>
                    inputs.get(decl.unqualifiedName) match {
                        case None =>
                            throw new AppInternalException(s"Accessing unbound variable ${decl.unqualifiedName}")
                        case Some(wvl) =>
                            val v: WdlValue = evalAndCache(decl.unqualifiedName, wvl)
                            Some((wvl, v))
                    }

                // declaration to evaluate, not an input
                case (WdlOptionalType(t), Some(expr)) =>
                    try {
                        val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
                        val wvl = WdlVarLinks.apply(t, attrs, v)
                        env = env + (decl.unqualifiedName -> (wvl, Some(v)))
                        Some((wvl, v))
                    } catch {
                        // trying to access an unbound variable.
                        // Since the result is optional.
                        case e: AppInternalException => None
                    }

                case (t, Some(expr)) =>
                    val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
                    val wvl = WdlVarLinks.apply(t, attrs, v)
                    env = env + (decl.unqualifiedName -> (wvl, Some(v)))
                    Some((wvl, v))
            }
        }

        // Process all the declarations. Take care to add new bindings
        // to the environment, so variables like [cmdline] will be able to
        // access previous results.
        declarations.map{ decl =>
            evalDecl(decl) match {
                case Some((wvl, wdlValue)) =>
                    Some(decl.unqualifiedName -> BValue(wvl, wdlValue, Some(decl)))
                case None =>
                    // optional input that was not provided
                    None
            }
        }.flatten.toMap
    }

    // evaluate Task output expressions
    private def evalTaskOutputs(inputs : Map[String, WdlValue])
            : Seq[(String, WdlType, WdlValue)] =
    {
        def lookup(varName : String) : WdlValue = {
            inputs.get(varName) match {
                case Some(x) => x
                case None => throw new AppInternalException(
                    s"No value found for variable ${varName}")
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
    private def writeEnvToDisk(env : Map[String, WdlValue]) : Unit = {
        val m : Map[String, JsValue] = env.map{ case(varName, wdlValue) =>
            (varName, JsString(Utils.marshal(wdlValue)))
        }.toMap
        val buf = (JsObject(m)).prettyPrint
        val inputVarsPath = getMetaDir().resolve("inputVars.json")
        Utils.writeFileContent(inputVarsPath, buf)
    }

    private def readTaskDeclarationsFromDisk() : Map[String, WdlValue] = {
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
    private def writeJobOutputs(jobOutputPath : Path,
                                outputs : Seq[(String, WdlType, WdlValue)]) : Unit = {
        // convert the WDL values to JSON
        val jsOutputs : Seq[(String, JsValue)] = outputs.map {
            case (key,wdlType,wdlValue) =>
                val wvl = WdlVarLinks.apply(wdlType, DeclAttrs.empty, wdlValue)
                val l = WdlVarLinks.genFields(wvl, key)
                l.map{ case (x,y) => (x, Utils.jsValueOfJsonNode(y)) }
        }.flatten
        val json = JsObject(jsOutputs.toMap)
        val ast_pp = json.prettyPrint
        System.err.println(s"writeJobOutputs ${ast_pp}")
        // write to the job outputs
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }

    // Figure out if a docker image is specified. If so, return it as a string.
    private def dockerImage(env: Map[String, WdlValue]) : Option[String] = {
        def lookup(varName : String) : WdlValue = {
            env.get(varName) match {
                case Some(x) => x
                case None => throw new AppInternalException(
                    s"No value found for variable ${varName}")
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
        // Figure out if docker is used. If so, it is specified by an
        // expression that requires evaluation.
        task.runtimeAttributes.attrs.get("docker") match {
            case None => None
            case Some(expr) => Some(evalStringExpr(expr))
        }
    }

    // Each file marked "stream", is converted into a special fifo
    // file on the instance.
    private def handleStreamingFiles(inputs: Map[String, BValue])
            : (Option[(String, String)], Map[String, BValue]) = {
        // A file that needs to be stream-downloaded.
        // Make a named pipe, and stream the file from the platform to the pipe.
        // Keep track of the download process. We need to ensure pipes have
        // different names, even if the file-names are the same.
        //
        // Note: all other files have already been downloaded.
        var fifoCount = 0
        def mkfifo(wvl: WdlVarLinks, path: String) : (WdlValue, String) = {
            val filename = Paths.get(path).toFile.getName
            val fifo:Path = Paths.get(Utils.DX_HOME, s"fifo_${fifoCount}_${filename}")
            fifoCount += 1
            val dxFileId = WdlVarLinks.getFileId(wvl)
            val bashSnippet:String =
                s"""|mkfifo ${fifo.toString}
                    |dx cat ${dxFileId} > ${fifo.toString} &
                    |""".stripMargin
            (WdlSingleFile(fifo.toString), bashSnippet)
        }

        val m:Map[String, (String, BValue)] = inputs.map{
            case (varName, BValue(wvl, wdlValue, declOpt)) =>
                val (wdlValueRewrite,bashSnippet) = wdlValue match {
                    case WdlSingleFile(path) if wvl.attrs.stream =>
                        mkfifo(wvl, path)
                    case WdlOptionalValue(_,Some(WdlSingleFile(path))) if wvl.attrs.stream =>
                        mkfifo(wvl, path)
                    case _ =>
                        // everything else
                        (wdlValue,"")
                }
                val bVal:BValue = BValue(wvl, wdlValueRewrite, declOpt)
                varName -> (bashSnippet, bVal)
        }.toMap

        // set up all the named pipes
        val snippets = m.collect{
            case (_, (bashSnippet,_)) if !bashSnippet.isEmpty => bashSnippet
        }.toVector
        val bashProlog = ("background_pids=()" +:
                              snippets).mkString("\n")

        // Wait for all background processes to complete. It is legal
        // for the user job to read only the beginning of the
        // file. This causes the download streams to close
        // prematurely, which can be show up as an error. We need to
        // tolerate this case.
        val bashEpilog = ""
//            "wait ${background_pids[@]}"
/*            """|echo "robust wait for ${background_pids[@]}"
               |for pid in ${background_pids[@]}; do
               |  while [[ ( -d /proc/$pid ) && ( -z `grep zombie /proc/$pid/status` ) ]]; do
               |    sleep 10
               |    echo "waiting for $pid"
               |  done
               |done
               |""".stripMargin.trim + "\n" */
        val inputsWithPipes = m.map{ case (varName, (_,bValue)) => varName -> bValue }.toMap
        val bashPrologEpilog =
            if (fifoCount == 0) {
                // No streaming files
                None
            } else {
                // There are some streaming files
                Some((bashProlog, bashEpilog))
            }
        (bashPrologEpilog, inputsWithPipes)
    }

    // Write the core bash script into a file. In some cases, we
    // need to run some shell setup statements before and after this
    // script. Returns these as two strings (prolog, epilog).
    private def writeBashScript(inputs: Map[String, BValue],
                                bashPrologEpilog: Option[(String, String)]) : Unit = {
        val metaDir = getMetaDir()
        val scriptPath = metaDir.resolve("script")
        val stdoutPath = metaDir.resolve("stdout")
        val stderrPath = metaDir.resolve("stderr")
        val rcPath = metaDir.resolve("rc")

        // instantiate the command
        val env: Map[Declaration, WdlValue] = inputs.map {
            case (_, BValue(_,wdlValue,Some(decl))) => decl -> wdlValue
            case (_, BValue(varName,_,None)) => throw new Exception("missing declaration")
        }.toMap
        val shellCmd : String = task.instantiateCommand(env, DxFunctions).get

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
                val cdHome = s"cd ${Utils.DX_HOME}"
                var cmdLines: List[String] = bashPrologEpilog match {
                    case None =>
                        List(cdHome, shellCmd)
                    case Some((bashProlog, bashEpilog)) =>
                        List(cdHome, bashProlog, shellCmd, bashEpilog)
                }
                val cmd = cmdLines.mkString("\n")
                s"""|#!/bin/bash
                    |(
                    |${cmd}
                    |) \\
                    |  > >( tee ${stdoutPath} ) \\
                    |  2> >( tee ${stderrPath} >&2 )
                    |echo $$? > ${rcPath}
                    |""".stripMargin.trim + "\n"
            }
        System.err.println(s"writing bash script to ${scriptPath}")
        Utils.writeFileContent(scriptPath, script)
    }

    private def writeDockerSubmitBashScript(env: Map[String, WdlValue],
                                            imgName: String,
                                            bashPrologEpilog: Option[(String, String)]) : Unit = {
        // The user wants to use a docker container with the
        // image [imgName]. We implement this with dx-docker.
        // There may be corner cases where the image will run
        // into permission limitations due to security.
        //
        // Map the home directory into the container, so that
        // we can reach the result files, and upload them to
        // the platform.
        val DX_HOME = Utils.DX_HOME
        val dockerCmd = s"""|dx-docker run --entrypoint /bin/bash
                            |-v ${DX_HOME}:${DX_HOME}
                            |${imgName}
                            |$${HOME}/execution/meta/script""".stripMargin.replaceAll("\n", " ")
        val dockerRunPath = getMetaDir().resolve("script.submit")
        val dockerRunScript = bashPrologEpilog match {
            case None =>
                s"""|#!/bin/bash -ex
                    |${dockerCmd}""".stripMargin
            case Some((bashProlog, bashEpilog)) =>
                List("#!/bin/bash -ex",
                     bashProlog,
                     dockerCmd,
                     bashEpilog
                ).mkString("\n")
        }
        System.err.println(s"writing docker run script to ${dockerRunPath}")
        Utils.writeFileContent(dockerRunPath,
                               dockerRunScript)
        dockerRunPath.toFile.setExecutable(true)
    }

    // Calculate the input variables for the task, download the input files,
    // and build a shell script to run the command.
    def prolog(jobInputPath : Path,
               jobOutputPath : Path,
               jobInfoPath: Path) : Unit = {
        // Extract types for the inputs
        val (inputTypes,_) = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        System.err.println(s"WdlType mapping =${inputTypes}")

        // Read the job input file
        val inputLines : String = Utils.readFileContent(jobInputPath)
        var inputWvls = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputTypes)

        // add the attributes from the parameter_meta section of the task
         inputWvls = inputWvls.map{ case (varName, wvl) =>
             val attrs = DeclAttrs.get(task, varName, cef)
             varName -> WdlVarLinks(wvl.wdlType, attrs, wvl.dxlink)
         }.toMap

        // evaluate the top declarations
        val inputs: Map[String, BValue] = evalDeclarations(task.declarations, inputWvls)
        val env:Map[String, WdlValue] = inputs.map{
            case (varName, BValue(_,wdlValue,_)) => varName -> wdlValue
        }.toMap
        val docker = dockerImage(env)

        // deal with files that need streaming
        val (bashPrologEpilog, inputsWithPipes) = handleStreamingFiles(inputs)

        // Write shell script to a file. It will be executed by the dx-applet code
        docker match {
            case None =>
                writeBashScript(inputsWithPipes, bashPrologEpilog)
            case Some(img) =>
                // write a script that launches the actual command inside a docker image.
                // Streamed files are set up before launching docker.
                writeBashScript(inputsWithPipes, None)
                writeDockerSubmitBashScript(env, img, bashPrologEpilog)
        }

        // serialize the environment, so we don't have to calculate it again in
        // the epilog
        writeEnvToDisk(env)
    }

    def epilog(jobInputPath : Path,
               jobOutputPath : Path,
               jobInfoPath: Path) : Unit = {
        val taskInputs : Map[String, WdlValue] = readTaskDeclarationsFromDisk()

        // evaluate outputs
        val outputs : Seq[(String, WdlType, WdlValue)] = evalTaskOutputs(taskInputs)
        writeJobOutputs(jobOutputPath, outputs)
    }


    // Evaluate the runtime expressions, and figure out which instance type
    // this task requires.
    private def calcInstanceType(taskInputs: Map[String, WdlVarLinks],
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
        System.err.println(s"""|calcInstanceType memory=${memory} disk=${diskSpace}
                              |cores=${cores} instancetype=${iType}"""
                              .stripMargin.replaceAll("\n", " "))
        iType
    }

    private def relaunchBuildInputs(inputWvls: Map[String, WdlVarLinks]) : ObjectNode = {
        var builder : DXJSON.ObjectBuilder = DXJSON.getObjectBuilder()
        inputWvls.foreach{ case (varName, wvl) =>
            WdlVarLinks.genFields(wvl, varName).foreach{ case (fieldName, jsNode) =>
                builder = builder.put(fieldName, jsNode)
            }
        }
        builder.build()
    }

    private def runSubJob(entryPoint:String,
                          instanceType:String,
                          inputs:ObjectNode) : DXJob = {
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
    def relaunch(jobInputPath : Path,
                 jobOutputPath : Path,
                 jobInfoPath: Path) : Unit = {
        // Extract types for the inputs
        val (inputTypes,_) = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        System.err.println(s"WdlType mapping =${inputTypes}")

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
        val instanceType:String = calcInstanceType(inputWvls, instanceTypeDB)

        // relaunch the applet on the correct instance type
        val inputs = relaunchBuildInputs(inputWvls)

        // Run a sub-job with the "body" entry point, and the required instance type
        val dxSubJob : DXJob = runSubJob("body", instanceType, inputs)

        // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
        // is exactly the same as the parent, we can immediately exit the parent job.
        val outputs: Map[String, JsonNode] = task.outputs.map { tso =>
            val wvl = WdlVarLinks(tso.wdlType,
                                  DeclAttrs.empty,
                                  DxlJob(dxSubJob, tso.unqualifiedName))
            WdlVarLinks.genFields(wvl, tso.unqualifiedName)
        }.flatten.toMap


        // write the outputs to the job_output.json file
        val json = JsObject(
            outputs.map{ case (key, json) => key -> Utils.jsValueOfJsonNode(json) }.toMap
        )
        val ast_pp = json.prettyPrint
        System.err.println(s"outputs = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
