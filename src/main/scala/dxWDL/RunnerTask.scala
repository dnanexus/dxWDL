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

import com.dnanexus.{DXJob}
import java.nio.file.{Path, Paths}
import scala.collection.mutable.HashMap
import spray.json._
import Utils.appletLog
import wdl4s.wdl.types._
import wdl4s.wdl.values._
import wdl4s.wdl.{Declaration, TaskOutput, WdlExpression, WdlTask}

case class RunnerTask(task:WdlTask,
                      cef: CompilerErrorFormatter) {
    def getMetaDir() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.safeMkdir(metaDir)
        metaDir
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
            val vRaw : WdlValue = expr.evaluate(lookup, DxFunctions).get
            val v = Utils.cast(tso.wdlType, vRaw, tso.unqualifiedName)
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
                val wvl = WdlVarLinks.importFromWDL(wdlType, DeclAttrs.empty, wdlValue)
                WdlVarLinks.genFields(wvl, key)
        }.flatten
        val json = JsObject(jsOutputs.toMap)
        val ast_pp = json.prettyPrint
        appletLog(s"writeJobOutputs ${ast_pp}")
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
            : (Option[String], Map[String, BValue]) = {
        // A file that needs to be stream-downloaded. Make a named
        // pipe, and stream the file from the platform to the pipe.
        // Ensure pipes have different names, even if the
        // file-names are the same. Write the process ids of the download jobs,
        // to stdout. The calling script will keep track of them, and check
        // for abnormal termination.
        //
        // Note: at this point, all other files have already been downloaded.
        var fifoCount = 0
        def mkfifo(wvl: WdlVarLinks, path: String) : (WdlValue, String) = {
            val filename = Paths.get(path).toFile.getName
            val fifo:Path = Paths.get(Utils.DX_HOME, s"fifo_${fifoCount}_${filename}")
            fifoCount += 1
            val dxFileId = WdlVarLinks.getFileId(wvl)
            val bashSnippet:String =
                s"""|mkfifo ${fifo.toString}
                    |dx cat ${dxFileId} > ${fifo.toString} &
                    |echo $$!
                    |""".stripMargin
            (WdlSingleFile(fifo.toString), bashSnippet)
        }

        val m:Map[String, (String, BValue)] = inputs.map{
            case (varName, BValue(wvl, wdlValue)) =>
                val (wdlValueRewrite,bashSnippet) = wdlValue match {
                    case WdlSingleFile(path) if wvl.attrs.stream =>
                        mkfifo(wvl, path)
                    case WdlOptionalValue(_,Some(WdlSingleFile(path))) if wvl.attrs.stream =>
                        mkfifo(wvl, path)
                    case _ =>
                        // everything else
                        (wdlValue,"")
                }
                val bVal:BValue = BValue(wvl, wdlValueRewrite)
                varName -> (bashSnippet, bVal)
        }.toMap

        // set up all the named pipes
        val bashSetupStreams:Option[String] =
            if (fifoCount > 0) {
                // There are streaming files to set up
                val snippets = m.collect{
                    case (_, (bashSnippet,_)) if !bashSnippet.isEmpty => bashSnippet
                }
                Some(snippets.mkString("\n"))
            } else {
                None
            }

        val inputsWithPipes = m.map{ case (varName, (_,bValue)) => varName -> bValue }.toMap
        (bashSetupStreams, inputsWithPipes)
    }

    // Write the core bash script into a file. In some cases, we
    // need to run some shell setup statements before and after this
    // script. Returns these as two strings (prolog, epilog).
    private def writeBashScript(inputs: Map[String, BValue]) : Unit = {
        val metaDir = getMetaDir()
        val scriptPath = metaDir.resolve("script")
        val stdoutPath = metaDir.resolve("stdout")
        val stderrPath = metaDir.resolve("stderr")
        val rcPath = metaDir.resolve("rc")

        // instantiate the command
        val env: Map[Declaration, WdlValue] = inputs.map {
            case (varName, BValue(cVar,wdlValue)) =>
                val decl = task.declarations.find(_.unqualifiedName == varName) match {
                    case Some(x) => x
                    case None => throw new Exception(
                        s"Cannot find declaration for variable ${varName}")
                }
                decl -> wdlValue
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
                s"""|#!/bin/bash
                    |(
                    |    cd ${Utils.DX_HOME}
                    |    ${shellCmd}
                    |) \\
                    |  > >( tee ${stdoutPath} ) \\
                    |  2> >( tee ${stderrPath} >&2 )
                    |echo $$? > ${rcPath}
                    |""".stripMargin.trim + "\n"
            }
        appletLog(s"writing bash script to ${scriptPath}")
        Utils.writeFileContent(scriptPath, script)
    }

    private def writeDockerSubmitBashScript(env: Map[String, WdlValue],
                                            imgName: String) : Unit = {
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
        val dockerRunScript =
            s"""|#!/bin/bash -ex
                |${dockerCmd}""".stripMargin
        appletLog(s"writing docker run script to ${dockerRunPath}")
        Utils.writeFileContent(dockerRunPath, dockerRunScript)
        dockerRunPath.toFile.setExecutable(true)
    }

    // Calculate the input variables for the task, download the input files,
    // and build a shell script to run the command.
    def prolog(jobInputPath : Path,
               jobOutputPath : Path,
               jobInfoPath: Path) : Unit = {
        // Extract types for the inputs
        val (inputSpec,_) = Utils.loadExecInfo
        appletLog(s"WdlType mapping =${inputSpec}")

        // Read the job input file
        val inputLines : String = Utils.readFileContent(jobInputPath)
        var inputWvls = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputSpec)

        // add the attributes from the parameter_meta section of the task
         inputWvls = inputWvls.map{ case (varName, wvl) =>
             val attrs = DeclAttrs.get(task, varName, cef)
             varName -> WdlVarLinks(wvl.wdlType, attrs, wvl.dxlink)
         }.toMap

        // evaluate the top declarations
        val inputs: Map[String, BValue] =
            RunnerEval.evalDeclarations(task.declarations, inputWvls, true, Some((task, cef)))
        val env:Map[String, WdlValue] = inputs.map{
            case (varName, BValue(_,wdlValue)) => varName -> wdlValue
        }.toMap
        val docker = dockerImage(env)

        // deal with files that need streaming
        val (bashSetupStreams, inputsWithPipes) = handleStreamingFiles(inputs)
        bashSetupStreams match {
            case Some(snippet) =>
                val path = getMetaDir().resolve("setup_streams")
                appletLog(s"writing bash script for stream(s) set up to ${path}")
                Utils.writeFileContent(path, snippet)
                path.toFile.setExecutable(true)
            case None => ()
        }

        // Write shell script to a file. It will be executed by the dx-applet code
        writeBashScript(inputsWithPipes)
        docker match {
            case Some(img) =>
                // write a script that launches the actual command inside a docker image.
                // Streamed files are set up before launching docker.
                writeDockerSubmitBashScript(env, img)
            case None => ()
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
        appletLog(s"""|calcInstanceType memory=${memory} disk=${diskSpace}
                              |cores=${cores} instancetype=${iType}"""
                              .stripMargin.replaceAll("\n", " "))
        iType
    }

    private def relaunchBuildInputs(inputWvls: Map[String, WdlVarLinks]) : JsValue = {
        val inputs:Map[String,JsValue] = inputWvls.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, wvl)) =>
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
        }
        JsObject(inputs.toMap)
    }

    /** The runtime attributes need to be calculated at runtime. Evaluate them,
      *  determine the instance type [xxxx], and relaunch the job on [xxxx]
      */
    def relaunch(jobInputPath : Path,
                 jobOutputPath : Path,
                 jobInfoPath: Path) : Unit = {
        // Extract types for the inputs
        val (inputSpec,_) = Utils.loadExecInfo
        appletLog(s"WdlType mapping =${inputSpec}")

        // Read the job input file, and load the inputs without downloading
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val inputWvls = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputSpec)

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
        val dxSubJob : DXJob = Utils.runSubJob("body", Some(instanceType), inputs, Vector.empty)

        // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
        // is exactly the same as the parent, we can immediately exit the parent job.
        val outputs: Map[String, JsValue] = task.outputs.map { tso =>
            val wvl = WdlVarLinks(tso.wdlType,
                                  DeclAttrs.empty,
                                  DxlJob(dxSubJob, tso.unqualifiedName))
            WdlVarLinks.genFields(wvl, tso.unqualifiedName)
        }.flatten.toMap


        // write the outputs to the job_output.json file
        val json = JsObject(outputs)
        val ast_pp = json.prettyPrint
        appletLog(s"outputs = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
