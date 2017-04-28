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

// DX bindings
import com.dnanexus.{DXFile, DXProject, DXEnvironment, DXApplet}

import java.nio.file.{Path, Paths, Files}
import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import wdl4s.AstTools._
import wdl4s.expression.{WdlStandardLibraryFunctionsType, WdlStandardLibraryFunctions}
import wdl4s.types._
import wdl4s.values._
import wdl4s.{Call, Declaration, WdlNamespaceWithWorkflow, Task, WdlExpression, WdlNamespace, Workflow}
import wdl4s.WdlExpression.AstForExpressions

object RunnerTask {
    lazy val dxEnv = DXEnvironment.create()

    def getMetaDir() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.safeMkdir(metaDir)
        metaDir
    }

    def evalDeclarations(task : Task, taskInputs : Map[String, WdlValue])
            : Map[Declaration, WdlValue] = {
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

                // declaration to evalute, not an input
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
        def lookup(varName : String) : WdlValue =
            inputs.get(varName) match {
                case Some(x) => x
                case None => throw new AppInternalException(s"No value found for variable ${varName}")
            }

        task.outputs.map { case outdef =>
            outdef.expression match {
                case Some(expr) =>
                    val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
                    (outdef.unqualifiedName, outdef.wdlType, v)
                case None =>
                    throw new AppInternalException("Output has no evaluating expression")
            }
        }
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
        System.err.println(s"writeJobOutputs ${ast_pp}")
        // write to the job outputs
        Utils.writeFileContent(jobOutputPath, ast_pp)
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
                s"""|#!/bin/sh
                    |echo 0 > ${rcPath}
                    |""".stripMargin.trim + "\n"
            } else {
                s"""|#!/bin/sh
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
        System.err.println(s"writing bash script to ${scriptPath}")
        Utils.writeFileContent(scriptPath, script)
    }

    def getCall(wf: Workflow, desc: DXApplet.Describe) : Call = {
        // Query the platform the the applet name
        val appFQName : String = desc.getName()
        // It has a fully qualified name A.B, extract the last component (B)
        val appName = appFQName.split("\\.").last

        val call : Call = wf.findCallByName(appName) match {
            case None => throw new AppInternalException(s"Call ${appName} not found in WDL file")
            case Some(call) => call
        }
        call
    }

    def prologCore(task: Task, inputs: Map[String, WdlValue]) : Unit = {
        val topDecls = evalDeclarations(task, inputs)

        // Write shell script to a file. It will be executed by the dx-applet code
        writeBashScript(task, topDecls)

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
        System.err.println(s"WdlType mapping =${inputTypes}")

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
}
