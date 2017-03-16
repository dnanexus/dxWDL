package dxWDL

// DX bindings
import com.dnanexus.{DXFile, DXProject, DXEnvironment, DXApplet}

import java.nio.file.{Path, Paths, Files}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
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

/** Run a dx applet previously compiled from a WDL workflow.

The canonical example for what is happening here, is the workflow below.
The Add2 call is compiled into an applet. The call inputs are {Add.sum},
implemented as a stage-dependency. The Task inputs are {a, b}.

task Add {
    Int a
    Int b

    command {
    }
    output {
        Int sum = a + b
    }
}

workflow call_expr_deps {
    call Add {
         input: a = 3, b = 2
    }
    call Add as Add2 {
         input: a = 2 * Add.sum, b = 3
    }
    output {
        Add2.sum
    }
}
  */

object AppletRunner {
    lazy val dxEnv = DXEnvironment.create()

    def getMetaDir() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.safeMkdir(metaDir)
        metaDir
    }

    // evaluate the call inputs, return inputs for the task.
    //
    // call Add as Add2 {
    //    input: a = 2 * Add.sum, b = 3
    // }
    // Start with a wdlValue for {Add.sum}, and get wdlValues for {a,b}
    def evalCallInputs(call : Call, callInputs : Map[String, WdlValue]) = {
        System.err.print(s"callInputs=\n${Utils.inputsToString(callInputs)}\n")
        def lookup(varName : String) : WdlValue =
            callInputs.get(varName) match {
                case Some(x) => x
                case None => throw new UnboundVariableException(varName)
            }

        val providedInputs = call.inputMappings.map { case (varName,expr) =>
            System.err.println(s"evalCallInputs ${varName} ${expr.toWdlString}")
            val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
            varName -> v
        }.toMap

        // Pass inputs passed not listed in the inputMappings. This allows
        // the user to pass inputs directly to the task.
        val extraInputs = callInputs.map{ case (varName, wdlValue) =>
            if (call.inputMappings.contains(varName)) {
                None
            } else {
                Some(varName -> wdlValue)
            }
        }.flatten.toMap

        providedInputs ++ extraInputs
    }

    def evalTaskInputs(task : Task, taskInputs : Map[String, WdlValue])
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

                    // compulstory input.
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
    def writeTaskInputsToDisk(taskInputs : Map[String, WdlValue]) : Unit = {
        val m : Map[String, JsValue] = taskInputs.map{ case(varName, wdlValue) =>
            (varName, JsString(Utils.marshal(wdlValue)))
        }.toMap
        val buf = (JsObject(m)).prettyPrint
        val inputVarsPath = getMetaDir().resolve("inputVars.json")
        Utils.writeFileContent(inputVarsPath, buf)
    }

    def readTaskInputsFromDisk() : Map[String, WdlValue] = {
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

    // At this point, the inputs are flat. For example:
    //  Map (
    //     Add.sum -> 1
    //     Mul.result -> 8
    //  )
    //
    // convert it into:
    //   Map (
    //    "Add" -> WdlObject(Map("sum" -> 1))
    //    "Mul" -> WdlObject(Map("result" -> 8))
    //   )
    def addScopeToInputs(callInputs : Map[String, WdlValue]) : Map[String, WdlValue] = {
        def recursiveBuild(rootMap : Map[String, WdlValue],
                           components : Seq[String],
                           wdlValue : WdlValue) : Map[String, WdlValue] = {
            if (components.length == 1) {
                // bottom of recursion
                rootMap + (components.head -> wdlValue)
            }
            else {
                if (rootMap contains components.head) {
                    // already have a sub-scope by this name, we need to update it, but not
                    // in place
                    val o : WdlObject = rootMap(components.head) match {
                        case o: WdlObject => o
                        case _ =>  throw new Exception("Scope should be a WdlObject")
                    }
                    val subTree = recursiveBuild(o.value, components.tail, wdlValue)
                    rootMap + (components.head -> new WdlObject(subTree))
                } else {
                    // new sub-scope
                    val subTree = recursiveBuild(Map.empty, components.tail, wdlValue)
                    rootMap + (components.head -> new WdlObject(subTree))
                }
            }
        }

        // A tree of nested scopes, that we update for each variable
        var tree = Map.empty[String, WdlValue]
        callInputs.map { case (varName, wdlValue) =>
            // "A.B.C"
            //    baseName = "C"
            //    fqPath = List("A", "B")
            val components = varName.split("\\.")
            tree = recursiveBuild(tree, components, wdlValue)
        }
        tree
    }

    // Upload output files as a consequence
    def writeJobOutputs(jobOutputPath : Path,
                        outputs : Seq[(String, WdlType, WdlValue)]) : Unit = {
        // convert the WDL values to JSON
        val jsOutputs : Seq[(String, JsValue)] = outputs.map {
            case (key,wdlType,wdlValue) =>
                val wvl = WdlVarLinks.outputFieldOfWdlValue(key, wdlType, wdlValue)
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

    def prologCore(call: Call, callInputs: Map[String, WdlValue]) : Unit = {
        // make the inputs conform to nested scopes
        val scopedCallInputs = addScopeToInputs(callInputs)

        // evaluate the inputs to the call
        val taskInputs : Map[String, WdlValue] = evalCallInputs(call, scopedCallInputs)

        val task : Task = Utils.taskOfCall(call)
        val inputs = evalTaskInputs(task, taskInputs)

        // Write shell script to a file. It will be executed by the dx-applet code
        writeBashScript(task, inputs)

        // serialize the environment, so we don't have to calculate it again in
        // the epilog
        val m = inputs.map{ case (decl, wdlValue) =>
            decl.unqualifiedName -> wdlValue
        }.toMap
        writeTaskInputsToDisk(m)
    }

    // Calculate the input variables for the task, download the input files,
    // and build a shell script to run the command.
    def prolog(wf: Workflow,
               jobInputPath : Path,
               jobOutputPath : Path,
               jobInfoPath: Path) : Unit = {
        val dxapp : DXApplet = dxEnv.getJob().describe().getApplet()
        val desc : DXApplet.Describe = dxapp.describe()
        val call: Call = getCall(wf, desc)

        // Extract types for closure inputs
        val closureTypes = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        System.err.println(s"WdlType mapping =${closureTypes}")

        // Read the job input file
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val callInputs = WdlVarLinks.loadJobInputs(inputLines, closureTypes)
        prologCore(call, callInputs)
    }

    def epilogCore(call: Call) : Seq[(String, WdlType, WdlValue)]  = {
        // read serialized inputs
        val task : Task = Utils.taskOfCall(call)
        val taskInputs : Map[String, WdlValue] = readTaskInputsFromDisk()

        // evaluate outputs
        evalTaskOutputs(task, taskInputs)
    }

    def epilog(wf: Workflow,
               jobInputPath : Path,
               jobOutputPath : Path,
               jobInfoPath: Path) : Unit = {
        val dxapp : DXApplet = dxEnv.getJob().describe().getApplet()
        val desc : DXApplet.Describe = dxapp.describe()
        val call: Call = getCall(wf, desc)
        val outputs : Seq[(String, WdlType, WdlValue)] = epilogCore(call)
        writeJobOutputs(jobOutputPath, outputs)
    }
}
