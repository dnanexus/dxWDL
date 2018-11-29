/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import dxWDL.util._
import IR.{CVar}
import wom.core.WorkflowSource
import wom.callable.{CallableTaskDefinition, ExecutableTaskDefinition, WorkflowDefinition}
import wom.values._

case class GenerateIR(verbose: Verbose) {
    //private val verbose2:Boolean = verbose.keywords contains "GenerateIR"

    private class DynamicInstanceTypesException private(ex: Exception) extends RuntimeException(ex) {
        def this() = this(new RuntimeException("Runtime instance type calculation required"))
    }

    // Figure out which instance to use.
    //
    // Extract three fields from the task:
    // RAM, disk space, and number of cores. These are WDL expressions
    // that, in the general case, could be calculated only at runtime.
    // At compile time, constants expressions are handled. Some can
    // only be evaluated at runtime.
    def calcInstanceType(taskOpt: Option[CallableTaskDefinition]) : IR.InstanceType = {
        def evalAttr(task: CallableTaskDefinition, attrName: String) : Option[WomValue] = {
            task.runtimeAttributes.attributes.get(attrName) match {
                case None => None
                case Some(expr) =>
                    val result: ErrorOr[WomValue] =
                        expr.evaluateValue(Map.empty[String, WomValue], wom.expression.NoIoFunctionSet)
                    result match {
                        case Invalid(_) => throw new DynamicInstanceTypesException()
                        case Valid(x: WomValue) => Some(x)
                    }
            }
        }

        try {
            taskOpt match {
                case None =>
                    // A utility calculation, that requires minimal computing resources.
                    // For example, the top level of a scatter block. We use
                    // the default instance type, because one will probably be available,
                    // and it will probably be inexpensive.
                    IR.InstanceTypeDefault
                case Some(task) =>
                    val dxInstaceType = evalAttr(task, Extras.DX_INSTANCE_TYPE_ATTR)
                    val memory = evalAttr(task, "memory")
                    val diskSpace = evalAttr(task, "disks")
                    val cores = evalAttr(task, "cpu")
                    val iTypeDesc = InstanceTypeDB.parse(dxInstaceType, memory, diskSpace, cores)
                    IR.InstanceTypeConst(iTypeDesc.dxInstanceType,
                                         iTypeDesc.memoryMB,
                                         iTypeDesc.diskGB,
                                         iTypeDesc.cpu)
            }
        } catch {
            case e : DynamicInstanceTypesException =>
                // The generated code will need to calculate the instance type at runtime
                IR.InstanceTypeRuntime
        }
    }

    // Compile a WDL task into an applet.
    //
    // Note: check if a task is a real WDL task, or if it is a wrapper for a
    // native applet.
    private def compileTask(task : CallableTaskDefinition,
                            taskSourceCode: String) : IR.Applet = {
        Utils.trace(verbose.on, s"Compiling task ${task.name}")

        val inputs = task.inputs.map{
            inp => CVar(inp.localName.value, inp.womType, DeclAttrs.empty)
        }.toVector
        val outputs = task.outputs.map{
            out => CVar(out.localName.value, out.womType, DeclAttrs.empty)
        }.toVector
        val instanceType = calcInstanceType(Some(task))

        val kind =
            (task.meta.get("type"), task.meta.get("id")) match {
                case (Some("native"), Some(id)) =>
                    // wrapper for a native applet.
                    // make sure the runtime block is empty
                    if (!task.runtimeAttributes.attributes.isEmpty)
                        throw new Exception(s"Native task ${task.name} should have an empty runtime block")
                    IR.AppletKindNative(id)
                case (_,_) =>
                    // a WDL task
                    IR.AppletKindTask
            }

        // Figure out if we need to use docker
        val docker = task.runtimeAttributes.attributes.get("docker") match {
            case None =>
                IR.DockerImageNone
            case Some(expr) if Utils.isExpressionConst(expr) =>
                val wdlConst = Utils.evalConst(expr)
                wdlConst match {
                    case WomString(url) if url.startsWith(Utils.DX_URL_PREFIX) =>
                        // A constant image specified with a DX URL
                        val dxRecord = DxPath.lookupDxURLRecord(url)
                        IR.DockerImageDxAsset(dxRecord)
                    case _ =>
                        // Probably a public docker image
                        IR.DockerImageNetwork
                }
            case _ =>
                // Image will be downloaded from the network
                IR.DockerImageNetwork
        }
        // The docker container is on the platform, we need to remove
        // the dxURLs in the runtime section, to avoid a runtime
        // lookup. For example:
        //
        //   dx://dxWDL_playground:/glnexus_internal  ->   dx://project-xxxx:record-yyyy
        /*val taskCleaned = docker match {
            case IR.DockerImageDxAsset(dxRecord) =>
                WdlRewrite.taskReplaceDockerValue(task, dxRecord)
            case _ => task
        }*/

        IR.Applet(task.name,
                  inputs,
                  outputs,
                  instanceType,
                  docker,
                  kind,
                  task,
                  taskSourceCode)
    }


    // Compile a (single) WDL workflow into a single dx:workflow.
    //
    // There are cases where we are going to need to generate dx:subworkflows.
    // This is not handled currently.
    def compileWorkflow(wf: WorkflowDefinition) : IR.Workflow = {
        System.out.println(s"wf = ${wf}")
        throw new Exception("TODO")
    }

    // Entry point for compiling tasks and workflows into IR
    def compileCallable(callable: wom.callable.Callable,
                        taskDir: Map[String, String]) : IR.Callable = {
        def compileTask2(task : CallableTaskDefinition) = {
            val taskSourceCode = taskDir.get(task.name) match {
                case None => throw new Exception(s"Did not find task ${task.name}")
                case Some(x) => x
            }
            compileTask(task, taskSourceCode)
        }
        callable match {
            case exec : ExecutableTaskDefinition =>
                val task = exec.callableTaskDefinition
                compileTask2(task)
            case task : CallableTaskDefinition =>
                compileTask2(task)
            case wf: WorkflowDefinition =>
                compileWorkflow(wf)
            case x =>
                throw new Exception(s"""|Can't compile: ${callable.name}, class=${callable.getClass}
                                        |${x}
                                        |""".stripMargin.replaceAll("\n", " "))
        }
    }
}


object GenerateIR {

    // Entrypoint
    def apply(womBundle : wom.executable.WomBundle,
              allSources: Map[String, WorkflowSource],
              verbose: Verbose) : IR.Bundle = {
        Utils.trace(verbose.on, s"IR pass")
        Utils.traceLevelInc()

        val gir = GenerateIR(verbose)

        // Scan the source files and extract the tasks. It is hard
        // to generate WDL from the abstract syntax tree (AST). One
        // issue is that tabs and special characters have to preserved.
        // There is no built-in method for this.
        val taskDir = allSources.foldLeft(Map.empty[String, String]) {
            case (accu, (filename, srcCode)) =>
                val d = ParseWomSourceFile.scanForTasks(srcCode)
                accu ++ d
        }

        val primary = womBundle.primaryCallable.map{ callable =>
            gir.compileCallable(callable, taskDir)
        }
        val allCallables = womBundle.allCallables.map{
            case (name, callable) =>
                val exec = gir.compileCallable(callable, taskDir)
                name -> exec
        }.toMap

        Utils.traceLevelDec()
        IR.Bundle(primary, allCallables)
    }
}
