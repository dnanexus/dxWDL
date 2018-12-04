/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import wom.core.WorkflowSource
import wom.callable.{CallableTaskDefinition, ExecutableTaskDefinition, WorkflowDefinition}
import wom.callable.Callable._
import wom.expression.WomExpression
import wom.graph._
import wom.types._
import wom.values._

import dxWDL.util._
import IR.{CVar, SArg}

case class GenerateIR(locked: Boolean,
                      wfKind: IR.WorkflowKind.Value,
                      verbose: Verbose) {
    //private val verbose2:Boolean = verbose.keywords contains "GenerateIR"

    case class LinkedVar(cVar: CVar, sArg: SArg)
    type CallEnv = Map[String, LinkedVar]

    private class DynamicInstanceTypesException private(ex: Exception) extends RuntimeException(ex) {
        def this() = this(new RuntimeException("Runtime instance type calculation required"))
    }

    // generate a stage Id, this is a string of the form: 'stage-xxx'
    /*
    private var stageNum = 0
    private def genStageId(stageName: Option[String] = None) : DXWorkflowStage = {
        stageName match {
            case None =>
                val retval = DXWorkflowStage(s"stage-${stageNum}")
                stageNum += 1
                retval
            case Some(nm) =>
                DXWorkflowStage(s"stage-${nm}")
        }
    }*/

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

        taskOpt match {
            case None =>
                // A utility calculation, that requires minimal computing resources.
                // For example, the top level of a scatter block. We use
                // the default instance type, because one will probably be available,
                // and it will probably be inexpensive.
                IR.InstanceTypeDefault
            case Some(task) =>
                try {
                    val dxInstaceType = evalAttr(task, Extras.DX_INSTANCE_TYPE_ATTR)
                    val memory = evalAttr(task, "memory")
                    val diskSpace = evalAttr(task, "disks")
                    val cores = evalAttr(task, "cpu")
                    val iTypeDesc = InstanceTypeDB.parse(dxInstaceType, memory, diskSpace, cores)
                    IR.InstanceTypeConst(iTypeDesc.dxInstanceType,
                                         iTypeDesc.memoryMB,
                                         iTypeDesc.diskGB,
                                         iTypeDesc.cpu)
                } catch {
                    case e : DynamicInstanceTypesException =>
                        // The generated code will need to calculate the instance type at runtime
                        IR.InstanceTypeRuntime
                }
        }
    }

    // Compile a WDL task into an applet.
    //
    // Note: check if a task is a real WDL task, or if it is a wrapper for a
    // native applet.
    private def compileTask(task : CallableTaskDefinition,
                            taskSourceCode: String) : IR.Applet = {
        Utils.trace(verbose.on, s"Compiling task ${task.name}")

        // create dx:applet input definitions. Note, some "inputs" are
        // actually expressions.
        val inputs: Vector[CVar] = task.inputs.flatMap{
            case RequiredInputDefinition(iName, womType, _, _) =>
                Some(CVar(iName.value, womType, None))

            case InputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
                Utils.ifConstEval(defaultExpr) match {
                    case None =>
                        // This is a task "input" of the form:
                        //    Int y = x + 3
                        // We consider it an expression, and not an input. The
                        // runtime system will evaluate it.
                        None
                    case Some(value) =>
                        Some(CVar(iName.value, womType, Some(value)))
                }

            // An input whose value should always be calculated from the default, and is
            // not allowed to be overridden.
            case FixedInputDefinition(iName, womType, defaultExpr, _, _) =>
                None

            case OptionalInputDefinition(iName, WomOptionalType(womType), _, _) =>
                Some(CVar(iName.value, WomOptionalType(womType), None))
        }.toVector

        // create dx:applet outputs
        val outputs : Vector[CVar] = task.outputs.map{
            case OutputDefinition(id, womType, expr) =>
                val defaultValue = Utils.ifConstEval(expr) match {
                    case None =>
                        // This is an expression to be evaluated at runtime
                        None
                    case Some(value) =>
                        // A constant, we can assign it now.
                        Some(value)
                }
                CVar(id.value, womType, defaultValue)
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


    // Represent each workflow input with:
    // 1. CVar, for type declarations
    // 2. SVar, connecting it to the source of the input
    //
    // It is possible to provide a default value to a workflow input.
    // For example:
    // workflow w {
    //   Int? x = 3
    //   ...
    // }
    // We handle only the case where the default is a constant.
    def buildWorkflowInput(input: GraphInputNode) : (CVar,SArg) = {
        val cVar = input match {
            case RequiredGraphInputNode(id, womType, nameInInputSet, valueMapper) =>
                CVar(id.workflowLocalName, womType, None)

            case OptionalGraphInputNode(id, womType, nameInInputSet, valueMapper) =>
                assert(womType.isInstanceOf[WomOptionalType])
                CVar(id.workflowLocalName, womType, None)

            case OptionalGraphInputNodeWithDefault(id, womType, defaultExpr : WomExpression, nameInInputSet, valueMapper) =>
                val defaultValue: WomValue = Utils.ifConstEval(defaultExpr) match {
                    case None => throw new Exception(s"""|default expression in input should be a constant
                                                         | ${input}
                                                         |""".stripMargin)
                    case Some(value) => value
                }
                CVar(id.workflowLocalName, womType, Some(defaultValue))

            case other =>
                throw new Exception(s"unhandled input ${other}")
        }

        (cVar, IR.SArgWorkflowInput(cVar))
    }

    // Compile a (single) WDL workflow into a single dx:workflow.
    //
    // There are cases where we are going to need to generate dx:subworkflows.
    // This is not handled currently.
    def compileWorkflow(wf: WorkflowDefinition) : IR.Workflow = {
        Utils.trace(verbose.on, s"compiling workflow ${wf.name}")

        val graph = wf.innerGraph

        // as a first step, only handle straight line workflows
        if (!graph.workflowCalls.isEmpty)
            throw new Exception(s"Workflow ${wf.name} calls a subworkflow; currently not supported")
        if (!graph.scatters.isEmpty)
            throw new Exception(s"Workflow ${wf.name} includes a scatter; currently not supported")
        if (!graph.conditionals.isEmpty)
            throw new Exception(s"Workflow ${wf.name} includes a conditional; currently not supported")

        // now we are sure the workflow is a simple straight line. It only contains
        // task calls.

        // print the inputs
        // compile these into workflow inputs
        val wfInputs:Vector[(CVar, SArg)] = graph.inputNodes.map(buildWorkflowInput).toVector

        // print the series of calls
        // compile into a series of stages, each of which
        // calls a task. Each task must be compiled in a dx:applet
        // beforehand (?)
        System.out.println(s"${wf.name} calls")
        graph.calls.foreach{ n =>
            System.out.println(s"${n}\n")
        }

        // print the outputs
        // compile into workflow outputs
        val wfOutputs : Vector[(CVar, SArg)] = graph.outputNodes.map{
            case PortBasedGraphOutputNode(id, womType, _) =>
                val cVar = CVar(id.workflowLocalName, womType, None)
                // TODO
                // 1. Setup the environment of linked values
                // 2. Translate the WOM port to a variable name.
                // 3. Lookup the variable in the environment
                // 4. the outputs could include expressions.
                //    This would require an extra calculation block.
                (cVar, IR.SArgEmpty)
            case exprGon :ExpressionBasedGraphOutputNode =>
                /*System.out.println(s"""|ExpressionBasedGraphOutputNode
                                       | ${exprGon.womType}
                                       | ${exprGon.graphOutputPort}
                                       |""".stripMargin)*/
                val cVar = CVar(exprGon.graphOutputPort.name, exprGon.womType, None)
                (cVar, IR.SArgEmpty)
            case other =>
                throw new Exception(s"unhandled output ${other}")
        }.toVector

        // Create a stage per call/scatter-block/declaration-block
        val subBlocks = Block.splitIntoBlocks(graph.nodes.toVector)
        //Utils.ignore(subBlocks)
        subBlocks.foreach{ block =>
            System.out.println(block + "\n")
        }

        /*
        val (allStageInfo_i, wfOutputs) =
            if (locked)
                compileWorkflowLocked(wf, wfInputs, subBlocks)
            else
                compileWorkflowRegular(wf, wfInputs, subBlocks)

        // Add a reorganization applet if requested
        val allStageInfo =
            if (reorg) {
                val (rStage, rApl) = createReorgApplet(wfOutputs)
                allStageInfo_i :+ (rStage, Some(rApl))
            } else {
                allStageInfo_i
            }

        val (stages, auxApplets) = allStageInfo.unzip
        val aApplets: Map[String, IR.Applet] =
            auxApplets
                .flatten
                .map(apl => apl.name -> apl).toMap
         */
        val irwf = IR.Workflow(wf.name, wfInputs, wfOutputs, /*stages*/ Vector.empty[IR.Stage], locked, wfKind)
        //execNameBox.add(wf.unqualifiedName)
        //(irwf, aApplets)
        irwf

        //throw new Exception("TODO")
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
              locked: Boolean,
              verbose: Verbose) : IR.Bundle = {
        Utils.trace(verbose.on, s"IR pass")
        Utils.traceLevelInc()

        val gir = GenerateIR(locked, IR.WorkflowKind.TopLevel, verbose)

        // Scan the source files and extract the tasks. It is hard
        // to generate WDL from the abstract syntax tree (AST). One
        // issue is that tabs and special characters have to preserved.
        // There is no built-in method for this.
        val taskDir = allSources.foldLeft(Map.empty[String, String]) {
            case (accu, (filename, srcCode)) =>
                val d = ParseWomSourceFile.scanForTasks(srcCode)
                accu ++ d
        }

        val allCallables = womBundle.allCallables.map{
            case (name, callable) =>
                val exec = gir.compileCallable(callable, taskDir)
                name -> exec
        }.toMap

        val primary = womBundle.primaryCallable.map{ callable =>
            allCallables(callable.name)
        }

        Utils.traceLevelDec()
        IR.Bundle(primary, allCallables)
    }
}
