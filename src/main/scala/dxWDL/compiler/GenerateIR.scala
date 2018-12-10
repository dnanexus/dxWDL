/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import wom.core.WorkflowSource
import wom.callable.{Callable, CallableTaskDefinition, ExecutableTaskDefinition, WorkflowDefinition}
import wom.callable.Callable._
import wom.expression.WomExpression
import wom.graph._
import wom.graph.expression._
import wom.types._
import wom.values._
import dxWDL.util._
import IR.{CVar, SArg}

case class GenerateIR(callables: Map[String, IR.Callable],
                      locked: Boolean,
                      wfKind: IR.WorkflowKind.Value,
                      verbose: Verbose) {
    //private val verbose2:Boolean = verbose.keywords contains "GenerateIR"

    private case class LinkedVar(cVar: CVar, sArg: SArg)
    private type CallEnv = Map[String, LinkedVar]

    private class DynamicInstanceTypesException private(ex: Exception) extends RuntimeException(ex) {
        def this() = this(new RuntimeException("Runtime instance type calculation required"))
    }

    // generate a stage Id, this is a string of the form: 'stage-xxx'
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
                        IR.DockerImageDxAsset(url, dxRecord)
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
        val taskCleanedSourceCode = docker match {
            case IR.DockerImageDxAsset(url, dxRecord) =>
                val recordUrl = DxPath.dxRecordToURL(dxRecord)
                taskSourceCode.replaceAll(url, recordUrl)
            case _ => taskSourceCode
        }

        IR.Applet(task.name,
                  inputs,
                  outputs,
                  instanceType,
                  docker,
                  kind,
                  task,
                  taskCleanedSourceCode)
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

    private def findInputByName(call: CallNode, cVar: CVar) : Option[TaskCallInputExpressionNode] = {
        val callInputs = call.upstream.collect{
            case expr : TaskCallInputExpressionNode => expr
        }
        callInputs.find{
            case expr : TaskCallInputExpressionNode =>
                cVar.name == expr.identifier.localName.value
        }
    }

    // compile a call into a stage in an IR.Workflow
    private def compileCall(call: CallNode,
                            env : CallEnv) : IR.Stage = {
        // Find the callee
        val calleeName = call.callable.name
        val callee: IR.Callable = callables(calleeName)

        // Extract the input values/links from the environment
        val inputs: Vector[SArg] = callee.inputVars.map{ cVar =>
            findInputByName(call, cVar) match {
                case None =>
                    IR.SArgEmpty
                case Some(tcInput) if Utils.isExpressionConst(tcInput.womExpression) =>
                    IR.SArgConst(Utils.evalConst(tcInput.womExpression))
                case Some(tcInput) =>
                    val exprSourceString = tcInput.womExpression.sourceString
                    val lVar = env(exprSourceString)
                    lVar.sArg
            }
        }

        val stageName = call.identifier.localName.value
        IR.Stage(stageName, None, genStageId(), calleeName, inputs, callee.outputVars)
    }


    // Build an applet to evaluate a WDL workflow fragment
    //
    // Note: all the calls have the compulsory arguments, this has
    // been checked in the Validate step.
    private def compileWfFragment(block: Vector[GraphNode],
                                  env : CallEnv) : (IR.Stage, IR.Applet) = {
        val desc = WomPrettyPrint(block)
        Utils.error(desc)
        throw new NotImplementedError("Workflow fragments")
    }

    // Compile a workflow, having compiled the independent tasks.
    // This is a locked-down workflow, we have workflow level inputs
    // and outputs.
    private def compileWorkflowLocked(wf: WorkflowDefinition,
                                      wfInputs: Vector[(CVar, SArg)],
                                      subBlocks: Vector[Vector[GraphNode]])
            : (Vector[(IR.Stage, Option[IR.Applet])], CallEnv) =
    {
        Utils.trace(verbose.on, s"Compiling locked-down workflow ${wf.name}")

        var env : CallEnv = wfInputs.map { case (cVar,sArg) =>
            cVar.name -> LinkedVar(cVar, sArg)
        }.toMap

        // link together all the stages into a linear workflow
        val emptyStages = Vector.empty[(IR.Stage, Option[IR.Applet])]
        val allStageInfo = subBlocks.foldLeft(emptyStages) {case (accu, block) =>
            Block.isSimpleCall(block) match {
                case Some(call) =>
                    // The block contains exactly one call, with no extra declarations.
                    // All the variables are already in the environment, so there
                    // is no need to do any extra work. Compile directly into a workflow
                    // stage.
                    val stage = compileCall(call, env)

                    // Add bindings for the output variables. This allows later calls to refer
                    // to these results.
                    for (cVar <- stage.outputs) {
                        env = env + (call.identifier.localName.value ++ "." + cVar.name ->
                                         LinkedVar(cVar, IR.SArgLink(stage.stageName, cVar)))
                    }
                    accu :+ (stage, None)

                case None =>
                    // General case
                    val (stage, apl) = compileWfFragment(block, env)
                    for (cVar <- stage.outputs) {
                        env = env + (cVar.name ->
                                         LinkedVar(cVar, IR.SArgLink(stage.stageName, cVar)))
                    }
                    accu :+ (stage, Some(apl))
            }
        }
        (allStageInfo, env)
    }


    // TODO: Currently, expressions in the output section are not supported.
    // We would need an extra rewrite step to support this. At the moment,
    // it is simpler to ask the user to perform the rewrite herself.
    private def buildWorkflowOutput(outputNode: GraphOutputNode, env: CallEnv) : (CVar, SArg) = {
        def getSArgFromEnv(source: String) : SArg = {
            env.get(source) match {
                case None => throw new Exception(s"Sanity: could not find ${source} in the workflow environment")
                case Some(lVar) => lVar.sArg
            }
        }
        outputNode match {
            case PortBasedGraphOutputNode(id, womType, sourcePort) =>
                val cVar = CVar(id.workflowLocalName, womType, None)
                val source = sourcePort.name
                (cVar, getSArgFromEnv(source))
            case expr :ExpressionBasedGraphOutputNode =>
                if (!Block.isTrivialExpression(expr.womExpression))
                    throw new Exception(s"""|Expressions are not supported as workflow outputs,
                                            |${WomPrettyPrint.apply(expr)}.
                                            |Please rewrite""".stripMargin.replaceAll("\n", " "))
                val cVar = CVar(expr.graphOutputPort.name, expr.womType, None)
                val source = expr.womExpression.sourceString
                (cVar, getSArgFromEnv(source))
            case other =>
                throw new Exception(s"unhandled output ${other}")
        }
    }

    private def dbgPrint(inputNodes: Vector[GraphInputNode],   // inputs
                         subBlocks: Vector[Vector[GraphNode]], // blocks
                         outputNodes: Vector[GraphOutputNode]) // outputs
            : Unit = {
        System.out.println("Inputs [")
        inputNodes.foreach{ node =>
            val desc = WomPrettyPrint.apply(node)
            System.out.println(s"  ${desc}")
        }
        System.out.println("]")
        subBlocks.foreach{ nodes =>
            System.out.println("Block [")
            nodes.foreach{ node =>
                val desc = WomPrettyPrint.apply(node)
                System.out.println(s"  ${desc}")
            }
            System.out.println("]")
        }
        System.out.println("Output [")
        outputNodes.foreach{ node =>
            val desc = WomPrettyPrint.apply(node)
            System.out.println(s"  ${desc}")
        }
        System.out.println("]")
    }

    // Compile a (single) WDL workflow into a single dx:workflow.
    //
    // There are cases where we are going to need to generate dx:subworkflows.
    // This is not handled currently.
    def compileWorkflow(wf: WorkflowDefinition) : (IR.Workflow, Map[String, IR.Applet]) =
    {
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

        // Create a stage per call/scatter-block/declaration-block
        val (inputNodes, subBlocks, outputNodes) = Block.splitIntoBlocks(graph)
        if (verbose.on)
            dbgPrint(inputNodes, subBlocks, outputNodes)

        // compile into dx:workflow inputs
        val wfInputs:Vector[(CVar, SArg)] = inputNodes.map(buildWorkflowInput).toVector

        val (allStageInfo, env) = compileWorkflowLocked(wf, wfInputs, subBlocks)

        // compile into workflow outputs
        val wfOutputs = outputNodes.map(node => buildWorkflowOutput(node, env)).toVector

        val (stages, auxApplets) = allStageInfo.unzip
        val aApplets: Map[String, IR.Applet] =
            auxApplets
                .flatten
                .map(apl => apl.name -> apl).toMap
        val irwf = IR.Workflow(wf.name, wfInputs, wfOutputs, stages, locked, wfKind)
        (irwf, aApplets)
    }

    // Entry point for compiling tasks and workflows into IR
    def compileCallable(callable: Callable,
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
                val (irwf, aApplets) = compileWorkflow(wf)
                irwf
            case x =>
                throw new Exception(s"""|Can't compile: ${callable.name}, class=${callable.getClass}
                                        |${x}
                                        |""".stripMargin.replaceAll("\n", " "))
        }
    }
}


object GenerateIR {
    def sortByDependencies(allCallables: Vector[Callable]) : Vector[Callable] = {
        // figure out, for each element, what it depends on.
        // tasks don't depend on anything else. They are at the bottom of the dependency
        // tree.
        val immediateDeps : Map[String, Set[String]] = allCallables.map{ callable =>
            val deps = callable match {
                case _ : ExecutableTaskDefinition => Set.empty[String]
                case _ : CallableTaskDefinition => Set.empty[String]
                case wf: WorkflowDefinition =>
                    val nodes = wf.innerGraph.allNodes
                    val callNodes : Vector[CallNode] = nodes.collect{
                        case cNode: CallNode => cNode
                    }.toVector
                    callNodes.map{ cNode =>  cNode.callable.name }.toSet
                case other =>
                    throw new Exception(s"Don't know how to deal with class ${other.getClass.getSimpleName}")
            }
            callable.name -> deps
        }.toMap

        // Find executables such that all of their dependencies are
        // satisfied. These can be compiled.
        def next(callables: Vector[Callable],
                 ready: Vector[Callable]) : Vector[Callable] = {
            val readyNames = ready.map(_.name).toSet
            val satisfiedCallables = callables.filter{ c =>
                val deps = immediateDeps(c.name)
                deps.subsetOf(readyNames)
            }
            if (satisfiedCallables.isEmpty)
                throw new Exception("Sanity: cannot find the next callable to compile.")
            satisfiedCallables
        }

        var accu = Vector.empty[Callable]
        var crnt = allCallables
        while (!crnt.isEmpty) {
            val execsToCompile = next(crnt, accu)
            accu = accu ++ execsToCompile
            val alreadyCompiled: Set[String] = accu.map(_.name).toSet
            crnt = crnt.filter{ exec => !(alreadyCompiled contains exec.name) }
        }
        assert(accu.length == allCallables.length)
        accu
    }

    // Entrypoint
    def apply(womBundle : wom.executable.WomBundle,
              allSources: Map[String, WorkflowSource],
              language: Language.Value,
              locked: Boolean,
              verbose: Verbose) : IR.Bundle = {
        Utils.trace(verbose.on, s"IR pass")
        Utils.traceLevelInc()

        // Scan the source files and extract the tasks. It is hard
        // to generate WDL from the abstract syntax tree (AST). One
        // issue is that tabs and special characters have to preserved.
        // There is no built-in method for this.
        val taskDir = allSources.foldLeft(Map.empty[String, String]) {
            case (accu, (filename, srcCode)) =>
                val d = ParseWomSourceFile.scanForTasks(language, srcCode)
                accu ++ d
        }

        Utils.trace(verbose.on,
                    s" sortByDependencies ${womBundle.allCallables.values.map{_.name}}")
        val order : Vector[Callable] = sortByDependencies(womBundle.allCallables.values.toVector)
        Utils.trace(verbose.on,
                    s"order =${order.map{_.name}}")

        // compile the tasks/workflows from bottom to top.
        val allCallables = order.foldLeft(Map.empty[String, IR.Callable]) {
            case (accu, callable) =>
                val gir = GenerateIR(accu, locked, IR.WorkflowKind.TopLevel, verbose)
                val exec = gir.compileCallable(callable, taskDir)
                accu + (callable.name -> exec)
        }.toMap

        // We already compiled all the individual wdl:tasks and
        // wdl:workflows, let's find the entrypoint.
        val primary = womBundle.primaryCallable.map{ callable =>
            allCallables(callable.name)
        }

        Utils.traceLevelDec()
        IR.Bundle(primary, allCallables, order.map{_.name})
    }
}
