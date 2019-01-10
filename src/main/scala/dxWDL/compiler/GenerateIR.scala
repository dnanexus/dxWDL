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

private case class NameBox(verbose: Verbose) {
    // never allow the reserved words
    private var namesUsed:Set[String] = Set.empty

    def chooseUniqueName(baseName: String) : String = {
        if (!(namesUsed contains baseName)) {
            namesUsed += baseName
            return baseName
        }

        // Add [1,2,3 ...] to the original name, until finding
        // an unused variable name
        for (i <- 1 to Utils.MAX_NUM_RENAME_TRIES) {
            val tentative = s"${baseName}${i}"
            if (!(namesUsed contains tentative)) {
                namesUsed += tentative
                return tentative
            }
        }
        throw new Exception(s"Could not find a unique name for ${baseName}")
    }
}

case class GenerateIR(callables: Map[String, IR.Callable],
                      language: Language.Value,
                      locked: Boolean,
                      wfKind: IR.WorkflowKind.Value,
                      verbose: Verbose) {
    val verbose2 : Boolean = verbose.keywords contains "GenerateIR"
    private val nameBox = NameBox(verbose)

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

    // create a unique name for a workflow fragment
    private var fragNum = 0
    private def genFragId() : String = {
        fragNum += 1
        fragNum.toString
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
                    IR.AppletKindTask(task)
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
        IR.Stage(stageName, genStageId(), calleeName, inputs, callee.outputVars)
    }


    // Create a human readable name for a block of statements
    //
    // 1. Ignore all declarations
    // 2. If there is a scatter/if, use that
    // 3. Otherwise, there must be at least one call. Use the first one.
    private def createBlockName(block: Block) : String = {
        val coreStmts = block.nodes.filter{
            case _: ScatterNode => true
            case _: ConditionalNode => true
            case _: CallNode => true
            case _ => false
        }
        if (coreStmts.isEmpty)
            return s"eval_${genFragId()}"

        coreStmts.head match {
            case ssc : ScatterNode =>
                val ids = ssc.scatterVariableNodes.map{
                    svn => svn.identifier.localName.value
                }.mkString(",")
                s"scatter (${ids})"
            case cond : ConditionalNode =>
                s"if (${cond.conditionExpression.womExpression.sourceString})"
            case call : CallNode =>
                // TODO: What happens with aliases?
                // call.fullyQualifiedName.value
                call.identifier.localName.value
            case _ =>
                throw new Exception("sanity")
        }
    }

    // Check if the environment has a variable with a binding for
    // a fully-qualified name. For example, if fqn is "A.B.C", then
    // look for "A.B.C", "A.B", or "A", in that order.
    //
    // If the environment has a pair "p", then we want to be able to
    // to return "p" when looking for "p.left" or "p.right".
    //
    private def lookupInEnv(fqn: String, env: CallEnv) : Option[(String, LinkedVar)] = {
        if (env contains fqn) {
            // exact match
            Some(fqn, env(fqn))
        } else {
            // A.B.C --> A.B
            val pos = fqn.lastIndexOf(".")
            if (pos < 0) None
            else {
                val lhs = fqn.substring(0, pos)
                lookupInEnv(lhs, env)
            }
        }
    }

    // Find the closure of a block. All the variables defined earlier
    // that are required for the calculation.
    private def blockClosure(block: Block,
                             env : CallEnv,
                             dbg: String) : CallEnv = {
        val inputPortsPerNode = block.nodes.map{ _.inputPorts }
        val allInputPorts : Set[GraphNodePort.InputPort] =
            inputPortsPerNode.foldLeft(Set.empty[GraphNodePort.InputPort]) {
                case (accu, inputs) => accu ++ inputs
            }

        // ignore references to variables not defined in the environment. These
        // have to be block internal variables.
        val closure = allInputPorts.flatMap { iPort =>
            // TODO: How do we get the fully qualified name from the input port?
            lookupInEnv(iPort.name, env)
        }.toMap

/*        if (verbose2) {
            val xtrnDesc = allInputPorts.map{
                WomPrettyPrint.apply(_)
            }.mkString(",")
        Utils.trace(verbose2,
                    s"""|blockClosure
                        |   stage: ${dbg}
                        |   external: ${xtrnDesc}
                        |   env: ${env.keys}
                        |   found: ${closure.keys}""".stripMargin)
 }*/
        closure
    }

    // Figure out all the outputs from a sequence of WDL statements.
    //
    // Note: The type outside a block is *different* than the type in
    // the block.  For example, 'Int x' declared inside a scatter, is
    // 'Array[Int] x' outside the scatter.
    private def blockOutputs(block: Block,
                             nextNodes: Set[GraphNode]) : Vector[CVar] = {
        // figure what the rest of the graph depends on
        val dependencies = nextNodes.flatMap{ _.upstream }

        // nodes required downstream
        val xtrnNodes : Set[GraphNode] = dependencies.intersect(block.nodes.toSet)

        // Get the output ports for these nodes
        val xtrnPorts : Set[GraphNodePort.OutputPort] =
            xtrnNodes.map{ _.outputPorts}
                .foldLeft(Set.empty[GraphNodePort.OutputPort]) {
                case (accu, outputs) => accu ++ outputs }

        // create a cVar definition from each WDL output port. The dx:stage
        // will output these cVars.
        val cVars = xtrnPorts.map{
            case gnop : GraphNodePort.GraphNodeOutputPort =>
                CVar(gnop.identifier.localName.value, gnop.womType, None)
            case ebop : GraphNodePort.ExpressionBasedOutputPort =>
                CVar(ebop.identifier.localName.value, ebop.womType, None)
            case other =>
                throw new Exception(s"unhandled case ${other.getClass}")
        }.toVector

/*        if (verbose2) {
            val dependenciesDesc = dependencies.map{
                "    " + WomPrettyPrint.apply(_)
            }.mkString("\n")
            val portDesc = xtrnPorts.map{
                "    " + WomPrettyPrint.apply(_)
            }.mkString("\n")
            val cVarDesc = cVars.mkString(",")
            Utils.trace(verbose2,
                        s"""|blockOutputs
                            |  dependencies: [
                            |${dependenciesDesc}
                            |  ]
                            |  ports: [
                            |${portDesc}
                            |  ]
                            |  cVars: ${cVarDesc}""".stripMargin)
        }*/
        cVars
    }

    // Build an applet to evaluate a WDL workflow fragment
    //
    // Note: all the calls have the compulsory arguments, this has
    // been checked in the Validate step.
    private def compileWfFragment(block: Block,
                                  blockNum : Int,
                                  nextNodes : Set[GraphNode],
                                  env : CallEnv,
                                  wfName : String,
                                  wfSource_WDLv1 : WdlCodeSnippet) : (IR.Stage, IR.Applet) = {
        val baseName = createBlockName(block)
        val stageName = nameBox.chooseUniqueName(baseName)
        Utils.trace(verbose.on, s"--- Compiling wfFragment ${stageName}")

        // Figure out the closure required for this block, out of the
        // environment
        val closure = blockClosure(block, env, stageName)
        val inputVars: Vector[CVar] = closure.map {
            case (fqn, LinkedVar(cVar, _)) =>
                cVar.copy(name = fqn)
        }.toVector


        // TODO create input variable definitions
        //
        // mapping of fully qualified names, to short names. For example:
        //   mul.result ---> result
        //   add.result --> result1
        //
        // Currently: a reversible conversion mul.result --> mul___result. This
        // assumes the ___ symbol is not used anywhere in the original WDL script.
        val fqnDictTypes = inputVars.map{ cVar => cVar.dxVarName -> cVar.womType}.toMap

        // To figure out the block outputs, we need the subsequent nodes.
        val outputVars = blockOutputs(block, nextNodes)

        // Make a list of all task/workflow calls made inside the block. We will need to link
        // to the equivalent dx:applets and dx:workflows.
        val allCallNames = Block.deepFindCalls(block.nodes).map{ cNode =>
            cNode.callable.name
        }.toVector

        // generate a new WDL script just for this sub-block
        val wdlCode = wfSource_WDLv1.value
        //Utils.trace(verbose2, wdlCode)

        val applet = IR.Applet(s"${wfName}_${stageName}",
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindWfFragment(allCallNames, blockNum, fqnDictTypes),
                               wdlCode)
        val sArgs : Vector[SArg] = closure.map {
            case (_, LinkedVar(_, sArg)) => sArg
        }.toVector

        Utils.trace(verbose.on, "---")
        (IR.Stage(stageName, genStageId(), applet.name, sArgs, outputVars),
         applet)
    }

    // Compile a workflow, having compiled the independent tasks.
    // This is a locked-down workflow, we have workflow level inputs
    // and outputs.
    private def compileWorkflowLocked(wf: WorkflowDefinition,
                                      wfSourceStandAlone : WdlCodeSnippet,
                                      wfInputs: Vector[(CVar, SArg)],
                                      subBlocks: Vector[Block],
                                      outputNodes: Vector[GraphOutputNode])
            : (Vector[(IR.Stage, Option[IR.Applet])], CallEnv) =
    {
        Utils.trace(verbose.on, s"Compiling locked-down workflow ${wf.name}")

        var env : CallEnv = wfInputs.map { case (cVar,sArg) =>
            cVar.name -> LinkedVar(cVar, sArg)
        }.toMap
        var allStageInfo = Vector.empty[(IR.Stage, Option[IR.Applet])]
        var remainingBlocks = subBlocks

        // link together all the stages into a linear workflow
        for (blockNum <- 0 to (subBlocks.length -1)) {
            val block = remainingBlocks.head
            remainingBlocks = remainingBlocks.tail

            val (stage, aplOpt) = Block.isSimpleCall(block) match {
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
                    (stage, None)

                case None =>
                    // General case
                    val nextNodes : Set[GraphNode] = remainingBlocks.foldLeft(Set.empty[GraphNode]) {
                        case (accu, b) => accu ++ b.nodes.toSet
                    }
                    val (stage, apl) = compileWfFragment(block, blockNum, nextNodes ++ outputNodes,
                                                         env, wf.name, wfSourceStandAlone)
                    for (cVar <- stage.outputs) {
                        env = env + (cVar.name ->
                                         LinkedVar(cVar, IR.SArgLink(stage.stageName, cVar)))
                    }
                    (stage, Some(apl))
            }
            allStageInfo :+= (stage, aplOpt)
        }
        (allStageInfo, env)
    }


    // TODO: Currently, expressions in the output section are not supported.
    // We would need an extra rewrite step to support this. At the moment,
    // it is simpler to ask the user to perform the rewrite herself.
    private def buildWorkflowOutput(outputNode: GraphOutputNode, env: CallEnv) : (CVar, SArg) = {
        def getSArgFromEnv(source: String) : SArg = {
            env.get(source) match {
                case None =>
                    val envDesc = env.mkString("\n")
                    Utils.error(s"""|env=[
                                    |${envDesc}
                                    |]""".stripMargin)
                    throw new Exception(s"Sanity: could not find ${source} in the workflow environment")
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

    // Compile a (single) WDL workflow into a single dx:workflow.
    //
    // There are cases where we are going to need to generate dx:subworkflows.
    // This is not handled currently.
    def compileWorkflow(wf: WorkflowDefinition,
                        wfSource: String) : (IR.Workflow, Vector[IR.Applet]) =
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

        val wdlCodeGen = new WdlCodeGen(verbose)

        // Make a list of all task/workflow calls made inside the block. We will need to link
        // to the equivalent dx:applets and dx:workflows.
        val callablesUsedInWorkflow : Vector[IR.Callable] =
            graph.allNodes.collect {
                case cNode : CallNode =>
                    callables(cNode.callable.name)
            }.toVector
        val wfSourceStandAlone = wdlCodeGen.standAloneWorkflow(wfSource, callablesUsedInWorkflow, language)

        // compile into dx:workflow inputs
        val wfInputs:Vector[(CVar, SArg)] = inputNodes.map(buildWorkflowInput).toVector

        val (allStageInfo, env) = compileWorkflowLocked(wf, wfSourceStandAlone, wfInputs, subBlocks, outputNodes)

        // compile into workflow outputs
        val wfOutputs = outputNodes.map(node => buildWorkflowOutput(node, env)).toVector

        val (stages, auxApplets) = allStageInfo.unzip
        val irwf = IR.Workflow(wf.name, wfInputs, wfOutputs, stages, locked, wfKind)
        (irwf, auxApplets.flatten)
    }

    // Entry point for compiling tasks and workflows into IR
    def compileCallable(callable: Callable,
                        taskDir: Map[String, String],
                        workflowDir: Map[String, String]) : (IR.Callable, Vector[IR.Applet]) = {
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
                (compileTask2(task), Vector.empty)
            case task : CallableTaskDefinition =>
                (compileTask2(task), Vector.empty)
            case wf: WorkflowDefinition =>
                workflowDir.get(wf.name) match {
                    case None =>
                        throw new Exception(s"Did not find sources for workflow ${wf.name}")
                    case Some(wfSource) =>
                        compileWorkflow(wf, wfSource)
                }
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
        val workflowDir = allSources.foldLeft(Map.empty[String, String]) {
            case (accu, (filename, srcCode)) =>
                ParseWomSourceFile.scanForWorkflow(language, srcCode) match {
                    case None =>
                        accu
                    case Some((wfName, wfSource)) =>
                        accu + (wfName -> wfSource)
                }
        }

        Utils.trace(verbose.on,
                    s" sortByDependencies ${womBundle.allCallables.values.map{_.name}}")
        val depOrder : Vector[Callable] = sortByDependencies(womBundle.allCallables.values.toVector)
        Utils.trace(verbose.on,
                    s"depOrder =${depOrder.map{_.name}}")

        // compile the tasks/workflows from bottom to top.
        var allCallables = Map.empty[String, IR.Callable]
        var allCallablesSorted = Vector.empty[IR.Callable]

        for (callable <- depOrder) {
            val gir = GenerateIR(allCallables, language, locked, IR.WorkflowKind.TopLevel, verbose)
            val (exec, auxApplets) = gir.compileCallable(callable, taskDir, workflowDir)

            allCallables = allCallables ++ (auxApplets.map{ apl => apl.name -> apl}.toMap)
            allCallables = allCallables + (exec.name -> exec)

            // Add the auxiliary applets while preserving the dependency order
            allCallablesSorted = allCallablesSorted ++ auxApplets :+ exec
        }

        // We already compiled all the individual wdl:tasks and
        // wdl:workflows, let's find the entrypoint.
        val primary = womBundle.primaryCallable.map{ callable =>
            allCallables(callable.name)
        }
        val allCallablesSorted2 = allCallablesSorted.map{_.name}
        Utils.trace(verbose.on, s"allCallablesSorted=${allCallablesSorted2}")
        assert(allCallables.size == allCallablesSorted2.size)

        Utils.traceLevelDec()
        IR.Bundle(primary, allCallables, allCallablesSorted2)
    }
}
