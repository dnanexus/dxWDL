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

            case OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
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
            case FixedInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
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
    def buildWorkflowInput(input: GraphInputNode) : CVar = {
        input match {
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
    }

    // In a call like:
    //   call lib.native_mk_list as mk_list {
    //     input: a=x, b=5
    //   }
    // it maps callee input <a> to expression <x>. The expressions are
    // trivial because this is a case where we can directly call an applet.
    //
    // Note that in an expression like:
    //    call volume { input: i = 10 }
    // the "i" parameter, under WDL draft-2, is compiled as "volume.i"
    // under WDL version 1.0, it is compiled as "i"
    //
    private def findInputByName(callInputs: Seq[TaskCallInputExpressionNode],
                                cVar: CVar) : Option[TaskCallInputExpressionNode] = {
        val retval = callInputs.find{
            case expr : TaskCallInputExpressionNode =>
                //Utils.trace(verbose2, s"compare ${cVar.name} to ${expr.identifier.localName.value}")
                cVar.name == Utils.getUnqualifiedName(expr.identifier.localName.value)
        }
        retval
    }

    // compile a call into a stage in an IR.Workflow
    private def compileCall(call: CallNode,
                            env : CallEnv,
                            locked: Boolean) : IR.Stage = {
        // Find the callee
        val calleeName = Utils.getUnqualifiedName(call.callable.name)
        val callee: IR.Callable = callables(calleeName)
        val callInputs: Seq[TaskCallInputExpressionNode] = call.upstream.collect{
            case tcExpr : TaskCallInputExpressionNode => tcExpr
        }.toSeq

        // Extract the input values/links from the environment
        val inputs: Vector[SArg] = callee.inputVars.map{ cVar =>
            findInputByName(callInputs, cVar) match {
                case None if Utils.isOptional(cVar.womType) =>
                    // optional argument that is not provided
                    IR.SArgEmpty
                case None if locked =>
                    val envDbg = env.map{ case (name, lVar) =>
                        s"  ${name} -> ${lVar.sArg}"
                    }.mkString("\n")
                    Utils.trace(verbose.on, s"""|env =
                                                |${envDbg}""".stripMargin)
                    throw new Exception(
                        s"""|input <${cVar.name}, ${cVar.womType}> to call <${call.fullyQualifiedName}>
                            |is unspecified""".stripMargin.replaceAll("\n", " "))
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
        val name = coreStmts.head match {
            case ssc : ScatterNode =>
                val ids = ssc.scatterVariableNodes.map{
                    svn => svn.identifier.localName.value
                }.mkString(",")
                s"scatter (${ids})"
            case cond : ConditionalNode =>
                s"if (${cond.conditionExpression.womExpression.sourceString})"
            case call : CallNode =>
                call.identifier.localName.value
            case _ =>
                throw new Exception("sanity")
        }
        s"wfFragment ${name}"
    }

    // Check if the environment has a variable with a binding for
    // a fully-qualified name. For example, if fqn is "A.B.C", then
    // look for "A.B.C", "A.B", or "A", in that order.
    //
    // If the environment has a pair "p", then we want to be able to
    // to return "p" when looking for "p.left" or "p.right".
    //
    private def lookupInEnvInner(fqn: String, env: CallEnv) : Option[(String, LinkedVar)] = {
        if (env contains fqn) {
            // exact match
            Some(fqn, env(fqn))
        } else {
            // A.B.C --> A.B
            val pos = fqn.lastIndexOf(".")
            if (pos < 0) None
            else {
                val lhs = fqn.substring(0, pos)
                lookupInEnvInner(lhs, env)
            }
        }
    }

    // Lookup in the environment. Provide a human readable error message
    // if the fully-qualified-name is not found.
    private def lookupInEnv(fqn: String,
                            womType: WomType,
                            env: CallEnv) : Option[(String, LinkedVar)] = {
        lookupInEnvInner(fqn, env) match {
            case None if Utils.isOptional(womType) =>
                None
            case None =>
                // A missing compulsory argument
                Utils.warning(verbose,
                              s"Missing argument ${fqn}, it will have to be provided at runtime")
                None
            case Some((name, lVar)) =>
                Some((name, lVar))
        }
    }

    // Find the closure of a block. All the variables defined earlier
    // that are required for the calculation.
    private def blockClosure(block: Block,
                             env : CallEnv,
                             dbg: String) : CallEnv = {
/*        if (verbose2) {
            val blockNodesDbg = block.nodes.map{
                "    " + WomPrettyPrint.apply(_)
            }.mkString("\n")
            Utils.trace(verbose2,
                        s"""|blockClosure nodes [
                            |${blockNodesDbg}
                            |]
                            |""".stripMargin)
        }*/
        val allInputs = Block.closure(block)
        val closure = allInputs.flatMap { case (name, womType) =>
            lookupInEnv(name, womType, env)
        }.toMap

/*        Utils.trace(verbose2,
                    s"""|blockClosure II
                        |   stage: ${dbg}
                        |   external: ${allInputs.mkString(",")}
                        |   found: ${closure.keys}""".stripMargin) */
        closure
    }

    // Build an applet to evaluate a WDL workflow fragment
    //
    // Note: all the calls have the compulsory arguments, this has
    // been checked in the Validate step.
    //
    // The [blockPath] argument keeps track of which block this fragment represents.
    // A top level block is a number. A sub-block of a top-level block is a vector of two
    // numbers, etc.
    private def compileWfFragment(block: Block,
                                  blockPath: Vector[Int],
                                  env : CallEnv,
                                  wfName : String,
                                  wfSourceStandAlone : WdlCodeSnippet) : (IR.Stage, IR.Applet) = {
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

        // A reversible conversion mul.result --> mul___result. This
        // assumes the '___' symbol is not used anywhere in the original WDL script.
        //
        // This is a simplifying assumption, that is hopefully sufficient. It disallows
        // users from using variables with the ___ character sequence.
        val fqnDictTypes = inputVars.map{ cVar => cVar.dxVarName -> cVar.womType}.toMap

        // Figure out the block outputs
        val outputs : Map[String, WomType] = Block.outputs(block)

        // create a cVar definition from each block output. The dx:stage
        // will output these cVars.
        val outputVars = outputs.map{case (fqn, womType) =>
            CVar(fqn, womType, None)
        }.toVector

        val callable : IR.Callable = match {
            case Block.AllExpressions |
                    Block.CallWithEval(_) |
                    Block.Cond(_) |
                    Block.Scatter(_) =>
                // A simple block with no nested sub-blocks, and a single call.
                //
                // Make a list of all task/workflow calls made inside the block. We will need to link
                // to the equivalent dx:applets and dx:workflows.
                val allCallNames = Block.deepFindCalls(block.nodes).map{ cNode =>
                    Utils.getUnqualifiedName(cNode.callable.name)
                }.toVector
                assert(allCallNames == 1)
                IR.Applet(s"${wfName}_${stageName}",
                          inputVars,
                          outputVars,
                          calcInstanceType(None),
                          IR.DockerImageNone,
                          IR.AppletKindWfFragment(allCallNames, blockPath, fqnDictTypes),
                          wfSourceStandAlone.value)

            case Block.CondWithNesting(_) |
                    Block.ScatterWithNesting(_) =>
                // A block complex enough to require a workflow.
                // Recursively call into the compile-a-workflow method, and
                // get a locked subworkflow.
                val (inputNodes, subBlocks, outputNodes) = Block.split(/*graph*/ block, wfSourceStandAlone.value)
                val (subWf,auxApplet, _ ) = compileWorkflowLocked(wf,
                                                                  inputNodes: Vector[GraphInputNode],
                                                                  outputNodes: Vector[GraphOutputNode],
                                                                  wfSourceStandAlone,
                                                                  blockPath,
                                                                  subBlocks)
                subwf
        }
        val sArgs : Vector[SArg] = closure.map {
            case (_, LinkedVar(_, sArg)) => sArg
        }.toVector

        Utils.trace(verbose.on, "---")
        (IR.Stage(stageName, genStageId(), callable.name, sArgs, outputVars),
         applet)
    }

    // Assemble the backbone of a workflow, having compiled the
    // independent tasks.  This is shared between locked and unlocked
    // workflows. At this point we we have workflow level inputs and
    // outputs.
    private def assembleBackbone(wf: WorkflowDefinition,
                                 wfSourceStandAlone : WdlCodeSnippet,
                                 wfInputs: Vector[(CVar, SArg)],
                                 blockPath: Vector[Int],
                                 subBlocks: Vector[Block],
                                 locked: Boolean)
            : (Vector[(IR.Stage, Option[IR.Applet])], CallEnv) =
    {
        Utils.trace(verbose.on, s"Assembling workfow backbone ${wf.name}")

        var env : CallEnv = wfInputs.map { case (cVar,sArg) =>
            cVar.name -> LinkedVar(cVar, sArg)
        }.toMap
        var allStageInfo = Vector.empty[(IR.Stage, Option[IR.Applet])]
        var remainingBlocks = subBlocks

        // link together all the stages into a linear workflow
        for (blockNum <- 0 to (subBlocks.length -1)) {
            val block = remainingBlocks.head
            remainingBlocks = remainingBlocks.tail

            val (_, category) = Block.categorize(block)
            val (stage, aplOpt) = category match {
                case Block.CallDirect(call) =>
                    // The block contains exactly one call, with no extra declarations.
                    // All the variables are already in the environment, so there
                    // is no need to do any extra work. Compile directly into a workflow
                    // stage.
                    Utils.trace(verbose.on, s"--- Compiling call ${call.callable.name} as stage")
                    val stage = compileCall(call, env, locked)

                    // Add bindings for the output variables. This allows later calls to refer
                    // to these results.
                    for (cVar <- stage.outputs) {
                        val fqn = call.identifier.localName.value ++ "." + cVar.name
                        val cVarFqn = cVar.copy(name = fqn)
                        env = env + (fqn -> LinkedVar(cVarFqn, IR.SArgLink(stage.stageName, cVar)))
                    }
                    (stage, None)

                case _ =>
                    //     A simple block that requires just one applet,
                    // OR: A complex block that needs a subworkflow
                    val (stage, apl) = compileWfFragment(block,
                                                         blockPath :+ blockNum,
                                                         env, wf.name, wfSourceStandAlone)
                    for (cVar <- stage.outputs) {
                        env = env + (cVar.name ->
                                         LinkedVar(cVar, IR.SArgLink(stage.stageName, cVar)))
                    }
                    (stage, Some(apl))
            }
            allStageInfo :+= (stage, aplOpt)
        }

        val stagesDbgStr = allStageInfo.map{ case (stage,_) =>
            s"Stage(${stage.stageName}, callee=${stage.calleeName})"
        }.mkString("\n")
        Utils.trace(verbose2, s"""|stages =
                                  |${stagesDbgStr}
                                  |""".stripMargin)
        (allStageInfo, env)
    }


    private def buildSimpleWorkflowOutput(outputNode: GraphOutputNode, env: CallEnv) : (CVar, SArg) = {
        def getSArgFromEnv(source: String) : SArg = {
            env.get(source) match {
                case None =>
                    val envDesc = env.mkString("\n")
                    Utils.trace(verbose.on, s"""|env=[
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
            case expr :ExpressionBasedGraphOutputNode if (Block.isTrivialExpression(expr.womExpression)) =>
                val cVar = CVar(expr.graphOutputPort.name, expr.womType, None)
                val source = expr.womExpression.sourceString
                (cVar, getSArgFromEnv(source))
            case expr :ExpressionBasedGraphOutputNode =>
                // An expression that requires evaluation
                throw new Exception(s"Internal error: non trivial expressions are handled elsewhere ${expr}")
            case other =>
                throw new Exception(s"unhandled output ${other}")
        }
    }


    // Create a preliminary applet to handle workflow input/outputs. This is
    // used only in the absence of workflow-level inputs/outputs.
    private def buildCommonApplet(wfName: String,
                                  wfSourceStandAlone: WdlCodeSnippet,
                                  inputVars: Vector[CVar]) : (IR.Stage, IR.Applet) = {
        val appletName = s"${wfName}_${Utils.COMMON}"
        Utils.trace(verbose.on, s"Compiling common applet ${appletName}")

        val WdlCodeSnippet(wdlCode) = wfSourceStandAlone
        val outputVars: Vector[CVar] = inputVars
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindWfInputs,
                               wdlCode)

        val sArgs: Vector[SArg] = inputVars.map{ _ => IR.SArgEmpty}.toVector
        (IR.Stage(Utils.COMMON, genStageId(), applet.name, sArgs, outputVars),
         applet)
    }

    // There are two reasons to be build a special output section:
    // 1. Locked workflow: some of the workflow outputs are expressions.
    //    We need an extra applet+stage to evaluate them.
    // 2. Unlocked workflow: there are no workflow outputs, so we create
    //    them artificially with a separate stage that collects the outputs.
    private def buildOutputStage(wfName: String,
                                 wfSourceStandAlone : WdlCodeSnippet,
                                 outputNodes : Vector[GraphOutputNode],
                                 env: CallEnv)
            : (IR.Stage, IR.Applet) = {
        // Figure out what variables from the environment we need to pass
        // into the applet.
        val closure = Block.outputClosure(outputNodes)

        val inputVars : Vector[LinkedVar] = closure.map{ name =>
            val lVar = env.find{ case (key,_) => key == name } match {
                case None => throw new Exception(s"could not find variable ${name} in the environment")
                case Some((_,lVar)) => lVar
            }
            lVar
        }.toVector
        Utils.trace(verbose.on, s"inputVars=${inputVars.map(_.cVar)}")

        // build definitions of the output variables
        val outputVars: Vector[CVar] = outputNodes.map {
            case PortBasedGraphOutputNode(id, womType, sourcePort) =>
                CVar(id.workflowLocalName, womType, None)
            case expr :ExpressionBasedGraphOutputNode =>
                CVar(expr.graphOutputPort.name, expr.womType, None)
            case other =>
                throw new Exception(s"unhandled output ${other}")
        }.toVector

        val WdlCodeSnippet(wdlCode) = wfSourceStandAlone
        val appletName = s"${wfName}_${Utils.OUTPUT_SECTION}"
        val applet = IR.Applet(appletName,
                               inputVars.map(_.cVar),
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindWfOutputs,
                               wdlCode)

        // define the extra stage we add to the workflow
        (IR.Stage(Utils.OUTPUT_SECTION, genStageId(Some("last")), applet.name,
                  inputVars.map(_.sArg), outputVars),
         applet)
    }

     // Create an applet to reorganize the output files. We want to
    // move the intermediate results to a subdirectory.  The applet
    // needs to process all the workflow outputs, to find the files
    // that belong to the final results.
    private def buildReorgStage(wfName: String,
                                wfSourceStandAlone : WdlCodeSnippet,
                                wfOutputs: Vector[(CVar, SArg)]) : (IR.Stage, IR.Applet) = {
        val appletName = s"${wfName}_${Utils.REORG}"
        Utils.trace(verbose.on, s"Compiling output reorganization applet ${appletName}")

        // We need minimal compute resources, use the default instance type
        val WdlCodeSnippet(wdlCode) = wfSourceStandAlone
        val applet = IR.Applet(appletName,
                               wfOutputs.map{ case (cVar, _) => cVar },
                               Vector.empty,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindWorkflowOutputReorg,
                               wdlCode)

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = wfOutputs.map{ case (_, sArg) => sArg }.toVector

        (IR.Stage(Utils.REORG, genStageId(Some("reorg")), applet.name, inputs, Vector.empty[CVar]),
         applet)
    }

    private def compileWorkflowLocked(wf: WorkflowDefinition,
                                      inputNodes: Vector[GraphInputNode],
                                      outputNodes: Vector[GraphOutputNode],
                                      wfSourceStandAlone : WdlCodeSnippet,
                                      blockPath: Vector[Int],
                                      subBlocks : Vector[Block]) :
            (IR.Workflow, Vector[IR.Applet], Vector[(CVar, SArg)]) =
    {
        val wfInputs:Vector[(CVar, SArg)] = inputNodes.map{
            case iNode =>
                val cVar = buildWorkflowInput(iNode)
                (cVar, IR.SArgWorkflowInput(cVar))
        }.toVector

        val (allStageInfo, env) = assembleBackbone(wf, wfSourceStandAlone, wfInputs,
                                                   blockPath, subBlocks, true)
        val (stages, auxApplets) = allStageInfo.unzip

        // Handle outputs that are constants or variables, we can output them directly
        if (outputNodes.forall(Block.isSimpleOutput)) {
            val simpleWfOutputs = outputNodes.map(node => buildSimpleWorkflowOutput(node, env)).toVector
            val irwf = IR.Workflow(wf.name, wfInputs, simpleWfOutputs, stages, true)
            (irwf, auxApplets.flatten, simpleWfOutputs)
        } else {
            // Some of the outputs are expressions. We need an extra applet+stage
            // to evaluate them.
            val (outputStage, outputApplet) = buildOutputStage(wf.name, wfSourceStandAlone, outputNodes, env)
            val wfOutputs = outputStage.outputs.map{ cVar =>
                (cVar, IR.SArgLink(outputStage.stageName, cVar))
            }
            val irwf = IR.Workflow(wf.name, wfInputs, wfOutputs, stages :+ outputStage, true)
            (irwf, auxApplets.flatten :+ outputApplet, wfOutputs)
        }
    }


    private def compileWorkflowRegular(wf: WorkflowDefinition,
                                       inputNodes: Vector[GraphInputNode],
                                       outputNodes: Vector[GraphOutputNode],
                                       wfSourceStandAlone : WdlCodeSnippet,
                                       blockPath: Vector[Int],
                                       subBlocks : Vector[Block])
            : (IR.Workflow, Vector[IR.Applet], Vector[(CVar, SArg)]) =
    {
        // Create a special applet+stage for the inputs. This is a substitute for
        // workflow inputs. We now call the workflow inputs, "fauxWfInputs". This is because
        // they are references to the outputs of this first applet.

        // compile into dx:workflow inputs
        val wfInputDefs:Vector[CVar] = inputNodes.map{
            case iNode => buildWorkflowInput(iNode)
        }.toVector
        val (commonStg, commonApplet) = buildCommonApplet(wf.name, wfSourceStandAlone, wfInputDefs)
        val fauxWfInputs:Vector[(CVar, SArg)] = commonStg.outputs.map{
            case cVar: CVar =>
                val sArg = IR.SArgLink(commonStg.stageName, cVar)
                (cVar, sArg)
        }.toVector

        val (allStageInfo, env) = assembleBackbone(wf, wfSourceStandAlone, fauxWfInputs,
                                                   blockPath, subBlocks, false)
        val (stages: Vector[IR.Stage], auxApplets) = allStageInfo.unzip

        // convert the outputs into an applet+stage
        val (outputStage, outputApplet) = buildOutputStage(wf.name, wfSourceStandAlone,
                                                          outputNodes, env)

        val wfInputs = wfInputDefs.map{ cVar =>
            (cVar, IR.SArgEmpty)
        }
        val wfOutputs = outputStage.outputs.map{ cVar =>
            (cVar, IR.SArgLink(outputStage.stageName, cVar))
        }
        val irwf = IR.Workflow(wf.name,
                               wfInputs.toVector,
                               wfOutputs.toVector,
                               commonStg +: stages :+ outputStage,
                               false)
        (irwf, commonApplet +: auxApplets.flatten :+ outputApplet, wfOutputs)
    }


    // Compile a (single) WDL workflow into a single dx:workflow.
    //
    // There are cases where we are going to need to generate dx:subworkflows.
    // This is not handled currently.
    private def compileWorkflow(wf: WorkflowDefinition,
                                wfSource: String,
                                locked: Boolean,
                                reorg: Boolean,
                                blockPath: Vector[Int]) : (IR.Workflow, Vector[IR.Applet]) =
    {
        Utils.trace(verbose.on, s"compiling workflow ${wf.name}")

        val graph = wf.innerGraph

        // Create a stage per call/scatter-block/declaration-block
        val (inputNodes, subBlocks, outputNodes) = Block.split(graph, wfSource)
        //Block.dbgPrint(inputNodes, subBlocks, outputNodes)

        // Make a list of all task/workflow calls made inside the block. We will need to link
        // to the equivalent dx:applets and dx:workflows.
        val callablesUsedInWorkflow : Vector[IR.Callable] =
            graph.allNodes.collect {
                case cNode : CallNode =>
                    val localname = Utils.getUnqualifiedName(cNode.callable.name)
                    callables(localname)
            }.toVector
        val wfSourceStandAlone = WdlCodeGen(verbose).standAloneWorkflow(wfSource,
                                                                        callablesUsedInWorkflow,
                                                                        language)

        // compile into an IR workflow
        val (irwf, applets, wfOutputs) =
            if (locked) {
                compileWorkflowLocked(wf, inputNodes, outputNodes, wfSourceStandAlone,
                                      blockPath, subBlocks)
            } else {
                compileWorkflowRegular(wf, inputNodes, outputNodes, wfSourceStandAlone,
                                       blockPath, subBlocks)
            }

        // Add a workflow reorg applet if necessary
        if (reorg) {
            val (reorgStage, reorgApl) = buildReorgStage(wf.name, wfSourceStandAlone, wfOutputs)
            (irwf.copy(stages = irwf.stages :+ reorgStage),
             applets :+ reorgApl)
        } else {
            (irwf, applets)
        }
    }

    // Entry point for compiling tasks and workflows into IR
    def compileCallable(callable: Callable,
                        locked: Boolean,
                        reorg: Boolean,
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
                        compileWorkflow(wf, wfSource, locked, reorg, Vector.empty[Int])
                }
            case x =>
                throw new Exception(s"""|Can't compile: ${callable.name}, class=${callable.getClass}
                                        |${x}
                                        |""".stripMargin.replaceAll("\n", " "))
        }
    }
}


object GenerateIR {
    def sortByDependencies(allCallables: Vector[Callable],
                           verbose: Verbose) : Vector[Callable] = {
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
                    callNodes.map{ cNode =>
                        // The name is fully qualified, for example, lib.add, lib.concat.
                        // We need the task/workflow itself ("add", "concat"). We are
                        // assuming that the namespace can be flattened; there are
                        // no lib.add and lib2.add.
                        Utils.getUnqualifiedName(cNode.callable.name)
                    }.toSet
                case other =>
                    throw new Exception(s"Don't know how to deal with class ${other.getClass.getSimpleName}")
            }
            Utils.getUnqualifiedName(callable.name) -> deps
        }.toMap

        // Find executables such that all of their dependencies are
        // satisfied. These can be compiled.
        def next(callables: Vector[Callable],
                 ready: Vector[Callable]) : Vector[Callable] = {
            val readyNames = ready.map(_.name).toSet
            val satisfiedCallables = callables.filter{ c =>
                val deps = immediateDeps(c.name)
                //Utils.trace(verbose.on, s"immediateDeps(${c.name}) = ${deps}")
                deps.subsetOf(readyNames)
            }
            if (satisfiedCallables.isEmpty)
                throw new Exception("Sanity: cannot find the next callable to compile.")
            satisfiedCallables
        }

        var accu = Vector.empty[Callable]
        var crnt = allCallables
        while (!crnt.isEmpty) {
            /*Utils.trace(verbose.on, s"""|  accu=${accu.map(_.name)}
                                        |  crnt=${crnt.map(_.name)}
                                        |""".stripMargin)*/
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
              reorg: Boolean,
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
        Utils.trace(verbose.on, s" tasks=${taskDir.keys}")

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
        val depOrder : Vector[Callable] = sortByDependencies(womBundle.allCallables.values.toVector,
                                                             verbose)
        Utils.trace(verbose.on,
                    s"depOrder =${depOrder.map{_.name}}")

        // compile the tasks/workflows from bottom to top.
        var allCallables = Map.empty[String, IR.Callable]
        var allCallablesSorted = Vector.empty[IR.Callable]

        // Only the toplevel workflow may be unlocked. This happens
        // only if the user specifically compiles it as "unlocked".
        def isLocked(callable: Callable): Boolean = {
            (callable, womBundle.primaryCallable) match {
                case (wf: WorkflowDefinition, Some(wf2: WorkflowDefinition)) =>
                    if (wf.name == wf2.name)
                        locked
                    else
                        true
                case (_, _) =>
                    true
            }
        }

        for (callable <- depOrder) {
            val gir = GenerateIR(allCallables, language, verbose)
            val (exec, auxApplets) = gir.compileCallable(callable,
                                                         isLocked(callable),
                                                         reorg,
                                                         taskDir,
                                                         workflowDir)
            allCallables = allCallables ++ (auxApplets.map{ apl => apl.name -> apl}.toMap)
            allCallables = allCallables + (exec.name -> exec)

            // Add the auxiliary applets while preserving the dependency order
            allCallablesSorted = allCallablesSorted ++ auxApplets :+ exec
        }

        // We already compiled all the individual wdl:tasks and
        // wdl:workflows, let's find the entrypoint.
        val primary = womBundle.primaryCallable.map{ callable =>
            allCallables(Utils.getUnqualifiedName(callable.name))
        }
        val allCallablesSorted2 = allCallablesSorted.map{_.name}
        Utils.trace(verbose.on, s"allCallablesSorted=${allCallablesSorted2}")
        assert(allCallables.size == allCallablesSorted2.size)

        Utils.traceLevelDec()
        IR.Bundle(primary, allCallables, allCallablesSorted2)
    }
}
