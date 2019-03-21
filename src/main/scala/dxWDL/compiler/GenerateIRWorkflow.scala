/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import wom.callable.WorkflowDefinition
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

case class GenerateIRWorkflow(wf : WorkflowDefinition,
                              wfSourceCode: String,
                              wfSourceStandAlone: String,
                              callsLoToHi: Vector[(String, Int)],
                              callables: Map[String, IR.Callable],
                              language: Language.Value,
                              verbose: Verbose) {
    val verbose2 : Boolean = verbose.keywords contains "GenerateIR"
    private val nameBox = NameBox(verbose)

    private case class LinkedVar(cVar: CVar, sArg: SArg)
    private type CallEnv = Map[String, LinkedVar]

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

    // Create a human readable name for a block of statements
    private def createBlockName(block: Block) : String = {
        block.makeName match {
            case None => s"eval ${genFragId()}"
            case Some(name) => nameBox.chooseUniqueName(name)
        }
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

            case ScatterVariableNode(id, expression: ExpressionNode , womType) =>
                CVar(id.workflowLocalName, womType, None)

            case other =>
                throw new Exception(s"Unhandled type ${other.getClass}")
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
                            |is unspecified. This is illegal in a locked workflow.""".stripMargin.replaceAll("\n", " "))
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

    private def getSArgFromEnv(source: String,
                               env: CallEnv) : SArg = {
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

    // A block complex enough to require a workflow.
    // Recursively call into the asssemble-backbone method, and
    // get a locked subworkflow.
    private def compileNestedBlock(graph: Graph,
                                   blockPath: Vector[Int]) : (IR.Workflow, Vector[IR.Callable]) = {
        val (inputNodes, subBlocks, outputNodes) = Block.splitGraph(graph, callsLoToHi)

        val name = createBlockName(subBlocks(0))
        val (subwf, auxCallables, _ ) = compileWorkflowLocked(wf.name + "_" + name,
                                                              inputNodes, outputNodes,
                                                              blockPath, subBlocks)
        (subwf, auxCallables)
    }

    // Build an applet to evaluate a WDL workflow fragment
    //
    // The [blockPath] argument keeps track of which block this fragment represents.
    // A top level block is a number. A sub-block of a top-level block is a vector of two
    // numbers, etc.
    private def compileWfFragment(block: Block,
                                  blockPath: Vector[Int],
                                  env : CallEnv) : (IR.Stage, Vector[IR.Callable]) = {
        val stageName = createBlockName(block)
        Utils.trace(verbose.on, s"--- Compiling fragment <${stageName}> as stage")

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

        // The fragment runner can only handle a single call. If the
        // block already has exactly one call, then we are good. If
        // it contains a scatter/conditional with several calls,
        // then compile the inner block into a sub-workflow.
        //
        // Figure out the name of the callable; we need to link with it when we
        // get to the native phase.
        val (_, catg) = Block.categorize(block)
        val (innerCall, auxCallables) : (Option[String], Vector[IR.Callable]) = catg match {
            case Block.AllExpressions => (None, Vector.empty)
            case _ : Block.CallDirect => throw new Exception(s"a direct call should not reach this stage")
            case Block.CallWithEval(_) | Block.Cond(_) | Block.Scatter(_) =>
                // A simple block with no nested sub-blocks, and a single call.
                val calls = Block.deepFindCalls(block.nodes).map{ cNode =>
                    Utils.getUnqualifiedName(cNode.callable.name)
                }.toVector
                assert(calls.size == 1)
                (Some(calls.head), Vector.empty)
            case Block.ScatterWithNesting(_) | Block.CondWithNesting(_) =>
                val innerGraph = catg.getInnerGraph
                val (subwf, auxCallables) = compileNestedBlock(innerGraph, blockPath)
                (Some(subwf.name), subwf +: auxCallables)
        }

        val applet = IR.Applet(s"${wf.name}_${stageName}",
                               inputVars,
                               outputVars,
                               IR.InstanceTypeDefault,
                               IR.DockerImageNone,
                               IR.AppletKindWfFragment(innerCall.toVector, blockPath, fqnDictTypes),
                               wfSourceStandAlone)

        val sArgs : Vector[SArg] = closure.map {
            case (_, LinkedVar(_, sArg)) => sArg
        }.toVector

        Utils.trace(verbose.on, "---")
        (IR.Stage(stageName, genStageId(), applet.name, sArgs, outputVars),
         applet +: auxCallables)
    }

    // Assemble the backbone of a workflow, having compiled the
    // independent tasks.  This is shared between locked and unlocked
    // workflows. At this point we we have workflow level inputs and
    // outputs.
    private def assembleBackbone(wfName: String,
                                 wfInputs: Vector[(CVar, SArg)],
                                 blockPath: Vector[Int],
                                 subBlocks: Vector[Block],
                                 locked: Boolean)
            : (Vector[(IR.Stage, Vector[IR.Callable])], CallEnv) =
    {
        Utils.trace(verbose.on, s"Assembling workfow backbone ${wf.name}")

        var env : CallEnv = wfInputs.map { case (cVar,sArg) =>
            cVar.name -> LinkedVar(cVar, sArg)
        }.toMap
        var allStageInfo = Vector.empty[(IR.Stage, Vector[IR.Callable])]
        var remainingBlocks = subBlocks

        // link together all the stages into a linear workflow
        for (blockNum <- 0 to (subBlocks.length -1)) {
            val block = remainingBlocks.head
            remainingBlocks = remainingBlocks.tail

            val (_, category) = Block.categorize(block)
            val (stage, auxCallables) = category match {
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
                    (stage, Vector.empty)

                case _ =>
                    //     A simple block that requires just one applet,
                    // OR: A complex block that needs a subworkflow
                    val (stage, auxCallables) = compileWfFragment(block,
                                                                  blockPath :+ blockNum,
                                                                  env)
                    for (cVar <- stage.outputs) {
                        env = env + (cVar.name ->
                                         LinkedVar(cVar, IR.SArgLink(stage.stageName, cVar)))
                    }
                    (stage, auxCallables)
            }
            allStageInfo :+= (stage, auxCallables)
        }

        val stagesDbgStr = allStageInfo.map{ case (stage,_) =>
            s"    Stage(${stage.stageName}, callee=${stage.calleeName})"
        }.mkString("\n")
        Utils.trace(verbose2, s"""|stages for workflow ${wfName} =
                                  |${stagesDbgStr}
                                  |""".stripMargin)
        (allStageInfo, env)
    }


    private def buildSimpleWorkflowOutput(outputNode: GraphOutputNode, env: CallEnv) : (CVar, SArg) = {
        outputNode match {
            case PortBasedGraphOutputNode(id, womType, sourcePort) =>
                val cVar = CVar(id.workflowLocalName, womType, None)
                val source = sourcePort.name
                (cVar, getSArgFromEnv(source, env))
            case expr :ExpressionBasedGraphOutputNode if (Block.isTrivialExpression(expr.womExpression)) =>
                val cVar = CVar(expr.graphOutputPort.name, expr.womType, None)
                val source = expr.womExpression.sourceString
                (cVar, getSArgFromEnv(source, env))
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
                                  wfSourceStandAlone: String,
                                  inputVars: Vector[CVar]) : (IR.Stage, IR.Applet) = {
        val appletName = s"${wfName}_${Utils.COMMON}"
        Utils.trace(verbose.on, s"Compiling common applet ${appletName}")

        val outputVars: Vector[CVar] = inputVars
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               IR.InstanceTypeDefault,
                               IR.DockerImageNone,
                               IR.AppletKindWfInputs,
                               wfSourceStandAlone)

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
                                 wfSourceStandAlone : String,
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

        val appletName = s"${wfName}_${Utils.OUTPUT_SECTION}"
        val applet = IR.Applet(appletName,
                               inputVars.map(_.cVar),
                               outputVars,
                               IR.InstanceTypeDefault,
                               IR.DockerImageNone,
                               IR.AppletKindWfOutputs,
                               wfSourceStandAlone)

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
                                wfSourceStandAlone : String,
                                wfOutputs: Vector[(CVar, SArg)]) : (IR.Stage, IR.Applet) = {
        val appletName = s"${wfName}_${Utils.REORG}"
        Utils.trace(verbose.on, s"Compiling output reorganization applet ${appletName}")

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletName,
                               wfOutputs.map{ case (cVar, _) => cVar },
                               Vector.empty,
                               IR.InstanceTypeDefault,
                               IR.DockerImageNone,
                               IR.AppletKindWorkflowOutputReorg,
                               wfSourceStandAlone)

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = wfOutputs.map{ case (_, sArg) => sArg }.toVector

        (IR.Stage(Utils.REORG, genStageId(Some("reorg")), applet.name, inputs, Vector.empty[CVar]),
         applet)
    }

    private def compileWorkflowLocked(wfName: String,
                                      inputNodes: Vector[GraphInputNode],
                                      outputNodes: Vector[GraphOutputNode],
                                      blockPath: Vector[Int],
                                      subBlocks : Vector[Block]) :
            (IR.Workflow, Vector[IR.Callable], Vector[(CVar, SArg)]) =
    {
        val wfInputs:Vector[(CVar, SArg)] = inputNodes.map{
            case iNode =>
                val cVar = buildWorkflowInput(iNode)
                (cVar, IR.SArgWorkflowInput(cVar))
        }.toVector

        val (allStageInfo, env) = assembleBackbone(wfName, wfInputs, blockPath, subBlocks, true)
        val (stages, auxCallables) = allStageInfo.unzip

        // Handle outputs that are constants or variables, we can output them directly
        if (outputNodes.forall(Block.isSimpleOutput)) {
            val simpleWfOutputs = outputNodes.map(node => buildSimpleWorkflowOutput(node, env)).toVector
            val irwf = IR.Workflow(wfName, wfInputs, simpleWfOutputs, stages, true)
            (irwf, auxCallables.flatten, simpleWfOutputs)
        } else {
            // Some of the outputs are expressions. We need an extra applet+stage
            // to evaluate them.
            val (outputStage, outputApplet) = buildOutputStage(wfName,
                                                               wfSourceStandAlone,
                                                               outputNodes,
                                                               env)
            val wfOutputs = outputStage.outputs.map{ cVar =>
                (cVar, IR.SArgLink(outputStage.stageName, cVar))
            }
            val irwf = IR.Workflow(wfName,
                                   wfInputs, wfOutputs, stages :+ outputStage, true)
            (irwf, auxCallables.flatten :+ outputApplet, wfOutputs)
        }
    }


    private def compileWorkflowRegular(inputNodes: Vector[GraphInputNode],
                                       outputNodes: Vector[GraphOutputNode],
                                       subBlocks : Vector[Block])
            : (IR.Workflow, Vector[IR.Callable], Vector[(CVar, SArg)]) =
    {
        // Create a special applet+stage for the inputs. This is a substitute for
        // workflow inputs. We now call the workflow inputs, "fauxWfInputs". This is because
        // they are references to the outputs of this first applet.

        // compile into dx:workflow inputs
        val wfInputDefs:Vector[CVar] = inputNodes.map{
            case iNode => buildWorkflowInput(iNode)
        }.toVector
        val (commonStg, commonApplet) = buildCommonApplet(wf.name,
                                                          wfSourceStandAlone,
                                                          wfInputDefs)
        val fauxWfInputs:Vector[(CVar, SArg)] = commonStg.outputs.map{
            case cVar: CVar =>
                val sArg = IR.SArgLink(commonStg.stageName, cVar)
                (cVar, sArg)
        }.toVector

        val (allStageInfo, env) = assembleBackbone(wf.name, fauxWfInputs, Vector.empty, subBlocks, false)
        val (stages: Vector[IR.Stage], auxCallables) = allStageInfo.unzip

        // convert the outputs into an applet+stage
        val (outputStage, outputApplet) = buildOutputStage(wf.name,
                                                           wfSourceStandAlone,
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
        (irwf, commonApplet +: auxCallables.flatten :+ outputApplet, wfOutputs)
    }


    // Compile a (single) WDL workflow into a single dx:workflow.
    //
    // There are cases where we are going to need to generate dx:subworkflows.
    // This is not handled currently.
    private def apply2(locked: Boolean, reorg: Boolean) : (IR.Workflow, Vector[IR.Callable]) =
    {
        Utils.trace(verbose.on, s"compiling workflow ${wf.name}")
        val graph = wf.innerGraph

        // Create a stage per call/scatter-block/declaration-block
        val (inputNodes, subBlocks, outputNodes) = Block.splitGraph(graph, callsLoToHi)

        // compile into an IR workflow
        val (irwf, irCallables, wfOutputs) =
            if (locked) {
                compileWorkflowLocked(wf.name, inputNodes, outputNodes,
                                      Vector.empty, subBlocks)
            } else {
                compileWorkflowRegular(inputNodes, outputNodes, subBlocks)
            }

        // Add a workflow reorg applet if necessary
        if (reorg) {
            val (reorgStage, reorgApl) = buildReorgStage(wf.name,
                                                         wfSourceStandAlone,
                                                         wfOutputs)
            (irwf.copy(stages = irwf.stages :+ reorgStage),
             irCallables :+ reorgApl)
        } else {
            (irwf, irCallables)
        }
    }


    def apply(locked: Boolean, reorg: Boolean) : (IR.Workflow, Vector[IR.Callable]) = {
        val (irwf, irCallables) = apply2(locked, reorg)

        // sanity check
        val callableNames: Set[String] =
            irCallables.map(_.name).toSet ++ callables.map(_._1).toSet

        irwf.stages.foreach {
            case stage =>
                if (!(callableNames contains stage.calleeName)) {
                    throw new Exception(
                        s"""|Generated bad workflow.
                            |Stage <${stage.stageName}> calls <${stage.calleeName}> which is missing.
                            |
                            |stage names = ${irwf.stages.map(_.stageName)}
                            |callables = ${callableNames}
                            |""".stripMargin)
                }
        }
        (irwf, irCallables)
    }
}
