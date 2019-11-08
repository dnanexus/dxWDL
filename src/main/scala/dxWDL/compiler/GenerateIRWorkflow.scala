/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import wom.callable.WorkflowDefinition
import wom.expression.WomExpression
import wom.graph._
import wom.graph.expression._
import wom.types._
import wom.values._
import dxWDL.base.{Language, _}
import dxWDL.dx._
import dxWDL.util._
import IR.{COMMON, CVar, OUTPUT_SECTION, REORG, SArg, SArgConst}

case class GenerateIRWorkflow(wf: WorkflowDefinition,
                              wfSourceCode: String,
                              wfSourceStandAlone: String,
                              callsLoToHi: Vector[String],
                              callables: Map[String, IR.Callable],
                              language: Language.Value, verbose: Verbose,
                              reorg: Either[Boolean, ReorgAttrs]) {
    val verbose2 : Boolean = verbose.containsKey("GenerateIR")

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

            case OptionalGraphInputNodeWithDefault(id, womType, defaultExpr : WomExpression,
                                                   nameInInputSet, valueMapper) =>
                val defaultValue: WomValue = WomValueAnalysis.ifConstEval(womType, defaultExpr) match {
                    case None => throw new Exception(
                        s"""|default expression in input should be a constant
                            | ${input}
                            |""".stripMargin)
                    case Some(value) => value
                }
                CVar(id.workflowLocalName, womType, Some(defaultValue))

            case ScatterVariableNode(id, expression: ExpressionNode , womType) =>
                CVar(id.workflowLocalName, womType, None)

            case ogin : OuterGraphInputNode =>
                val womType = ogin.linkToOuterGraph.womType
                CVar(ogin.identifier.workflowLocalName, womType, None)

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
    private def findInputByName(callInputs: Seq[AnonymousExpressionNode],
                                cVar: CVar) : Option[AnonymousExpressionNode] = {
        val retval = callInputs.find{
            case expr : AnonymousExpressionNode =>
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
        val callInputs: Seq[AnonymousExpressionNode] = call.upstream.collect{
            case tcExpr : AnonymousExpressionNode => tcExpr
        }.toSeq

        // Extract the input values/links from the environment
        val inputs: Vector[SArg] = callee.inputVars.map{ cVar =>
            findInputByName(callInputs, cVar) match {
                case None if Utils.isOptional(cVar.womType) =>
                    // optional argument that is not provided
                    IR.SArgEmpty

                case None if cVar.default != None =>
                    // argument that has a default, it can be omitted in the call
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

                case Some(tcInput) if WomValueAnalysis.isExpressionConst(cVar.womType, tcInput.womExpression) =>
                    IR.SArgConst(WomValueAnalysis.evalConst(cVar.womType, tcInput.womExpression))

                case Some(tcInput) =>
                    val exprSourceString = tcInput.womExpression.sourceString
                    env.get(exprSourceString) match {
                        case None =>
                            val envDbg = env.map{ case (name, lVar) =>
                                s"  ${name} -> ${lVar.sArg}"
                            }.mkString("\n")
                            Utils.trace(verbose.on, s"""|env =
                                                        |${envDbg}""".stripMargin)
                            throw new Exception(
                                s"""|Internal compiler error.
                                    |
                                    |Input <${cVar.name}, ${cVar.womType}> to call <${call.fullyQualifiedName}>
                                    |is missing from the environment."""
                                    .stripMargin.replaceAll("\n", " "))
                        case Some(lVar) => lVar.sArg
                    }
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
                            env: CallEnv,
                            optional: Boolean) : Option[(String, LinkedVar)] = {
        lookupInEnvInner(fqn, env) match {
            case None if Utils.isOptional(womType) =>
                None
            case None =>
                if (!optional) {
                    // A missing compulsory argument
                    Utils.warning(verbose,
                                  s"Missing argument ${fqn}, it will have to be provided at runtime")
                }
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
        val (allInputs, optionalArgNames) = Block.closure(block)
        allInputs.flatMap { case (name, womType) =>
            val isOptionalArg = (optionalArgNames contains name)
            lookupInEnv(name, womType, env, isOptionalArg)
        }.toMap
    }

    // Find the closure of a graph, besides its normal inputs. Create an input
    // node for each of these external references.
    private def graphClosure(inputNodes: Vector[GraphInputNode],
                             subBlocks: Vector[Block]) : Map[String, WomType] = {
        val allInputs : Map[String, WomType] = subBlocks.map{ block =>
            val (inputs, _) = Block.closure(block)
            inputs
        }.flatten.toMap

        // remove the regular inputs
        val regularInputNames : Set[String] = inputNodes.map {
            case iNode => iNode.localName
        }.toSet
        allInputs.filter{
            case (name, _) =>
                !(regularInputNames contains name)
        }.toMap
    }

    // A block inside a conditional or scatter. If it is simple,
    // we can use a direct call. Otherwise, recursively call into the asssemble-backbone method, and
    // get a locked subworkflow.
    private def compileNestedBlock(wfName: String,
                                   graph: Graph,
                                   blockPath: Vector[Int],
                                   env: CallEnv) : (IR.Callable, Vector[IR.Callable]) = {
        val (inputNodes, _, subBlocks, outputNodes) =
            Block.splitGraph(graph, callsLoToHi)
        assert(subBlocks.size > 0)

        if (subBlocks.size == 1) {
            Block.categorize(subBlocks(0)) match {
                case Block.CallDirect(_,_) | Block.CallWithSubexpressions(_, _) =>
                    throw new Exception("sanity")
                case _ => ()
            }

            // At runtime, we will need to execute a workflow
            // fragment. This requires an applet.
            //
            // This is a recursive call, to compile a  potentially
            // complex sub-block. It could have many calls generating
            // many applets and subworkflows.
            val (stage, aux) = compileWfFragment(wfName,
                                                 subBlocks(0),
                                                 blockPath :+ 0,
                                                 env)
            val fragName = stage.calleeName
            val main = aux.find(_.name == fragName) match {
                case None => throw new Exception(s"Could not find ${fragName}")
                case Some(x) => x
            }
            (main, aux)
        } else {
            // there are several subblocks, we need a subworkflow to string them
            // together.
            //
            // The subworkflow may access declerations outside of its scope.
            // For example, stage-0.result, and stage-1.result are inputs to
            // stage-2, that belongs to a subworkflow. Because the subworkflow is
            // locked, we need to make them proper inputs.
            //
            //  |- stage-0.result
            //  |
            //  |- stage-1.result
            //  |
            //  |--- |- stage-2
            //       |
            //       |- stage-3
            //       |
            //       |- stage-4
            //
            val pathStr = blockPath.map(x => x.toString).mkString("_")
            val closureInputs = graphClosure(inputNodes, subBlocks)
            Utils.trace(verbose.on, s"""|compileNestedBlock, closureInputs=
                                        | ${closureInputs}
                                        |""".stripMargin)
            val (subwf, auxCallables, _ ) = compileWorkflowLocked(wfName + "_block_" + pathStr,
                                                                  inputNodes,
                                                                  closureInputs,
                                                                  outputNodes,
                                                                  blockPath, subBlocks,
                                                                  IR.Level.Sub)
            (subwf, auxCallables)
        }
    }

    // Build an applet to evaluate a WDL workflow fragment
    //
    // The [blockPath] argument keeps track of which block this fragment represents.
    // A top level block is a number. A sub-block of a top-level block is a vector of two
    // numbers, etc.
    private def compileWfFragment(wfName: String,
                                  block: Block,
                                  blockPath: Vector[Int],
                                  env : CallEnv) : (IR.Stage, Vector[IR.Callable]) = {
        val stageName = block.makeName match {
            case None => "eval"
            case Some(name) => name
        }
        Utils.trace(verbose.on, s"Compiling fragment <${stageName}> as stage")

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
        val catg = Block.categorize(block)
        Utils.trace(verbose2, s"""|category : ${Block.Category.toString(catg)}
                                  |""".stripMargin)

        val (innerCall, auxCallables) : (Option[String], Vector[IR.Callable]) = catg match {
            case Block.AllExpressions(_) => (None, Vector.empty)
            case Block.CallDirect(_,_) => throw new Exception(s"a direct call should not reach this stage")

            // A block with no nested sub-blocks, and a single call.
            case Block.CallWithSubexpressions(_, cNode) =>
                (Some(Utils.getUnqualifiedName(cNode.callable.name)), Vector.empty)
            case Block.CallFragment(_, cNode) =>
                (Some(Utils.getUnqualifiedName(cNode.callable.name)), Vector.empty)

            // A conditional/scatter with exactly one call in the sub-block.
                // Can be executed by a fragment.
            case Block.CondOneCall(_, _, cNode) =>
                (Some(Utils.getUnqualifiedName(cNode.callable.name)), Vector.empty)
            case Block.ScatterOneCall(_, _, cNode) =>
                (Some(Utils.getUnqualifiedName(cNode.callable.name)), Vector.empty)

            case Block.CondFullBlock(_, condNode) =>
                val (callable, aux) = compileNestedBlock(wfName, condNode.innerGraph,
                                                         blockPath, env)
                (Some(callable.name), aux :+ callable)

            case Block.ScatterFullBlock(_,sctNode) =>
                // add the iteration variable to the inner environment
                assert(sctNode.scatterVariableNodes.size == 1)
                val svNode: ScatterVariableNode = sctNode.scatterVariableNodes.head
                val iterVarName = svNode.identifier.localName.value
                val cVar = CVar(iterVarName, svNode.womType, None)
                val innerEnv = env + (iterVarName -> LinkedVar(cVar, IR.SArgEmpty))
                val (callable, aux) = compileNestedBlock(wfName, sctNode.innerGraph,
                                                         blockPath, innerEnv)
                (Some(callable.name), aux :+ callable)
        }

        val applet = IR.Applet(s"${wfName}_frag_${genFragId()}",
                               inputVars,
                               outputVars,
                               IR.InstanceTypeDefault,
                               IR.DockerImageNone,
                               IR.AppletKindWfFragment(innerCall.toVector, blockPath, fqnDictTypes),
                               wfSourceStandAlone)

        val sArgs : Vector[SArg] = closure.map {
            case (_, LinkedVar(_, sArg)) => sArg
        }.toVector

        (IR.Stage(stageName, genStageId(), applet.name, sArgs, outputVars),
         auxCallables :+ applet)
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
        Utils.trace(verbose.on, s"Assembling workflow backbone ${wfName}")
        Utils.traceLevelInc()
        val inputNamesDbg = wfInputs.map{ case (cVar, _) => cVar.name }
        Utils.trace(verbose.on, s"inputs= ${inputNamesDbg}")

        var env : CallEnv = wfInputs.map { case (cVar,sArg) =>
            cVar.name -> LinkedVar(cVar, sArg)
        }.toMap

        var allStageInfo = Vector.empty[(IR.Stage, Vector[IR.Callable])]
        var remainingBlocks = subBlocks

        // link together all the stages into a linear workflow
        for (blockNum <- 0 to (subBlocks.length -1)) {
            val block = remainingBlocks.head
            remainingBlocks = remainingBlocks.tail

            val (stage, auxCallables) = Block.categorize(block) match {
                case Block.CallDirect(_, call) =>
                    // The block contains exactly one call, with no extra declarations.
                    // All the variables are already in the environment, so there
                    // is no need to do any extra work. Compile directly into a workflow
                    // stage.
                    Utils.trace(verbose.on, s"Compiling call ${call.callable.name} as stage")
                    val stage = compileCall(call, env, locked)

                    // Add bindings for the output variables. This allows later calls to refer
                    // to these results.
                    for (cVar <- stage.outputs) {
                        val fqn = call.identifier.localName.value ++ "." + cVar.name
                        val cVarFqn = cVar.copy(name = fqn)
                        env = env + (fqn -> LinkedVar(cVarFqn, IR.SArgLink(stage.id, cVar)))
                    }
                    (stage, Vector.empty)

                case _ =>
                    //     A simple block that requires just one applet,
                    // OR: A complex block that needs a subworkflow
                    val (stage, auxCallables) = compileWfFragment(wfName,
                                                                  block,
                                                                  blockPath :+ blockNum,
                                                                  env)
                    for (cVar <- stage.outputs) {
                        env = env + (cVar.name ->
                                         LinkedVar(cVar, IR.SArgLink(stage.id, cVar)))
                    }
                    (stage, auxCallables)
            }
            allStageInfo :+= (stage, auxCallables)
        }

        if (verbose2) {
            Utils.trace(verbose2, s"stages for workflow ${wfName} = [")
            allStageInfo.foreach{ case (stage,_) =>
                Utils.trace(verbose2, s"    ${stage.description}, ${stage.id.getId} -> callee=${stage.calleeName}")
            }
            Utils.trace(verbose2, "]")
        }
        Utils.traceLevelDec()
        (allStageInfo, env)
    }


    private def buildSimpleWorkflowOutput(outputNode: GraphOutputNode, env: CallEnv) : (CVar, SArg) = {
        outputNode match {
            case PortBasedGraphOutputNode(id, womType, sourcePort) =>
                val cVar = CVar(id.fullyQualifiedName.value, womType, None)
                val source = sourcePort.name
                (cVar, getSArgFromEnv(source, env))
            case expr :ExpressionBasedGraphOutputNode if (Block.isTrivialExpression(expr.womType, expr.womExpression)) =>
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
        val outputVars: Vector[CVar] = inputVars

        val applet = IR.Applet(s"${wfName}_${COMMON}",
                               inputVars,
                               outputVars,
                               IR.InstanceTypeDefault,
                               IR.DockerImageNone,
                               IR.AppletKindWfInputs,
                               wfSourceStandAlone)
        Utils.trace(verbose.on, s"Compiling common applet ${applet.name}")

        val sArgs: Vector[SArg] = inputVars.map{ _ => IR.SArgEmpty}.toVector
        (IR.Stage(COMMON, genStageId(Some(COMMON)), applet.name, sArgs, outputVars),
         applet)
    }

    private def addOutputStatus(outputsVar: Vector[CVar]) = {
        outputsVar :+ CVar(
            Utils.REORG_STATUS,
            WomStringType,
            Some(WomString("completed"))
        )
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

        val updatedOutputVars: Vector[CVar] = reorg match {
            case Left(reorg_flag) => outputVars
            case Right(reorg_attrs) => addOutputStatus(outputVars)
        }
        val applet = IR.Applet(s"${wfName}_${OUTPUT_SECTION}",
            inputVars.map(_.cVar),
            updatedOutputVars,
            IR.InstanceTypeDefault,
            IR.DockerImageNone,
            IR.AppletKindWfOutputs,
            wfSourceStandAlone)


        // define the extra stage we add to the workflow
        (IR.Stage(OUTPUT_SECTION, genStageId(Some(OUTPUT_SECTION)), applet.name,
                  inputVars.map(_.sArg), updatedOutputVars),
         applet)
    }

     // Create an applet to reorganize the output files. We want to
    // move the intermediate results to a subdirectory.  The applet
    // needs to process all the workflow outputs, to find the files
    // that belong to the final results.
    private def buildReorgStage(wfName: String,
                                wfSourceStandAlone : String,
                                wfOutputs: Vector[(CVar, SArg)]) : (IR.Stage, IR.Applet) = {
        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(s"${wfName}_${REORG}",
                               wfOutputs.map{ case (cVar, _) => cVar },
                               Vector.empty,
                               IR.InstanceTypeDefault,
                               IR.DockerImageNone,
                               IR.AppletKindWorkflowOutputReorg,
                               wfSourceStandAlone)
        Utils.trace(verbose.on, s"Compiling output reorganization applet ${applet.name}")

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = wfOutputs.map{ case (_, sArg) => sArg }.toVector

        (IR.Stage(REORG, genStageId(Some(REORG)), applet.name, inputs, Vector.empty[CVar]),
         applet)
    }

    private def addCustomReorgStage(wfName: String,
                                    wfSourceStandAlone: String,
                                    wfOutputs: Vector[(CVar, SArg)],
                                    reorgAttributes: ReorgAttrs
                                   ) : (IR.Stage, IR.Applet) = {

        val appletKind = IR.AppletKindWorkflowCustomReorg(reorgAttributes.appId)

        // will throw error if there is no status string. Should consider checking there i s only one.
        val reorgStatusInput: (CVar, SArg) = wfOutputs.filter(x=> x._1.name == Utils.REORG_STATUS).head

        val configFile: Option[WomSingleFile] = reorgAttributes.reorgInputs match {
            case "" => None
            case x: String => Some(WomSingleFile(x))
        }

        val appInputs = Vector(
            reorgStatusInput._1,
            CVar(Utils.REORG_CONFIG, WomSingleFileType, configFile)
        )

        val applet = IR.Applet(
            reorgAttributes.appId,
            appInputs,
            Vector.empty,
            IR.InstanceTypeDefault,
            IR.DockerImageNone,
            appletKind,
            wfSourceStandAlone
        )

        Utils.trace(verbose.on, s"Adding custom output reorganization applet ${reorgAttributes.appId}")

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = Vector(reorgStatusInput._2,  SArgConst(configFile.get))

        (IR.Stage(REORG, genStageId(Some(REORG)), applet.name, inputs
            , Vector.empty[CVar]),
          applet)
    }


    private def compileWorkflowLocked(wfName: String,
                                      inputNodes: Vector[GraphInputNode],
                                      closureInputs: Map[String, WomType],
                                      outputNodes: Vector[GraphOutputNode],
                                      blockPath: Vector[Int],
                                      subBlocks : Vector[Block],
                                      level: IR.Level.Value) :
            (IR.Workflow, Vector[IR.Callable], Vector[(CVar, SArg)]) =
    {
        val wfInputs:Vector[(CVar, SArg)] = inputNodes.map{
            case iNode =>
                val cVar = buildWorkflowInput(iNode)
                (cVar, IR.SArgWorkflowInput(cVar))
        }.toVector

        // inputs that are a result of accessing declarations in an ecompassing
        // WDL workflow.
        val clsInputs: Vector[(CVar, SArg)] = closureInputs.map{
            case (name, womType) =>
                val cVar = CVar(name, womType, None)
                (cVar, IR.SArgWorkflowInput(cVar))
        }.toVector
        val allWfInputs = wfInputs ++ clsInputs

        val (allStageInfo, env) = assembleBackbone(wfName, allWfInputs,
                                                   blockPath, subBlocks, true)
        val (stages, auxCallables) = allStageInfo.unzip

        // Handle outputs that are constants or variables, we can output them directly.
        //
        // Is an output used directly as an input? For example, in the
        // small workflow below, 'lane' is used in such a manner.
        //
        // workflow inner {
        //   input {
        //      String lane
        //   }
        //   output {
        //      String blah = lane
        //   }
        // }
        //
        // In locked dx:workflows, it is illegal to access a workflow input directly from
        // a workflow output. It is only allowed to access a stage input/output.
        if (outputNodes.forall(Block.isSimpleOutput) &&
                Block.inputsUsedAsOutputs(inputNodes, outputNodes).isEmpty) {
            val simpleWfOutputs = outputNodes.map(node => buildSimpleWorkflowOutput(node, env)).toVector
            val irwf = IR.Workflow(wfName, allWfInputs, simpleWfOutputs, stages, wfSourceCode, true, level)
            (irwf, auxCallables.flatten, simpleWfOutputs)
        } else {
            // Some of the outputs are expressions. We need an extra applet+stage
            // to evaluate them.
            val (outputStage, outputApplet) = buildOutputStage(wfName,
                                                               wfSourceStandAlone,
                                                               outputNodes,
                                                               env)
            val wfOutputs = outputStage.outputs.map{ cVar =>
                (cVar, IR.SArgLink(outputStage.id, cVar))
            }
            val irwf = IR.Workflow(wfName,
                                   allWfInputs, wfOutputs,
                                   stages :+ outputStage,
                                   wfSourceCode,
                                   true, level)
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
                val sArg = IR.SArgLink(commonStg.id, cVar)
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
            (cVar, IR.SArgLink(outputStage.id, cVar))
        }
        val irwf = IR.Workflow(wf.name,
                               wfInputs.toVector,
                               wfOutputs.toVector,
                               commonStg +: stages :+ outputStage,
                               wfSourceCode,
                               false,
                               IR.Level.Top)
        (irwf, commonApplet +: auxCallables.flatten :+ outputApplet, wfOutputs)
    }


    // Compile a (single) user defined WDL workflow into a dx:workflow.
    //
    private def apply2(locked: Boolean) =
    {
        Utils.trace(verbose.on, s"compiling workflow ${wf.name}")
        val graph = wf.innerGraph

        // Create a stage per call/scatter-block/declaration-block
        val (inputNodes, _, subBlocks, outputNodes) = Block.splitGraph(graph, callsLoToHi)

        // compile into an IR workflow
        val (irwf, irCallables, wfOutputs) =
            if (locked) {
                compileWorkflowLocked(wf.name,
                                      inputNodes, Map.empty, outputNodes,
                                      Vector.empty, subBlocks, IR.Level.Top)
            } else {
                compileWorkflowRegular(inputNodes, outputNodes, subBlocks)
            }

        // Add a workflow reorg applet if necessary
        val (wf2: IR.Workflow, apl2: Vector[IR.Callable]) = reorg match {
            case Left(reorg_flag) => if (reorg_flag) {
                val (reorgStage, reorgApl) = buildReorgStage(wf.name,
                    wfSourceStandAlone,
                    wfOutputs)
                (irwf.copy(stages = irwf.stages :+ reorgStage),
                  irCallables :+ reorgApl)
            } else {
                (irwf, irCallables)
            }
            case Right(reorgAttributes) =>
                val (reorgStage, reorgApl) = addCustomReorgStage(wf.name,
                    wfSourceStandAlone,
                    wfOutputs,
                    reorgAttributes)
                (irwf.copy(stages = irwf.stages :+ reorgStage),
                  irCallables :+ reorgApl)
        }

        (wf2, apl2)
    }

    def apply(locked: Boolean): (IR.Workflow, Vector[IR.Callable]) = {
        val (irwf, irCallables) = apply2(locked)

        // sanity check
        val callableNames: Set[String] =
            irCallables.map(_.name).toSet ++ callables.map(_._1).toSet

        irwf.stages.foreach {
            case stage =>
                if (!(callableNames contains stage.calleeName)) {
                    val allStages = irwf.stages.map{ case stg =>
                        s"${stg}.description, ${stg}.id.getId"
                    }.mkString("    ")
                    throw new Exception(
                        s"""|Generated bad workflow.
                            |Stage <${stage.id.getId}, ${stage.description}> calls <${stage.calleeName}>
                            |which is missing.
                            |
                            |stages = ${allStages}
                            |callables = ${callableNames}
                            |""".stripMargin)
                }
        }

        (irwf, irCallables)
    }
}
