package dx.compiler

import dx.api.DxWorkflowStage
import dx.compiler.IR.{COMMON, CVar, OUTPUT_SECTION, REORG, SArg, SArgConst}
import dx.core.{REORG_STATUS, REORG_STATUS_COMPLETE}
import dx.core.languages.Language
import dx.core.languages.wdl.{Block, PrettyPrintApprox, WdlValueAnalysis}
import wdlTools.eval.WdlValues
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{Adjuncts, Logger}

case class GenerateIRWorkflow(wf: TAT.Workflow,
                              wfStandAlone: TAT.Document,
                              callables: Map[String, IR.Callable],
                              language: Language.Value,
                              logger: Logger,
                              locked: Boolean,
                              reorg: Either[Boolean, ReorgAttrs],
                              adjunctFiles: Option[Vector[Adjuncts.AdjunctFile]]) {
  private case class LinkedVar(cVar: CVar, sArg: SArg)
  private type CallEnv = Map[String, LinkedVar]

  // generate a stage Id, this is a string of the form: 'stage-xxx'
  private var stageNum = 0
  private def genStageId(stageName: Option[String] = None): DxWorkflowStage = {
    stageName match {
      case None =>
        val retval = DxWorkflowStage(s"stage-$stageNum")
        stageNum += 1
        retval
      case Some(nm) =>
        DxWorkflowStage(s"stage-$nm")
    }
  }

  // create a unique name for a workflow fragment
  private var fragNum = 0
  private def genFragId(): String = {
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
  //   String s = "glasses"
  //   ...
  // }
  // Also return the default expression in the case of a complex workflow
  // input that needs to be evaluated at runtime.
  def buildWorkflowInput(input: Block.InputDefinition): (CVar, Option[TAT.Expr]) = {
    // figure out the meta attribute for this input, if it is
    // specified in the parameter meta section.
    val metaValue: Option[TAT.MetaValue] = wf.parameterMeta match {
      case None => None
      case Some(TAT.ParameterMetaSection(kvs, _)) =>
        kvs.get(input.name)
    }
    val attr = ParameterMeta.unwrap(metaValue, input.wdlType)

    input match {
      case Block.RequiredInputDefinition(id, wdlType) =>
        (CVar(id, wdlType, None, attr), None)
      case Block.OverridableInputDefinitionWithConstantDefault(id, wdlType, defaultValue) =>
        (CVar(id, wdlType, Some(defaultValue), attr), None)
      case Block.OverridableInputDefinitionWithDynamicDefault(id, wdlType, defaultExpr) =>
        // If the default value is an expression that requires evaluation (i.e. not a constant),
        // treat the input as optional and leave the default value to be calculated at runtime
        (CVar(id, wdlType, None, attr), Some(defaultExpr))
      case Block.OptionalInputDefinition(id, wdlType) =>
        (CVar(id, wdlType, None, attr), None)
    }
  }

  // In an expression like `a.b`, the left-hand side (a) is an expression and the
  // right-hand side (b) is an identifier. The env only contains a, so we need to
  // resolve a before we can access b.
  private def isOptional(t: WdlTypes.T): Boolean = {
    t match {
      case WdlTypes.T_Optional(_) => true
      case _                      => false
    }
  }

  def constInputToSArg(expr: Option[TAT.Expr],
                       cVar: CVar,
                       env: CallEnv,
                       locked: Boolean,
                       callFqn: String): SArg = {
    expr match {
      case None if isOptional(cVar.wdlType) =>
        // optional argument that is not provided
        IR.SArgEmpty

      case None if cVar.default.isDefined =>
        // argument that has a default, it can be omitted in the call
        IR.SArgEmpty

      case None if locked =>
        val envDbg = env
          .map {
            case (name, lVar) =>
              s"  $name -> ${lVar.sArg}"
          }
          .mkString("\n")
        logger.trace(s"""|env =
                         |$envDbg""".stripMargin)
        throw new Exception(
            s"""|input <${cVar.name}, ${cVar.wdlType}> to call <${callFqn}>
                |is unspecified. This is illegal in a locked workflow.""".stripMargin
              .replaceAll("\n", " ")
        )

      case None =>
        // Perhaps the callee is not going to use the argument, lets not fail
        // right here, but leave it for runtime.
        IR.SArgEmpty

      case Some(_: TAT.ValueNone) =>
        // same as above
        IR.SArgEmpty

      case Some(expr) if WdlValueAnalysis.isExpressionConst(cVar.wdlType, expr) =>
        IR.SArgConst(WdlValueAnalysis.evalConst(cVar.wdlType, expr))

      case Some(TAT.ExprIdentifier(id, _, _)) =>
        env.get(id) match {
          case None =>
            val envDbg = env
              .map {
                case (name, lVar) =>
                  s"  $name -> ${lVar.sArg}"
              }
              .mkString("\n")
            logger.trace(s"""|env =
                             |$envDbg""".stripMargin)
            throw new Exception(
                s"""|Internal compiler error.
                    |
                    |Input <${cVar.name}, ${cVar.wdlType}> to call <${callFqn}>
                    |is missing from the environment. We don't have ${id} in the environment.
                    |""".stripMargin
                  .replaceAll("\n", " ")
            )
          case Some(lVar) => lVar.sArg
        }

      case Some(TAT.ExprAt(expr, index, _, _)) =>
        val indexValue = constInputToSArg(Some(index), cVar, env, locked, callFqn) match {
          case IR.SArgConst(WdlValues.V_Int(value))                                 => value
          case IR.SArgWorkflowInput(CVar(_, _, Some(WdlValues.V_Int(value)), _), _) => value
          case other =>
            throw new Exception(
                s"Array index expression ${other} is not a constant nor an identifier"
            )
        }
        val value = constInputToSArg(Some(expr), cVar, env, locked, callFqn) match {
          case IR.SArgConst(WdlValues.V_Array(arrayValue)) if arrayValue.size > indexValue =>
            arrayValue(indexValue)
          case IR.SArgWorkflowInput(CVar(_, _, Some(WdlValues.V_Array(arrayValue)), _), _)
              if arrayValue.size > indexValue =>
            arrayValue(indexValue)
          case other =>
            throw new Exception(
                s"Left-hand side expression ${other} is not a constant nor an identifier"
            )
        }
        IR.SArgConst(value)

      case Some(TAT.ExprGetName(TAT.ExprIdentifier(id, _, _), field, _, _))
          if env.contains(s"$id.$field") =>
        env(s"$id.$field").sArg

      case Some(TAT.ExprGetName(expr, id, _, _)) =>
        val lhs = constInputToSArg(Some(expr), cVar, env, locked, callFqn)
        val lhsWdlValue = lhs match {
          case IR.SArgConst(wdlValue)                                 => wdlValue
          case IR.SArgWorkflowInput(cVar, _) if cVar.default.nonEmpty => cVar.default.get
          case other =>
            throw new Exception(
                s"Left-hand side expression ${other} is not a constant nor an identifier"
            )
        }
        val wdlValue = lhsWdlValue match {
          case WdlValues.V_Object(members) if members.contains(id) => members(id)
          case WdlValues.V_Pair(l, r) =>
            id match {
              case "left"  => l
              case "right" => r
              case _ =>
                throw new Exception(s"Pair member name must be 'left' or 'right', not ${id}")
            }
          case WdlValues.V_Call(_, members) if members.contains(id)   => members(id)
          case WdlValues.V_Struct(_, members) if members.contains(id) => members(id)
          case other =>
            throw new Exception(s"Cannot resolve id ${id} for value ${other}")
        }
        IR.SArgConst(wdlValue)

      case Some(expr) =>
        throw new Exception(s"Expression $expr is not a constant nor an identifier")
    }
  }

  // compile a call into a stage in an IR.Workflow
  //
  // In a call like:
  //   call lib.native_mk_list as mk_list {
  //     input: a=x, b=5
  //   }
  // it maps callee input <a> to expression <x>. The expressions are
  // trivial because this is a case where we can directly call an applet.
  //
  private def compileCall(call: TAT.Call, env: CallEnv, locked: Boolean): IR.Stage = {
    // Find the callee
    val calleeName = call.unqualifiedName
    val callee: IR.Callable = callables.get(calleeName) match {
      case None =>
        throw new Exception(s"""|Callable ${calleeName} should exist
                                |but is missing from the list of known tasks/workflows ${callables.keys}
                                |""".stripMargin)
      case Some(x) => x
    }

    // Extract the input values/links from the environment
    val inputs: Vector[SArg] = callee.inputVars.map(cVar =>
      constInputToSArg(call.inputs.get(cVar.name), cVar, env, locked, call.fullyQualifiedName)
    )

    val stageName = call.actualName
    IR.Stage(stageName, genStageId(), calleeName, inputs, callee.outputVars)
  }

  // Check if the environment has a variable with a binding for
  // a fully-qualified name. For example, if fqn is "A.B.C", then
  // look for "A.B.C", "A.B", or "A", in that order.
  //
  // If the environment has a pair "p", then we want to be able to
  // to return "p" when looking for "p.left" or "p.right".
  //
  @scala.annotation.tailrec
  private def lookupInEnv(fqn: String, env: CallEnv): Option[(String, LinkedVar)] = {
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

  private def getSArgFromEnv(source: String, env: CallEnv): SArg = {
    env.get(source) match {
      case None =>
        val envDesc = env.mkString("\n")
        logger.trace(s"""|env=[
                         |$envDesc
                         |]""".stripMargin)
        throw new Exception(s"Could not find $source in the workflow environment")
      case Some(lVar) => lVar.sArg
    }
  }

  // Find the closure of the input nodes. Do not include the inputs themselves. Create an input
  // node for each of these external references.
  private def inputNodeClosure(inputNodes: Vector[Block.InputDefinition],
                               subBlocks: Vector[Block]): Map[String, (WdlTypes.T, Boolean)] = {
    val allInputs: Vector[Block.InputDefinition] = subBlocks.flatMap { block =>
      block.inputs
    }
    val allInputs2: Map[String, (WdlTypes.T, Boolean)] = allInputs.map { bInput =>
      bInput.name -> (bInput.wdlType, Block.isOptional(bInput))
    }.toMap

    val regularInputNames: Set[String] = inputNodes.map(_.name).toSet

    // remove the regular inputs
    allInputs2.filter {
      case (name, _) =>
        !(regularInputNames contains name)
    }
  }

  // A block inside a conditional or scatter. If it is simple,
  // we can use a direct call. Otherwise, recursively call into the asssemble-backbone method, and
  // get a locked subworkflow.
  private def compileNestedBlock(wfName: String,
                                 statements: Vector[TAT.WorkflowElement],
                                 blockPath: Vector[Int],
                                 env: CallEnv): (IR.Callable, Vector[IR.Callable]) = {
    val (inputNodes, subBlocks, outputNodes) = Block.split(statements)
    assert(subBlocks.nonEmpty)

    if (subBlocks.size == 1) {
      Block.categorize(subBlocks(0)) match {
        case Block.CallDirect(_, _) | Block.CallWithSubexpressions(_, _) =>
          throw new RuntimeException("Single call not expected in nested block")
        case _ => ()
      }

      // At runtime, we will need to execute a workflow
      // fragment. This requires an applet.
      //
      // This is a recursive call, to compile a  potentially
      // complex sub-block. It could have many calls generating
      // many applets and subworkflows.
      val (stage, aux) = compileWfFragment(wfName, subBlocks(0), blockPath :+ 0, env)
      val fragName = stage.calleeName
      val main = aux.find(_.name == fragName) match {
        case None    => throw new Exception(s"Could not find $fragName")
        case Some(x) => x
      }
      (main, aux)
    } else {
      // there are several subblocks, we need a subworkflow to string them
      // together.
      //
      // The subworkflow may access declarations outside of its scope.
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
      val closureInputs = inputNodeClosure(inputNodes, subBlocks)
      logger.trace(
          s"""|compileNestedBlock
              |    inputNodes = $inputNodes
              |    closureInputs= $closureInputs
              |""".stripMargin
      )
      val (subwf, auxCallables, _) = compileWorkflowLocked(wfName + "_block_" + pathStr,
                                                           inputNodes,
                                                           closureInputs,
                                                           outputNodes,
                                                           blockPath,
                                                           subBlocks,
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
                                env: CallEnv): (IR.Stage, Vector[IR.Callable]) = {
    val stageName = block.makeName match {
      case None       => IR.EVAL_STAGE
      case Some(name) => name
    }
    logger.trace(s"Compiling fragment <$stageName> as stage")
    logger.withTraceIfContainsKey("GenerateIR").trace(s"""|block=
                                                          |${PrettyPrintApprox.block(block)}
                                                          |""".stripMargin)

    // Figure out the closure required for this block, out of the environment
    // Find the closure of a block, all the variables defined earlier
    // that are required for the calculation.
    //
    // Note: some referenced variables may be undefined. This could be because they are:
    // 1) optional
    // 2) defined -inside- the block
    val closure = block.inputs.flatMap { i: Block.InputDefinition =>
      lookupInEnv(i.name, env) match {
        case None               => None
        case Some((name, lVar)) => Some((name, lVar))
      }
    }.toMap

    val inputVars: Vector[CVar] = closure.map {
      case (fqn, LinkedVar(cVar, _)) =>
        cVar.copy(name = fqn)
    }.toVector

    // A reversible conversion mul.result --> mul___result. This
    // assumes the '___' symbol is not used anywhere in the original WDL script.
    //
    // This is a simplifying assumption, that is hopefully sufficient. It disallows
    // users from using variables with the ___ character sequence.
    val fqnDictTypes = inputVars.map { cVar =>
      cVar.dxVarName -> cVar.wdlType
    }.toMap

    // Figure out the block outputs
    val outputs: Map[String, WdlTypes.T] = block.outputs.map { bOut =>
      bOut.name -> bOut.wdlType
    }.toMap

    // create a cVar definition from each block output. The dx:stage
    // will output these cVars.
    val outputVars = outputs.map {
      case (fqn, wdlType) =>
        CVar(fqn, wdlType, None)
    }.toVector

    // The fragment runner can only handle a single call. If the
    // block already has exactly one call, then we are good. If
    // it contains a scatter/conditional with several calls,
    // then compile the inner block into a sub-workflow.
    //
    // Figure out the name of the callable; we need to link with it when we
    // get to the native phase.
    val catg = Block.categorize(block)
    logger
      .withTraceIfContainsKey("GenerateIR")
      .trace(s"""|category : ${Block.Category.toString(catg)}
                 |""".stripMargin)

    val (innerCall, auxCallables): (Option[String], Vector[IR.Callable]) = catg match {
      case Block.AllExpressions(_) => (None, Vector.empty)
      case Block.CallDirect(_, _) =>
        throw new Exception(s"a direct call should not reach this stage")

      // A block with no nested sub-blocks, and a single call.
      case Block.CallWithSubexpressions(_, cNode) =>
        (Some(cNode.unqualifiedName), Vector.empty)
      case Block.CallFragment(_, cNode) =>
        (Some(cNode.unqualifiedName), Vector.empty)

      // A conditional/scatter with exactly one call in the sub-block.
      // Can be executed by a fragment.
      case Block.CondOneCall(_, _, cNode) =>
        (Some(cNode.unqualifiedName), Vector.empty)
      case Block.ScatterOneCall(_, _, cNode) =>
        (Some(cNode.unqualifiedName), Vector.empty)

      case Block.CondFullBlock(_, condNode) =>
        val (callable, aux) = compileNestedBlock(wfName, condNode.body, blockPath, env)
        (Some(callable.name), aux :+ callable)

      case Block.ScatterFullBlock(_, sctNode) =>
        // add the iteration variable to the inner environment
        val iterWdlType = sctNode.expr.wdlType match {
          case WdlTypes.T_Array(t, _) => t
          case _                      => throw new Exception("scatter doesn't have an array expression")
        }
        val cVar = CVar(sctNode.identifier, iterWdlType, None)
        val innerEnv = env + (sctNode.identifier -> LinkedVar(cVar, IR.SArgEmpty))
        val (callable, aux) = compileNestedBlock(wfName, sctNode.body, blockPath, innerEnv)
        (Some(callable.name), aux :+ callable)
    }

    val applet = IR.Applet(
        s"${wfName}_frag_${genFragId()}",
        inputVars,
        outputVars,
        IR.InstanceTypeDefault,
        IR.DockerImageNone,
        IR.AppletKindWfFragment(innerCall.toVector, blockPath, fqnDictTypes),
        wfStandAlone
    )

    val sArgs: Vector[SArg] = closure.map {
      case (_, LinkedVar(_, sArg)) => sArg
    }.toVector

    (IR.Stage(stageName, genStageId(), applet.name, sArgs, outputVars), auxCallables :+ applet)
  }

  // Assemble the backbone of a workflow, having compiled the independent tasks.
  // This is shared between locked and unlocked workflows.
  // At this point we we have workflow level inputs and outputs.
  // Some of the inputs may have default values that are complex expressions
  // (passed as `wfInputExprs`), necessitating an initial fragment that performs
  // the evaluation. This only applies to locked workflows, since unlocked workflows
  // always have a "common" applet to handle such expressions.
  private def assembleBackbone(
      wfName: String,
      wfInputs: Vector[(CVar, SArg)],
      blockPath: Vector[Int],
      subBlocks: Vector[Block],
      locked: Boolean
  ): (Vector[(IR.Stage, Vector[IR.Callable])], CallEnv) = {
    logger.trace(s"Assembling workflow backbone $wfName")
    val logger2 = logger.withIncTraceIndent()
    val inputNamesDbg = wfInputs.map { case (cVar, _) => cVar.name }
    logger2.trace(s"inputs= $inputNamesDbg")

    var env: CallEnv = wfInputs.map {
      case (cVar, sArg) =>
        cVar.name -> LinkedVar(cVar, sArg)
    }.toMap

    // link together all the stages into a linear workflow
    val allStageInfo: Vector[(IR.Stage, Vector[IR.Callable])] = subBlocks.zipWithIndex.map {
      case (block: Block, blockNum: Int) =>
        val (stage, auxCallables) = Block.categorize(block) match {
          case Block.CallDirect(_, call) =>
            // The block contains exactly one call, with no extra declarations.
            // All the variables are already in the environment, so there
            // is no need to do any extra work. Compile directly into a workflow
            // stage.
            logger2.trace(s"Compiling call ${call.actualName} as stage")
            val stage = compileCall(call, env, locked)
            // Add bindings for the output variables. This allows later calls to refer
            // to these results.
            stage.outputs.foreach { cVar =>
              val fqn = call.actualName ++ "." + cVar.name
              val cVarFqn = cVar.copy(name = fqn)
              env = env + (fqn -> LinkedVar(cVarFqn, IR.SArgLink(stage.id, cVar)))
            }
            (stage, Vector.empty)
          case _ =>
            //     A simple block that requires just one applet,
            // OR: A complex block that needs a subworkflow
            val (stage, auxCallables) = compileWfFragment(wfName, block, blockPath :+ blockNum, env)
            stage.outputs.foreach { cVar =>
              env = env + (cVar.name -> LinkedVar(cVar, IR.SArgLink(stage.id, cVar)))
            }
            (stage, auxCallables)
        }
        (stage, auxCallables)
    }

    if (logger2.containsKey("GenerateIR")) {
      val logger3 = logger2.withTraceIfContainsKey("GenerateIR")
      logger3.trace(s"stages for workflow $wfName = [")
      allStageInfo.foreach {
        case (stage, _) =>
          logger3.trace(
              s"    ${stage.description}, ${stage.id.getId} -> callee=${stage.calleeName}"
          )
      }
      logger3.trace("]")
    }

    (allStageInfo, env)
  }

  private def buildSimpleWorkflowOutput(output: Block.OutputDefinition,
                                        env: CallEnv): (CVar, SArg) = {
    if (env.contains(output.name)) {
      val cVar = CVar(output.name, output.wdlType, None)
      val sArg = getSArgFromEnv(output.name, env)
      (cVar, sArg)
    } else {
      output.expr match {
        case expr if WdlValueAnalysis.isExpressionConst(output.wdlType, expr) =>
          // the output is a constant
          val wdlConst = WdlValueAnalysis.evalConst(output.wdlType, expr)
          val cVar = CVar(output.name, output.wdlType, Some(wdlConst))
          val sArg = IR.SArgConst(wdlConst)
          (cVar, sArg)
        case TAT.ExprIdentifier(id, _, _) =>
          // The output is a reference to a previously defined variable
          val cVar = CVar(output.name, output.wdlType, None)
          val sArg = getSArgFromEnv(id, env)
          (cVar, sArg)
        case TAT.ExprGetName(TAT.ExprIdentifier(id2, _, _), id, _, _) =>
          // The output is a reference to a previously defined variable
          val fqn = s"$id2.$id"
          if (!(env contains fqn))
            throw new Exception(s"Internal error: (${fqn}) is not in the environment")
          val cVar = CVar(output.name, output.wdlType, None)
          val sArg = getSArgFromEnv(fqn, env)
          (cVar, sArg)
        case _ =>
          // An expression that requires evaluation
          throw new Exception(
              s"""|Internal error: (${output.expr}) is a non trivial expression.
                  |It requires constructing an output applet and a stage""".stripMargin
          )
      }
    }
  }

  // Create a preliminary applet to handle workflow input/outputs. This is
  // used only in the absence of workflow-level inputs/outputs.
  private def buildCommonApplet(wfName: String,
                                appletInputs: Vector[CVar],
                                stageInputs: Vector[SArg],
                                outputVars: Vector[CVar]): (IR.Stage, IR.Applet) = {

    val applet = IR.Applet(s"${wfName}_$COMMON",
                           appletInputs,
                           outputVars,
                           IR.InstanceTypeDefault,
                           IR.DockerImageNone,
                           IR.AppletKindWfInputs,
                           wfStandAlone)
    logger.trace(s"Compiling common applet ${applet.name}")

    (IR.Stage(COMMON, genStageId(Some(COMMON)), applet.name, stageInputs, outputVars), applet)
  }

  private def addOutputStatus(outputsVar: Vector[CVar]) = {
    outputsVar :+ CVar(
        REORG_STATUS,
        WdlTypes.T_String,
        Some(WdlValues.V_String(REORG_STATUS_COMPLETE))
    )
  }

  // There are two reasons to be build a special output section:
  // 1. Locked workflow: some of the workflow outputs are expressions.
  //    We need an extra applet+stage to evaluate them.
  // 2. Unlocked workflow: there are no workflow outputs, so we create
  //    them artificially with a separate stage that collects the outputs.
  private def buildOutputStage(wfName: String,
                               outputNodes: Vector[Block.OutputDefinition],
                               env: CallEnv): (IR.Stage, IR.Applet) = {
    // Figure out what variables from the environment we need to pass into the applet.
    // create inputs from outputs
    val outputInputVars: Map[String, LinkedVar] = outputNodes.collect {
      case Block.OutputDefinition(name, _, _) if env.contains(name) => name -> env(name)
    }.toMap
    // create inputs from the closure of the output nodes, which includes (recursively)
    // all the variables in the output node expressions
    val closure = Block.outputClosure(outputNodes)
    val closureInputVars: Map[String, LinkedVar] = closure.map {
      case (name, _) =>
        val lVar = env.get(name) match {
          case None       => throw new Exception(s"could not find variable $name in the environment")
          case Some(lVar) => name -> lVar
        }
        lVar
    }
    val inputVars = (outputInputVars ++ closureInputVars).values.toVector
    logger.trace(s"inputVars=${inputVars.map(_.cVar)}")

    // build definitions of the output variables
    val outputVars: Vector[CVar] = outputNodes
      .foldLeft(Vector.empty[CVar]) {
        case (accu, output) =>
          output.expr match {
            case expr if WdlValueAnalysis.isExpressionConst(output.wdlType, expr) =>
              // the output is a constant
              val wdlConst = WdlValueAnalysis.evalConst(output.wdlType, expr)
              accu :+ CVar(output.name, output.wdlType, Some(wdlConst))
            case _: TAT.ExprIdentifier =>
              // The output is a reference to a previously defined variable
              accu :+ CVar(output.name, output.wdlType, None)
            case _ =>
              // An expression that requires evaluation
              accu :+ CVar(output.name, output.wdlType, None)
          }
      }

    val updatedOutputVars: Vector[CVar] = reorg match {
      case Left(_)             => outputVars
      case Right(_) if locked  => outputVars
      case Right(_) if !locked => addOutputStatus(outputVars)
    }

    val appletKind: IR.AppletKind = reorg match {
      case Left(_) => IR.AppletKindWfOutputs
      // if custom reorg app is used, check if workflow is not locked to ensure that the wf is top level
      // You cannot declare a custom reorg app with a locked workflow.
      // This is checked in Main.scala
      case Right(_) if locked  => IR.AppletKindWfOutputs
      case Right(_) if !locked => IR.AppletKindWfCustomReorgOutputs
    }

    val applet = IR.Applet(s"${wfName}_$OUTPUT_SECTION",
                           inputVars.map(_.cVar),
                           updatedOutputVars,
                           IR.InstanceTypeDefault,
                           IR.DockerImageNone,
                           appletKind,
                           wfStandAlone)

    // define the extra stage we add to the workflow
    (IR.Stage(OUTPUT_SECTION,
              genStageId(Some(OUTPUT_SECTION)),
              applet.name,
              inputVars.map(_.sArg),
              updatedOutputVars),
     applet)
  }

  // Create an applet to reorganize the output files. We want to
  // move the intermediate results to a subdirectory.  The applet
  // needs to process all the workflow outputs, to find the files
  // that belong to the final results.
  private def buildReorgStage(wfName: String,
                              wfOutputs: Vector[(CVar, SArg)]): (IR.Stage, IR.Applet) = {
    // We need minimal compute resources, use the default instance type
    val applet = IR.Applet(
        s"${wfName}_$REORG",
        wfOutputs.map { case (cVar, _) => cVar },
        Vector.empty,
        IR.InstanceTypeDefault,
        IR.DockerImageNone,
        IR.AppletKindWorkflowOutputReorg,
        wfStandAlone
    )
    logger.trace(s"Compiling output reorganization applet ${applet.name}")

    // Link to the X.y original variables
    val inputs: Vector[IR.SArg] = wfOutputs.map { case (_, sArg) => sArg }

    (IR.Stage(REORG, genStageId(Some(REORG)), applet.name, inputs, Vector.empty[CVar]), applet)
  }

  private def addCustomReorgStage(wfOutputs: Vector[(CVar, SArg)],
                                  reorgAttributes: ReorgAttrs): (IR.Stage, IR.Applet) = {

    val appletKind = IR.AppletKindWorkflowCustomReorg(reorgAttributes.appId)

    // will throw error if there is no status string. Should consider checking there i s only one.
    val (reorgStatusCvar, reorgStatusSArg): (CVar, SArg) = wfOutputs.filter {
      case (x, _) => x.name == REORG_STATUS
    }.head

    val configFile: Option[WdlValues.V_File] = reorgAttributes.reorgConf match {
      case ""        => None
      case x: String => Some(WdlValues.V_File(x))
    }

    val appInputs = Vector(
        reorgStatusCvar,
        CVar(REORG_CONFIG, WdlTypes.T_File, configFile)
    )

    val applet = IR.Applet(
        reorgAttributes.appId,
        appInputs,
        Vector.empty,
        IR.InstanceTypeDefault,
        IR.DockerImageNone,
        appletKind,
        wfStandAlone
    )

    logger.trace(s"Adding custom output reorganization applet ${reorgAttributes.appId}")

    // Link to the X.y original variables

    val inputs: Vector[IR.SArg] = configFile match {
      case Some(x) => Vector(reorgStatusSArg, SArgConst(x))
      case _       => Vector(reorgStatusSArg)
    }

    (IR.Stage(REORG, genStageId(Some(REORG)), applet.name, inputs, Vector.empty[CVar]), applet)
  }

  private def unwrapWorkflowMeta(): Vector[IR.WorkflowAttr] = {
    val kvs: Map[String, TAT.MetaValue] = wf.meta match {
      case None                          => Map.empty
      case Some(TAT.MetaSection(kvs, _)) => kvs
    }
    val wfAttrs = kvs.flatMap {
      case (IR.META_TITLE, TAT.MetaValueString(text, _)) => Some(IR.WorkflowAttrTitle(text))
      case (IR.META_DESCRIPTION, TAT.MetaValueString(text, _)) =>
        Some(IR.WorkflowAttrDescription(text))
      case (IR.META_SUMMARY, TAT.MetaValueString(text, _)) => Some(IR.WorkflowAttrSummary(text))
      case (IR.META_VERSION, TAT.MetaValueString(text, _)) => Some(IR.WorkflowAttrVersion(text))
      case (IR.META_DETAILS, TAT.MetaValueObject(fields, _)) =>
        Some(IR.WorkflowAttrDetails(ParameterMeta.translateMetaKVs(fields)))
      case (IR.META_TYPES, TAT.MetaValueArray(array, _)) =>
        Some(IR.WorkflowAttrTypes(array.map {
          case TAT.MetaValueString(text, _) => text
          case other                        => throw new Exception(s"Invalid type: $other")
        }))
      case (IR.META_TAGS, TAT.MetaValueArray(array, _)) =>
        Some(IR.WorkflowAttrTags(array.map {
          case TAT.MetaValueString(text, _) => text
          case other                        => throw new Exception(s"Invalid tag: $other")
        }))
      case (IR.META_PROPERTIES, TAT.MetaValueObject(fields, _)) =>
        Some(IR.WorkflowAttrProperties(fields.view.mapValues {
          case TAT.MetaValueString(text, _) => text
          case other                        => throw new Exception(s"Invalid property value: $other")
        }.toMap))
      case (IR.META_CALL_NAMES, TAT.MetaValueObject(fields, _)) =>
        Some(IR.WorkflowAttrCallNames(fields.view.mapValues {
          case TAT.MetaValueString(text, _) => text
          case other                        => throw new Exception(s"Invalid call name value: $other")
        }.toMap))
      case (IR.META_RUN_ON_SINGLE_NODE, TAT.MetaValueBoolean(value, _)) =>
        Some(IR.WorkflowAttrRunOnSingleNode(value))
      case _ => None
    }.toVector

    // Fill in missing attributes from adjunct files
    wfAttrs ++ (adjunctFiles match {
      case Some(adj) =>
        adj.flatMap {
          case Adjuncts.Readme(text) if !wf.meta.exists(_.kvs.contains(IR.META_DESCRIPTION)) =>
            Some(IR.WorkflowAttrDescription(text))
          case _ => None
        }
      case None => Vector.empty
    })
  }

  /**
    * Compile a top-level locked workflow.
    */
  private def compileTopWorkflowLocked(
      inputNodes: Vector[Block.InputDefinition],
      outputNodes: Vector[Block.OutputDefinition],
      subBlocks: Vector[Block]
  ): (IR.Workflow, Vector[IR.Callable], Vector[(CVar, SArg)]) = {
    compileWorkflowLocked(wf.name,
                          inputNodes,
                          Map.empty,
                          outputNodes,
                          Vector.empty,
                          subBlocks,
                          IR.Level.Top)
  }

  /**
    * Compile a locked workflow. This is called at the top level for locked workflows,
    * and it is always called for nested workflows regarless of whether the top level
    * is locked.
    */
  private def compileWorkflowLocked(
      wfName: String,
      inputNodes: Vector[Block.InputDefinition],
      closureInputs: Map[String, (WdlTypes.T, Boolean)],
      outputNodes: Vector[Block.OutputDefinition],
      blockPath: Vector[Int],
      subBlocks: Vector[Block],
      level: IR.Level.Value
  ): (IR.Workflow, Vector[IR.Callable], Vector[(CVar, SArg)]) = {
    // translate wf inputs, and also get a Vector of any non-constant
    // expressions that need to be evaluated in the common stage
    val (wfInputs, wfInputExprs): (Vector[(CVar, SArg)], Vector[Option[TAT.Expr]]) =
      inputNodes.map { iNode =>
        buildWorkflowInput(iNode) match {
          case (cVar, None) =>
            ((cVar, IR.SArgWorkflowInput(cVar)), None)
          case (cVar, Some(expr)) =>
            // the workflow input default value is a complex expression
            // that requires evaluation at runtime - we will need to add
            // a WfFrag to do the evaluation
            ((cVar, IR.SArgWorkflowInput(cVar, dynamicDefault = true)), Some(expr))
        }
      }.unzip
    // inputs that are a result of accessing declarations in an encompassing
    // WDL workflow.
    val clsInputs: Vector[(CVar, SArg)] = closureInputs.map {
      case (name, (wdlType, false)) =>
        // no default value
        val cVar = CVar(name, wdlType, None)
        (cVar, IR.SArgWorkflowInput(cVar))
      case (name, (wdlType, true)) =>
        // there is a default value. This input is de facto optional.
        // We change the type of the CVar and make sure it is optional.
        val cVar =
          if (isOptional(wdlType)) {
            CVar(name, wdlType, None)
          } else {
            CVar(name, WdlTypes.T_Optional(wdlType), None)
          }
        (cVar, IR.SArgWorkflowInput(cVar))
    }.toVector
    val allWfInputs = wfInputs ++ clsInputs
    // If the workflow has inputs that are defined with complex expressions,
    // we need to build an initial applet to evaluate those.
    // TODO: In v1, this was done with a fragment - an applet that does the evaluation
    //  and calls the applet to execute the workflow via subjob. Here we are instead
    //  using the "common applet" mechanism employed for unlocked workflows. I believe
    //  the v1 way of doing it was problematic because if, for example, there are two
    //  calls that both depend on the complex inputs, the second would be dependent on
    //  the completion of the first (and thus of the first's subjob). But we should
    //  evaluate whether there are any downsides to doing it the "common applet" way.
    val (backboneInputs, initialStageInfo) = if (wfInputExprs.flatten.nonEmpty) {
      val commonAppletInputs = allWfInputs.map(_._1)
      val commonStageInputs = allWfInputs.map(_._2)
      val inputOutputs: Vector[CVar] = inputNodes.map {
        case Block.OverridableInputDefinitionWithDynamicDefault(name, wdlType, _) =>
          val nonOptType = wdlType match {
            case WdlTypes.T_Optional(t) => t
            case t                      => t
          }
          CVar(name, nonOptType, None, None)
        case i: Block.InputDefinition =>
          CVar(i.name, i.wdlType, None, None)
      }
      val closureOutputs: Vector[CVar] = clsInputs.map(_._1)
      val (commonStage, commonApplet) =
        buildCommonApplet(wf.name,
                          commonAppletInputs,
                          commonStageInputs,
                          inputOutputs ++ closureOutputs)
      val fauxWfInputs: Vector[(CVar, SArg)] = commonStage.outputs.map { cVar: CVar =>
        val sArg = IR.SArgLink(commonStage.id, cVar)
        (cVar, sArg)
      }
      (fauxWfInputs, Vector((commonStage, Vector(commonApplet))))
    } else {
      (allWfInputs, Vector.empty)
    }
    // translate the Block(s) into workflow stages
    val (backboneStageInfo, env) =
      assembleBackbone(wfName, backboneInputs, blockPath, subBlocks, locked = true)
    val (stages, auxCallables) = (initialStageInfo ++ backboneStageInfo).unzip
    // translate workflow-level metadata to IR
    val wfAttr = unwrapWorkflowMeta()
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
    val definedVars = env.keys.toSet
    if (outputNodes.forall(oNode => Block.isSimpleOutput(oNode, definedVars)) &&
        Block.inputsUsedAsOutputs(inputNodes, outputNodes).isEmpty) {
      val simpleWfOutputs = outputNodes.map(node => buildSimpleWorkflowOutput(node, env))
      val irwf =
        IR.Workflow(wfName,
                    allWfInputs,
                    simpleWfOutputs,
                    stages,
                    wf,
                    locked = true,
                    level,
                    Some(wfAttr))
      (irwf, auxCallables.flatten, simpleWfOutputs)
    } else {
      // Some of the outputs are expressions. We need an extra applet+stage
      // to evaluate them.
      val (outputStage, outputApplet) = buildOutputStage(wfName, outputNodes, env)
      val wfOutputs = outputStage.outputs.map { cVar =>
        (cVar, IR.SArgLink(outputStage.id, cVar))
      }
      val irwf = IR.Workflow(wfName,
                             allWfInputs,
                             wfOutputs,
                             stages :+ outputStage,
                             wf,
                             locked = true,
                             level,
                             Some(wfAttr))
      (irwf, auxCallables.flatten :+ outputApplet, wfOutputs)
    }
  }

  /**
    * Compile a "regular" (i.e. unlocked) workflow. This function only get's
    * called at the top-level.
    */
  private def compileTopWorkflowUnlocked(
      inputNodes: Vector[Block.InputDefinition],
      outputNodes: Vector[Block.OutputDefinition],
      subBlocks: Vector[Block]
  ): (IR.Workflow, Vector[IR.Callable], Vector[(CVar, SArg)]) = {
    // Create a special applet+stage for the inputs. This is a substitute for
    // workflow inputs. We now call the workflow inputs, "fauxWfInputs". This is because
    // they are references to the outputs of this first applet.

    // compile into dx:workflow inputs
    val commonAppletInputs: Vector[CVar] = inputNodes.map(iNode => buildWorkflowInput(iNode)._1)
    val commonStageInputs: Vector[SArg] = inputNodes.map { _ =>
      IR.SArgEmpty
    }
    val (commonStg, commonApplet) =
      buildCommonApplet(wf.name, commonAppletInputs, commonStageInputs, commonAppletInputs)
    val fauxWfInputs: Vector[(CVar, SArg)] = commonStg.outputs.map { cVar: CVar =>
      val sArg = IR.SArgLink(commonStg.id, cVar)
      (cVar, sArg)
    }

    val (allStageInfo, env) =
      assembleBackbone(wf.name, fauxWfInputs, Vector.empty, subBlocks, locked = false)
    val (stages: Vector[IR.Stage], auxCallables) = allStageInfo.unzip

    // convert the outputs into an applet+stage
    val (outputStage, outputApplet) = buildOutputStage(wf.name, outputNodes, env)

    val wfInputs = commonAppletInputs.map(cVar => (cVar, IR.SArgEmpty))
    val wfOutputs = outputStage.outputs.map { cVar =>
      (cVar, IR.SArgLink(outputStage.id, cVar))
    }
    val wfAttr = unwrapWorkflowMeta()
    val irwf = IR.Workflow(wf.name,
                           wfInputs,
                           wfOutputs,
                           commonStg +: stages :+ outputStage,
                           wf,
                           locked = false,
                           IR.Level.Top,
                           Some(wfAttr))
    (irwf, commonApplet +: auxCallables.flatten :+ outputApplet, wfOutputs)
  }

  // Compile a (single) user defined WDL workflow into a dx:workflow.
  //
  private def apply2(): (IR.Workflow, Vector[IR.Callable]) = {
    logger.trace(s"compiling workflow ${wf.name}")

    // Create a stage per workflow body element (declaration block, call,
    // scatter block, conditional block)
    val subBlocks = Block.splitWorkflow(wf)
    // translate workflow inputs/outputs to equivalent classes defined in Block
    val inputs = wf.inputs.map(Block.translate)
    val outputs = wf.outputs.map(Block.translate)

    // compile into an IR workflow
    val (irwf, irCallables, wfOutputs) =
      if (locked) {
        compileTopWorkflowLocked(inputs, outputs, subBlocks)
      } else {
        compileTopWorkflowUnlocked(inputs, outputs, subBlocks)
      }

    // Add a workflow reorg applet if necessary
    val (wf2: IR.Workflow, apl2: Vector[IR.Callable]) = reorg match {
      case Left(reorg_flag) =>
        if (reorg_flag) {
          val (reorgStage, reorgApl) = buildReorgStage(wf.name, wfOutputs)
          (irwf.copy(stages = irwf.stages :+ reorgStage), irCallables :+ reorgApl)
        } else {
          (irwf, irCallables)
        }

      // Only the top level workflow will have a custom reorg stage.
      // All subworkflow are locked.
      // Cannot use custom reorg stage with --locked flag.
      // This is checked in Main.scala.
      case Right(reorgAttributes) =>
        if (!locked) {
          val (reorgStage, reorgApl) = addCustomReorgStage(wfOutputs, reorgAttributes)
          (irwf.copy(stages = irwf.stages :+ reorgStage), irCallables :+ reorgApl)
        } else {
          (irwf, irCallables)
        }
    }

    (wf2, apl2)
  }

  // TODO: use RuntimeExceptions for assertions
  def apply(): (IR.Workflow, Vector[IR.Callable]) = {
    val (irwf, irCallables) = apply2()

    val callableNames: Set[String] =
      irCallables.map(_.name).toSet ++ callables.keySet

    irwf.stages.foreach { stage =>
      if (!callableNames.contains(stage.calleeName)) {
        val allStages = irwf.stages
          .map(stg => s"$stg.description, $stg.id.getId")
          .mkString("    ")
        throw new Exception(s"""|Generated bad workflow.
                                |Stage <${stage.id.getId}, ${stage.description}> calls <${stage.calleeName}>
                                |which is missing.
                                |
                                |stages = $allStages
                                |callables = $callableNames
                                |""".stripMargin)
      }
    }

    (irwf, irCallables)
  }
}
