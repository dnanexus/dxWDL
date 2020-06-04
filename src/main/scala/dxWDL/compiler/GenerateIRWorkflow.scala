/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import wdlTools.eval.WdlValues
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.types.WdlTypes

import dxWDL.base.{Language, _}
import dxWDL.dx._
import dxWDL.util._
import IR.{COMMON, CVar, OUTPUT_SECTION, REORG, SArg, SArgConst}

case class GenerateIRWorkflow(wf: TAT.Workflow,
                              wfSourceCode: String,
                              wfSourceStandAlone: String,
                              callables: Map[String, IR.Callable],
                              language: Language.Value,
                              verbose: Verbose,
                              locked: Boolean,
                              reorg: Either[Boolean, ReorgAttrs],
                              adjunctFiles: Option[Vector[Adjuncts.AdjunctFile]]) {
  val verbose2: Boolean = verbose.containsKey("GenerateIR")

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
  // We handle only the case where the default is a constant.
  def buildWorkflowInput(input: Block.InputDefinition): CVar = {
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
        CVar(id, wdlType, None, attr)
      case Block.OverridableInputDefinitionWithDefault(id, womType, defaultExpr) =>
        val defaultValue: WdlValues.V = WomValueAnalysis.ifConstEval(womType, defaultExpr) match {
          case None        => throw new Exception(s"""|default expression in input should be a constant
                                               | $defaultExpr
                                               |""".stripMargin)
          case Some(value) => value
        }
        CVar(id, womType, Some(defaultValue), attr)
      case Block.OptionalInputDefinition(id, womType) =>
        CVar(id, womType, None, attr)
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
    val calleeName = Utils.getUnqualifiedName(call.callee.name)
    val callee: IR.Callable = callables.get(calleeName) match {
      case None =>
        throw new Exception(s"""|sanity: callable ${calleeName} should exist
                                |but is missing from the list of known tasks/workflows ${callables.keys}
                                |""".stripMargin)
      case Some(x) => x
    }

    // Extract the input values/links from the environment
    val inputs: Vector[SArg] = callee.inputVars.map { cVar =>
      call.inputs.get(cVar.name) match {
        case None if Utils.isOptional(cVar.womType) =>
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
          Utils.trace(verbose.on, s"""|env =
                                      |$envDbg""".stripMargin)
          throw new Exception(
              s"""|input <${cVar.name}, ${cVar.womType}> to call <${call.fullyQualifiedName}>
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

        case Some(expr) if WomValueAnalysis.isExpressionConst(cVar.womType, expr) =>
          IR.SArgConst(WomValueAnalysis.evalConst(cVar.womType, expr))

        case Some(TAT.ExprIdentifier(id, _, _)) =>
          env.get(id) match {
            case None =>
              val envDbg = env
                .map {
                  case (name, lVar) =>
                    s"  $name -> ${lVar.sArg}"
                }
                .mkString("\n")
              Utils.trace(verbose.on, s"""|env =
                                          |$envDbg""".stripMargin)
              throw new Exception(
                  s"""|Internal compiler error.
                      |
                      |Input <${cVar.name}, ${cVar.womType}> to call <${call.fullyQualifiedName}>
                      |is missing from the environment.""".stripMargin
                    .replaceAll("\n", " ")
              )
            case Some(lVar) => lVar.sArg
          }
        case Some(expr) =>
          throw new Exception(s"Expression $expr is not a constant nor an identifier")
      }
    }

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
  private def lookupInEnvInner(fqn: String, env: CallEnv): Option[(String, LinkedVar)] = {
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

  private def getSArgFromEnv(source: String, env: CallEnv): SArg = {
    env.get(source) match {
      case None =>
        val envDesc = env.mkString("\n")
        Utils.trace(verbose.on, s"""|env=[
                                    |$envDesc
                                    |]""".stripMargin)
        throw new Exception(s"Sanity: could not find $source in the workflow environment")
      case Some(lVar) => lVar.sArg
    }
  }

  // Lookup in the environment. Provide a human readable error message
  // if the fully-qualified-name is not found.
  private def lookupInEnv(fqn: String,
                          womType: WdlTypes.T,
                          env: CallEnv,
                          optional: Boolean): Option[(String, LinkedVar)] = {
    lookupInEnvInner(fqn, env) match {
      case None if Utils.isOptional(womType) =>
        None
      case None =>
        if (!optional) {
          // A missing compulsory argument
          Utils.warning(verbose, s"Missing argument $fqn, it will have to be provided at runtime")
        }
        None
      case Some((name, lVar)) =>
        Some((name, lVar))
    }
  }

  // Find the closure of a block. All the variables defined earlier
  // that are required for the calculation.
  private def blockClosure(block: Block, env: CallEnv, dbg: String): CallEnv = {
    block.inputs.flatMap { i: Block.InputDefinition =>
      lookupInEnv(i.name, i.wdlType, env, Block.isOptional(i))
    }.toMap
  }

  // Find the closure of a graph, excluding the straightforward inputs. Create an input
  // node for each of these external references.
  private def graphClosure(inputNodes: Vector[Block.InputDefinition],
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
    val (inputNodes, subBlocks, outputNodes) =
      Block.split(statements)
    assert(subBlocks.nonEmpty)

    if (subBlocks.size == 1) {
      Block.categorize(subBlocks(0)) match {
        case Block.CallDirect(_, _) | Block.CallWithSubexpressions(_, _) =>
          throw new Exception("sanity")
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
      val closureInputs = graphClosure(inputNodes, subBlocks)
      Utils.trace(
          verbose.on,
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
      case None       => "eval"
      case Some(name) => name
    }
    Utils.trace(verbose.on, s"Compiling fragment <$stageName> as stage")

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
    val fqnDictTypes = inputVars.map { cVar =>
      cVar.dxVarName -> cVar.womType
    }.toMap

    // Figure out the block outputs
    val outputs: Map[String, WdlTypes.T] = block.outputs.map { bOut =>
      bOut.name -> bOut.wdlType
    }.toMap

    // create a cVar definition from each block output. The dx:stage
    // will output these cVars.
    val outputVars = outputs.map {
      case (fqn, womType) =>
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

    val (innerCall, auxCallables): (Option[String], Vector[IR.Callable]) = catg match {
      case Block.AllExpressions(_) => (None, Vector.empty)
      case Block.CallDirect(_, _) =>
        throw new Exception(s"a direct call should not reach this stage")

      // A block with no nested sub-blocks, and a single call.
      case Block.CallWithSubexpressions(_, cNode) =>
        (Some(Utils.getUnqualifiedName(cNode.callee.name)), Vector.empty)
      case Block.CallFragment(_, cNode) =>
        (Some(Utils.getUnqualifiedName(cNode.callee.name)), Vector.empty)

      // A conditional/scatter with exactly one call in the sub-block.
      // Can be executed by a fragment.
      case Block.CondOneCall(_, _, cNode) =>
        (Some(Utils.getUnqualifiedName(cNode.callee.name)), Vector.empty)
      case Block.ScatterOneCall(_, _, cNode) =>
        (Some(Utils.getUnqualifiedName(cNode.callee.name)), Vector.empty)

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
        wfSourceStandAlone
    )

    val sArgs: Vector[SArg] = closure.map {
      case (_, LinkedVar(_, sArg)) => sArg
    }.toVector

    (IR.Stage(stageName, genStageId(), applet.name, sArgs, outputVars), auxCallables :+ applet)
  }

  // Assemble the backbone of a workflow, having compiled the
  // independent tasks.  This is shared between locked and unlocked
  // workflows. At this point we we have workflow level inputs and
  // outputs.
  private def assembleBackbone(
      wfName: String,
      wfInputs: Vector[(CVar, SArg)],
      blockPath: Vector[Int],
      subBlocks: Vector[Block],
      locked: Boolean
  ): (Vector[(IR.Stage, Vector[IR.Callable])], CallEnv) = {
    Utils.trace(verbose.on, s"Assembling workflow backbone $wfName")
    Utils.traceLevelInc()
    val inputNamesDbg = wfInputs.map { case (cVar, _) => cVar.name }
    Utils.trace(verbose.on, s"inputs= $inputNamesDbg")

    var env: CallEnv = wfInputs.map {
      case (cVar, sArg) =>
        cVar.name -> LinkedVar(cVar, sArg)
    }.toMap

    var allStageInfo = Vector.empty[(IR.Stage, Vector[IR.Callable])]
    var remainingBlocks = subBlocks

    // link together all the stages into a linear workflow
    for (blockNum <- subBlocks.indices) {
      val block = remainingBlocks.head
      remainingBlocks = remainingBlocks.tail

      val (stage, auxCallables) = Block.categorize(block) match {
        case Block.CallDirect(_, call) =>
          // The block contains exactly one call, with no extra declarations.
          // All the variables are already in the environment, so there
          // is no need to do any extra work. Compile directly into a workflow
          // stage.
          Utils.trace(verbose.on, s"Compiling call ${call.actualName} as stage")
          val stage = compileCall(call, env, locked)

          // Add bindings for the output variables. This allows later calls to refer
          // to these results.
          for (cVar <- stage.outputs) {
            val fqn = call.actualName ++ "." + cVar.name
            val cVarFqn = cVar.copy(name = fqn)
            env = env + (fqn -> LinkedVar(cVarFqn, IR.SArgLink(stage.id, cVar)))
          }
          (stage, Vector.empty)

        case _ =>
          //     A simple block that requires just one applet,
          // OR: A complex block that needs a subworkflow
          val (stage, auxCallables) = compileWfFragment(wfName, block, blockPath :+ blockNum, env)
          for (cVar <- stage.outputs) {
            env = env + (cVar.name ->
              LinkedVar(cVar, IR.SArgLink(stage.id, cVar)))
          }
          (stage, auxCallables)
      }
      allStageInfo :+= (stage, auxCallables)
    }

    if (verbose2) {
      Utils.trace(verbose2, s"stages for workflow $wfName = [")
      allStageInfo.foreach {
        case (stage, _) =>
          Utils.trace(
              verbose2,
              s"    ${stage.description}, ${stage.id.getId()} -> callee=${stage.calleeName}"
          )
      }
      Utils.trace(verbose2, "]")
    }
    Utils.traceLevelDec()
    (allStageInfo, env)
  }

  private def buildSimpleWorkflowOutput(output: Block.OutputDefinition,
                                        env: CallEnv): (CVar, SArg) = {
    output.expr match {
      case TAT.ExprIdentifier(id, _, _) =>
        // The output is a reference to a previously defined variable
        val cVar = CVar(output.name, output.wdlType, None)
        val sArg = getSArgFromEnv(id, env)
        (cVar, sArg)
      case expr if WomValueAnalysis.isExpressionConst(output.wdlType, expr) =>
        // the output is a constant
        val womConst = WomValueAnalysis.evalConst(output.wdlType, expr)
        val cVar = CVar(output.name, output.wdlType, Some(womConst))
        val sArg = IR.SArgConst(womConst)
        (cVar, sArg)
      case _ =>
        // An expression that requires evaluation
        throw new Exception(
            s"Internal error: non trivial expressions are handled elsewhere ${output.expr}"
        )
    }
  }

  // Create a preliminary applet to handle workflow input/outputs. This is
  // used only in the absence of workflow-level inputs/outputs.
  private def buildCommonApplet(wfName: String,
                                wfSourceStandAlone: String,
                                inputVars: Vector[CVar]): (IR.Stage, IR.Applet) = {
    val outputVars: Vector[CVar] = inputVars

    val applet = IR.Applet(s"${wfName}_$COMMON",
                           inputVars,
                           outputVars,
                           IR.InstanceTypeDefault,
                           IR.DockerImageNone,
                           IR.AppletKindWfInputs,
                           wfSourceStandAlone)
    Utils.trace(verbose.on, s"Compiling common applet ${applet.name}")

    val sArgs: Vector[SArg] = inputVars.map { _ =>
      IR.SArgEmpty
    }
    (IR.Stage(COMMON, genStageId(Some(COMMON)), applet.name, sArgs, outputVars), applet)
  }

  private def addOutputStatus(outputsVar: Vector[CVar]) = {
    outputsVar :+ CVar(
        Utils.REORG_STATUS,
        WdlTypes.T_String,
        Some(WdlValues.V_String(Utils.REORG_STATUS_COMPLETE))
    )
  }

  // There are two reasons to be build a special output section:
  // 1. Locked workflow: some of the workflow outputs are expressions.
  //    We need an extra applet+stage to evaluate them.
  // 2. Unlocked workflow: there are no workflow outputs, so we create
  //    them artificially with a separate stage that collects the outputs.
  private def buildOutputStage(wfName: String,
                               wfSourceStandAlone: String,
                               outputNodes: Vector[Block.OutputDefinition],
                               env: CallEnv): (IR.Stage, IR.Applet) = {
    // Figure out what variables from the environment we need to pass
    // into the applet.
    val closure = Block.outputClosure(outputNodes)

    val inputVars: Vector[LinkedVar] = closure.map {
      case (name, _) =>
        val lVar = env.find { case (key, _) => key == name } match {
          case None            => throw new Exception(s"could not find variable $name in the environment")
          case Some((_, lVar)) => lVar
        }
        lVar
    }.toVector
    Utils.trace(verbose.on, s"inputVars=${inputVars.map(_.cVar)}")

    // build definitions of the output variables
    val outputVars: Vector[CVar] = outputNodes
      .foldLeft(Vector.empty[CVar]) {
        case (accu, output) =>
          output.expr match {
            case expr if WomValueAnalysis.isExpressionConst(output.wdlType, expr) =>
              // the output is a constant
              val womConst = WomValueAnalysis.evalConst(output.wdlType, expr)
              accu :+ CVar(output.name, output.wdlType, Some(womConst))
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
                           wfSourceStandAlone)

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
                              wfSourceStandAlone: String,
                              wfOutputs: Vector[(CVar, SArg)]): (IR.Stage, IR.Applet) = {
    // We need minimal compute resources, use the default instance type
    val applet = IR.Applet(
        s"${wfName}_$REORG",
        wfOutputs.map { case (cVar, _) => cVar },
        Vector.empty,
        IR.InstanceTypeDefault,
        IR.DockerImageNone,
        IR.AppletKindWorkflowOutputReorg,
        wfSourceStandAlone
    )
    Utils.trace(verbose.on, s"Compiling output reorganization applet ${applet.name}")

    // Link to the X.y original variables
    val inputs: Vector[IR.SArg] = wfOutputs.map { case (_, sArg) => sArg }

    (IR.Stage(REORG, genStageId(Some(REORG)), applet.name, inputs, Vector.empty[CVar]), applet)
  }

  private def addCustomReorgStage(wfName: String,
                                  wfSourceStandAlone: String,
                                  wfOutputs: Vector[(CVar, SArg)],
                                  reorgAttributes: ReorgAttrs): (IR.Stage, IR.Applet) = {

    val appletKind = IR.AppletKindWorkflowCustomReorg(reorgAttributes.appId)

    // will throw error if there is no status string. Should consider checking there i s only one.
    val (reorgStatusCvar, reorgStatusSArg): (CVar, SArg) = wfOutputs.filter {
      case (x, _) => x.name == Utils.REORG_STATUS
    }.head

    val configFile: Option[WdlValues.V_File] = reorgAttributes.reorgConf match {
      case ""        => None
      case x: String => Some(WdlValues.V_File(x))
    }

    val appInputs = Vector(
        reorgStatusCvar,
        CVar(Utils.REORG_CONFIG, WdlTypes.T_File, configFile)
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

  private def compileWorkflowLocked(
      wfName: String,
      inputNodes: Vector[Block.InputDefinition],
      closureInputs: Map[String, (WdlTypes.T, Boolean)],
      outputNodes: Vector[Block.OutputDefinition],
      blockPath: Vector[Int],
      subBlocks: Vector[Block],
      level: IR.Level.Value
  ): (IR.Workflow, Vector[IR.Callable], Vector[(CVar, SArg)]) = {
    val wfInputs: Vector[(CVar, SArg)] = inputNodes.map { iNode =>
      val cVar = buildWorkflowInput(iNode)
      (cVar, IR.SArgWorkflowInput(cVar))
    }

    // inputs that are a result of accessing declarations in an encompassing
    // WDL workflow.
    val clsInputs: Vector[(CVar, SArg)] = closureInputs.map {
      case (name, (womType, false)) =>
        // no default value
        val cVar = CVar(name, womType, None)
        (cVar, IR.SArgWorkflowInput(cVar))
      case (name, (womType, true)) =>
        // there is a default value. This input is de facto optional.
        // We change the type of the CVar and make sure it is optional.
        val cVar =
          if (Utils.isOptional(womType))
            CVar(name, womType, None)
          else
            CVar(name, WdlTypes.T_Optional(womType), None)
        (cVar, IR.SArgWorkflowInput(cVar))
    }.toVector
    val allWfInputs = wfInputs ++ clsInputs

    val (allStageInfo, env) =
      assembleBackbone(wfName, allWfInputs, blockPath, subBlocks, locked = true)
    val (stages, auxCallables) = allStageInfo.unzip
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
    if (outputNodes.forall(Block.isSimpleOutput) &&
        Block.inputsUsedAsOutputs(inputNodes, outputNodes).isEmpty) {
      val simpleWfOutputs = outputNodes.map(node => buildSimpleWorkflowOutput(node, env))
      val irwf =
        IR.Workflow(wfName,
                    allWfInputs,
                    simpleWfOutputs,
                    stages,
                    wfSourceCode,
                    locked = true,
                    level,
                    Some(wfAttr))
      (irwf, auxCallables.flatten, simpleWfOutputs)
    } else {
      // Some of the outputs are expressions. We need an extra applet+stage
      // to evaluate them.
      val (outputStage, outputApplet) =
        buildOutputStage(wfName, wfSourceStandAlone, outputNodes, env)
      val wfOutputs = outputStage.outputs.map { cVar =>
        (cVar, IR.SArgLink(outputStage.id, cVar))
      }
      val irwf = IR.Workflow(wfName,
                             allWfInputs,
                             wfOutputs,
                             stages :+ outputStage,
                             wfSourceCode,
                             locked = true,
                             level,
                             Some(wfAttr))
      (irwf, auxCallables.flatten :+ outputApplet, wfOutputs)
    }
  }

  private def compileWorkflowRegular(
      inputNodes: Vector[Block.InputDefinition],
      outputNodes: Vector[Block.OutputDefinition],
      subBlocks: Vector[Block]
  ): (IR.Workflow, Vector[IR.Callable], Vector[(CVar, SArg)]) = {
    // Create a special applet+stage for the inputs. This is a substitute for
    // workflow inputs. We now call the workflow inputs, "fauxWfInputs". This is because
    // they are references to the outputs of this first applet.

    // compile into dx:workflow inputs
    val wfInputDefs: Vector[CVar] = inputNodes.map(iNode => buildWorkflowInput(iNode))
    val (commonStg, commonApplet) = buildCommonApplet(wf.name, wfSourceStandAlone, wfInputDefs)
    val fauxWfInputs: Vector[(CVar, SArg)] = commonStg.outputs.map { cVar: CVar =>
      val sArg = IR.SArgLink(commonStg.id, cVar)
      (cVar, sArg)
    }

    val (allStageInfo, env) =
      assembleBackbone(wf.name, fauxWfInputs, Vector.empty, subBlocks, locked = false)
    val (stages: Vector[IR.Stage], auxCallables) = allStageInfo.unzip

    // convert the outputs into an applet+stage
    val (outputStage, outputApplet) =
      buildOutputStage(wf.name, wfSourceStandAlone, outputNodes, env)

    val wfInputs = wfInputDefs.map { cVar =>
      (cVar, IR.SArgEmpty)
    }
    val wfOutputs = outputStage.outputs.map { cVar =>
      (cVar, IR.SArgLink(outputStage.id, cVar))
    }
    val wfAttr = unwrapWorkflowMeta()
    val irwf = IR.Workflow(wf.name,
                           wfInputs,
                           wfOutputs,
                           commonStg +: stages :+ outputStage,
                           wfSourceCode,
                           locked = false,
                           IR.Level.Top,
                           Some(wfAttr))
    (irwf, commonApplet +: auxCallables.flatten :+ outputApplet, wfOutputs)
  }

  // Compile a (single) user defined WDL workflow into a dx:workflow.
  //
  private def apply2() = {
    Utils.trace(verbose.on, s"compiling workflow ${wf.name}")

    // Create a stage per call/scatter-block/declaration-block
    val subBlocks = Block.splitWorkflow(wf)
    val inputs = wf.inputs.map(Block.translate)
    val outputs = wf.outputs.map(Block.translate)

    // compile into an IR workflow
    val (irwf, irCallables, wfOutputs) =
      if (locked) {
        compileWorkflowLocked(wf.name,
                              inputs,
                              Map.empty,
                              outputs,
                              Vector.empty,
                              subBlocks,
                              IR.Level.Top)
      } else {
        compileWorkflowRegular(inputs, outputs, subBlocks)
      }

    // Add a workflow reorg applet if necessary
    val (wf2: IR.Workflow, apl2: Vector[IR.Callable]) = reorg match {
      case Left(reorg_flag) =>
        if (reorg_flag) {
          val (reorgStage, reorgApl) = buildReorgStage(wf.name, wfSourceStandAlone, wfOutputs)
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
          val (reorgStage, reorgApl) =
            addCustomReorgStage(wf.name, wfSourceStandAlone, wfOutputs, reorgAttributes)
          (irwf.copy(stages = irwf.stages :+ reorgStage), irCallables :+ reorgApl)
        } else {
          (irwf, irCallables)
        }
    }

    (wf2, apl2)
  }

  def apply(): (IR.Workflow, Vector[IR.Callable]) = {
    val (irwf, irCallables) = apply2()

    // sanity check
    val callableNames: Set[String] =
      irCallables.map(_.name).toSet ++ callables.keySet

    irwf.stages.foreach { stage =>
      if (!(callableNames contains stage.calleeName)) {
        val allStages = irwf.stages
          .map(stg => s"$stg.description, $stg.id.getId")
          .mkString("    ")
        throw new Exception(s"""|Generated bad workflow.
                                |Stage <${stage.id
                                 .getId()}, ${stage.description}> calls <${stage.calleeName}>
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
