package dx.translator.wdl

import dx.api.{DxApi, DxUtils, DxWorkflowStage}
import dx.core.Constants
import dx.translator.RunSpec.{DefaultInstanceType, DxFileDockerImage, NoImage}
import dx.translator.{
  CommonStage,
  CustomReorgSettings,
  DefaultReorgSettings,
  EvalStage,
  OutputStage,
  ReorgSettings,
  ReorgStage
}
import dx.core.ir._
import dx.core.ir.Type._
import dx.core.ir.Value._
import dx.core.Constants.{ReorgStatus, ReorgStatusCompleted}
import dx.core.languages.wdl.{
  OptionalBlockInput,
  OverridableBlockInputWithDynamicDefault,
  OverridableBlockInputWithStaticDefault,
  RequiredBlockInput,
  WdlBlock,
  WdlBlockInput,
  WdlUtils
}
import wdlTools.eval.{Eval, EvalException, EvalPaths}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.types.WdlTypes._
import wdlTools.util.{Adjuncts, FileSourceResolver, Logger}

case class CallableTranslator(wdlBundle: WdlBundle,
                              typeAliases: Map[String, T_Struct],
                              locked: Boolean,
                              defaultRuntimeAttrs: Map[String, Value],
                              reorgAttrs: ReorgSettings,
                              dxApi: DxApi = DxApi.get,
                              fileResolver: FileSourceResolver = FileSourceResolver.get,
                              logger: Logger = Logger.get) {

  private lazy val evaluator: Eval =
    Eval(EvalPaths.empty, Some(wdlBundle.version), fileResolver, logger)
  private lazy val codegen = CodeGenerator(typeAliases, wdlBundle.version, logger)

  private case class WdlTaskTranslator(task: TAT.Task) {
    private lazy val runtime =
      RuntimeTranslator(wdlBundle.version,
                        task.runtime,
                        task.hints,
                        task.meta,
                        defaultRuntimeAttrs,
                        evaluator,
                        dxApi)
    private lazy val adjunctFiles: Vector[Adjuncts.AdjunctFile] =
      wdlBundle.adjunctFiles.getOrElse(task.name, Vector.empty)
    private lazy val meta = ApplicationMetaTranslator(wdlBundle.version, task.meta, adjunctFiles)
    private lazy val parameterMeta = ParameterMetaTranslator(wdlBundle.version, task.parameterMeta)

    private def translateInput(input: TAT.InputParameter): Parameter = {
      val wdlType = input.wdlType
      val irType = WdlUtils.toIRType(wdlType)
      val attrs = parameterMeta.translate(input.name, wdlType)

      input match {
        case TAT.RequiredInputParameter(name, _, _) => {
          // This is a task "input" parameter of the form:
          //     Int y

          if (isOptional(irType)) {
            throw new Exception(s"Required input ${name} cannot have optional type ${wdlType}")
          }
          Parameter(name, irType, attributes = attrs)
        }
        case TAT.OverridableInputParameterWithDefault(name, _, defaultExpr, _) =>
          try {
            // This is a task "input" parameter definition of the form:
            //    Int y = 3
            val defaultValue = evaluator.applyConstAndCoerce(defaultExpr, wdlType)
            Parameter(name, irType, Some(WdlUtils.toIRValue(defaultValue, wdlType)), attrs)
          } catch {
            // This is a task "input" parameter definition of the form:
            //    Int y = x + 3
            // We treat it as an optional input - the runtime system will
            // evaluate the expression if no value is specified.
            case _: EvalException =>
              val optType = Type.ensureOptional(irType)
              Parameter(name, optType, None, attrs)

          }
        case TAT.OptionalInputParameter(name, _, _) =>
          val optType = Type.ensureOptional(irType)
          Parameter(name, optType, None, attrs)
      }
    }

    private def translateOutput(output: TAT.OutputParameter): Parameter = {
      val wdlType = output.wdlType
      val irType = WdlUtils.toIRType(wdlType)
      val defaultValue = {
        try {
          val wdlValue = evaluator.applyConstAndCoerce(output.expr, wdlType)
          Some(WdlUtils.toIRValue(wdlValue, wdlType))
        } catch {
          case _: EvalException => None
        }
      }
      val attr = parameterMeta.translate(output.name, wdlType)
      Parameter(output.name, irType, defaultValue, attr)
    }

    def apply: Application = {
      // If the container is stored as a file on the platform, we need to remove
      // the dxURLs in the runtime section, to avoid a runtime lookup. For example:
      //   dx://dxWDL_playground:/glnexus_internal -> dx://project-xxxx:record-yyyy
      def replaceContainer(runtime: TAT.RuntimeSection,
                           newContainer: String): TAT.RuntimeSection = {
        Set("docker", "container").foreach { key =>
          if (runtime.kvs.contains(key)) {
            return TAT.RuntimeSection(
                runtime.kvs ++ Map(
                    key -> TAT.ValueString(newContainer, T_String, runtime.kvs(key).loc)
                ),
                runtime.loc
            )
          }
        }
        runtime
      }

      logger.trace(s"Translating task ${task.name}")
      val inputs = task.inputs.map(translateInput)
      val outputs = task.outputs.map(translateOutput)
      val instanceType = runtime.translateInstanceType
      val kind = runtime.translateExecutableKind match {
        case Some(native) => native
        case None         => ExecutableKindApplet
      }
      val requirements = runtime.translateRequirements
      val attributes = meta.translate
      val container = runtime.translateContainer
      val cleanedTask: TAT.Task = container match {
        case DxFileDockerImage(_, dxFile) =>
          val dxURL = DxUtils.dxDataObjectToUri(dxFile)
          task.copy(runtime = task.runtime.map(rt => replaceContainer(rt, dxURL)))
        case _ => task
      }
      val standAloneTask = WdlDocumentSource(codegen.createStandAloneTask(cleanedTask))
      Application(
          task.name,
          inputs,
          outputs,
          instanceType,
          container,
          kind,
          standAloneTask,
          attributes,
          requirements
      )
    }
  }

  private case class WdlWorkflowTranslator(wf: TAT.Workflow, dependencies: Map[String, Callable]) {
    private lazy val adjunctFiles: Vector[Adjuncts.AdjunctFile] =
      wdlBundle.adjunctFiles.getOrElse(wf.name, Vector.empty)
    private lazy val meta = WorkflowMetaTranslator(wdlBundle.version, wf.meta, adjunctFiles)
    private lazy val parameterMeta = ParameterMetaTranslator(wdlBundle.version, wf.parameterMeta)
    private lazy val standAloneWorkflow =
      WdlDocumentSource(codegen.standAloneWorkflow(wf, dependencies.values.toVector))
    // Only the toplevel workflow may be unlocked. This happens
    // only if the user specifically compiles it as "unlocked".
    private lazy val isLocked: Boolean = {
      wdlBundle.primaryCallable match {
        case Some(wf2: TAT.Workflow) =>
          (wf.name != wf2.name) || locked
        case _ =>
          true
      }
    }

    private type LinkedVar = (Parameter, StageInput)

    case class CallEnv(env: Map[String, LinkedVar]) {
      def add(key: String, lvar: LinkedVar): CallEnv = {
        CallEnv(env + (key -> lvar))
      }

      def contains(key: String): Boolean = env.contains(key)

      def keys: Set[String] = env.keySet

      def get(key: String): Option[LinkedVar] = env.get(key)

      def apply(key: String): LinkedVar = {
        get(key) match {
          case None =>
            log()
            throw new Exception(s"${key} does not exist in the environment.")
          case Some(lvar) => lvar
        }
      }

      def log(): Unit = {
        if (logger.isVerbose) {
          logger.trace("env:")
          val logger2 = logger.withIncTraceIndent()
          stageInputs.map {
            case (name, stageInput) =>
              logger2.trace(s"$name -> ${stageInput}")
          }
        }
      }

      /**
        * Check if the environment has a variable with a binding for
        * a fully-qualified name. For example, if fqn is "A.B.C", then
        * look for "A.B.C", "A.B", or "A", in that order.
        *
        * If the environment has a pair "p", then we want to be able to
        * to return "p" when looking for "p.left" or "p.right".
        *
        * @param fqn fully-qualified name
        * @return
        */
      def lookup(fqn: String): Option[(String, LinkedVar)] = {
        if (env.contains(fqn)) {
          // exact match
          Some(fqn, env(fqn))
        } else {
          // A.B.C --> A.B
          fqn.lastIndexOf(".") match {
            case pos if pos >= 0 => lookup(fqn.substring(0, pos))
            case _               => None
          }
        }
      }

      def stageInputs: Map[String, StageInput] = {
        env.map {
          case (key, (_, stageInput)) => key -> stageInput
        }
      }
    }

    object CallEnv {
      def fromLinkedVars(lvars: Vector[LinkedVar]): CallEnv = {
        CallEnv(lvars.map {
          case (parameter, stageInput) =>
            parameter.name -> (parameter, stageInput)
        }.toMap)
      }
    }

    /**
      * Represents each workflow input with:
      * 1. Parameter, for type declarations
      * 2. StageInput, connecting it to the source of the input
      *
      * It is possible to provide a default value to a workflow input.
      * For example:
      *  workflow w {
      *    Int? x = 3
      *    String s = "glasses"
      *    ...
      *  }
      * @param input the workflow input
      * @return (parameter, isDynamic)
      */
    private def createWorkflowInput(input: WdlBlockInput): (Parameter, Boolean) = {
      val wdlType = input.wdlType
      val irType = WdlUtils.toIRType(wdlType)
      val attr = parameterMeta.translate(input.name, input.wdlType)
      input match {
        case RequiredBlockInput(name, _) =>
          if (Type.isOptional(irType)) {
            throw new Exception(s"Required input ${name} cannot have optional type ${wdlType}")
          }
          (Parameter(name, irType, None, attr), false)
        case OverridableBlockInputWithStaticDefault(name, _, defaultValue) =>
          (Parameter(name, irType, Some(WdlUtils.toIRValue(defaultValue, wdlType)), attr), false)
        case OverridableBlockInputWithDynamicDefault(name, _, _) =>
          // If the default value is an expression that requires evaluation (i.e. not a constant),
          // treat the input as an optional applet input and leave the default value to be calculated
          // at runtime
          val optType = Type.ensureOptional(irType)
          (Parameter(name, optType, None, attr), true)
        case OptionalBlockInput(name, _) =>
          val optType = Type.ensureOptional(irType)
          (Parameter(name, optType, None, attr), false)
      }
    }

    // generate a stage Id, this is a string of the form: 'stage-xxx'
    private val fragNumIter = Iterator.from(0)
    private def genFragId(stageName: Option[String] = None): DxWorkflowStage = {
      stageName match {
        case None =>
          val retval = DxWorkflowStage(s"stage-${fragNumIter.next()}")
          retval
        case Some(name) =>
          DxWorkflowStage(s"stage-${name}")
      }
    }

    /**
      * Create a preliminary applet to handle workflow input/outputs. This is
      * used only in the absence of workflow-level inputs/outputs.
      * @param wfName the workflow name
      * @param appletInputs the common applet inputs
      * @param stageInputs the common stage inputs
      * @param outputs the outputs
      * @return
      */
    private def createCommonApplet(wfName: String,
                                   appletInputs: Vector[Parameter],
                                   stageInputs: Vector[StageInput],
                                   outputs: Vector[Parameter]): (Stage, Application) = {
      val applet = Application(s"${wfName}_${CommonStage}",
                               appletInputs,
                               outputs,
                               DefaultInstanceType,
                               NoImage,
                               ExecutableKindWfInputs,
                               standAloneWorkflow)
      logger.trace(s"Compiling common applet ${applet.name}")
      val stage = Stage(
          CommonStage,
          genFragId(Some(CommonStage)),
          applet.name,
          stageInputs,
          outputs
      )
      (stage, applet)
    }

    private def constToStageInput(expr: Option[TAT.Expr],
                                  param: Parameter,
                                  env: CallEnv,
                                  locked: Boolean,
                                  callFqn: String): StageInput = {
      expr match {
        case None if isOptional(param.dxType) =>
          // optional argument that is not provided
          EmptyInput
        case None if param.defaultValue.isDefined =>
          // argument that has a default, it can be omitted in the call
          EmptyInput
        case None if locked =>
          env.log()
          throw new Exception(
              s"""|input <${param.name}, ${param.dxType}> to call <${callFqn}>
                  |is unspecified. This is illegal in a locked workflow.""".stripMargin
                .replaceAll("\n", " ")
          )
        case None | Some(_: TAT.ValueNone) | Some(_: TAT.ValueNull) =>
          // Perhaps the callee is not going to use the argument, so wait until
          // runtime to determine whether to fail.
          EmptyInput
        case Some(expr) =>
          // first try to treat it as a constant
          val wdlType = WdlUtils.fromIRType(param.dxType, typeAliases)
          try {
            val value = evaluator.applyConstAndCoerce(expr, wdlType)
            StaticInput(WdlUtils.toIRValue(value, wdlType))
          } catch {
            case _: EvalException =>
              expr match {
                case TAT.ExprIdentifier(id, _, _) =>
                  try {
                    env(id)._2
                  } catch {
                    case _: Exception =>
                      throw new Exception(
                          s"""|input <${param.name}, ${param.dxType}> to call <${callFqn}>
                              |is missing from the environment. We don't have ${id} in the environment.
                              |""".stripMargin.replaceAll("\n", " ")
                      )
                  }
                case TAT.ExprAt(expr, index, _, _) =>
                  val indexValue =
                    constToStageInput(Some(index), param, env, locked, callFqn) match {
                      case StaticInput(VInt(value)) => value
                      case WorkflowInput(Parameter(_, _, Some(VInt(value)), _)) =>
                        value
                      case other =>
                        throw new Exception(
                            s"Array index expression ${other} is not a constant nor an identifier"
                        )
                    }
                  val value = constToStageInput(Some(expr), param, env, locked, callFqn) match {
                    case StaticInput(VArray(arrayValue)) if arrayValue.size > indexValue =>
                      arrayValue(indexValue.toInt)
                    case WorkflowInput(Parameter(_, _, Some(VArray(arrayValue)), _))
                        if arrayValue.size > indexValue =>
                      arrayValue(indexValue.toInt)
                    case other =>
                      throw new Exception(
                          s"Left-hand side expression ${other} is not a constant nor an identifier"
                      )
                  }
                  StaticInput(value)
                case TAT.ExprGetName(TAT.ExprIdentifier(id, _, _), field, _, _)
                    if env.contains(s"$id.$field") =>
                  env(s"$id.$field")._2
                case TAT.ExprGetName(expr, field, _, _) =>
                  val lhs = constToStageInput(Some(expr), param, env, locked, callFqn)
                  val lhsValue = lhs match {
                    case StaticInput(value)                          => value
                    case WorkflowInput(p) if p.defaultValue.nonEmpty => p.defaultValue.get
                    case other =>
                      throw new Exception(
                          s"Left-hand side expression ${other} is not a constant nor an identifier"
                      )
                  }
                  val wdlValue = lhsValue match {
                    // TODO: the original lcode implies lhsValue can be a V_Call - I don't
                    //  think that's possible, but check to make sure
                    case VHash(members) if members.contains(field) => members(field)
                    case other =>
                      throw new Exception(s"Cannot resolve id ${field} for value ${other}")
                  }
                  StaticInput(wdlValue)
                case _ =>
                  throw new Exception(s"Expression $expr is not a constant nor an identifier")
              }
          }
      }
    }

    /**
      * Compile a call into a stage in an IR Workflow.
      * In a call like:
      *    call lib.native_mk_list as mk_list {
      *      input: a=x, b=5
      *    }
      * it maps callee input `a` to expression `x`. The expressions are
      * trivial because this is a case where we can directly call an applet.
      * @param call the WDL Call object
      * @param env the environment in which the Callee is being called
      * @param locked whether the workflow is locked
      * @return
      */
    private def translateCall(call: TAT.Call, env: CallEnv, locked: Boolean): Stage = {
      // Find the callee
      val calleeName = call.unqualifiedName
      val callee: Callable = dependencies.get(calleeName) match {
        case Some(x) => x
        case _ =>
          throw new Exception(
              s"""|Callable ${calleeName} should exist but is missing from the list of known 
                  |tasks/workflows ${dependencies.keys}|""".stripMargin.replaceAll("\n", " ")
          )
      }
      // Extract the input values/links from the environment
      val inputs: Vector[StageInput] = callee.inputVars.map(param =>
        constToStageInput(call.inputs.get(param.name), param, env, locked, call.fullyQualifiedName)
      )
      Stage(call.actualName, genFragId(), calleeName, inputs, callee.outputVars)
    }

    // Find the closure of the inputs. Do not include the inputs themselves. Create an input
    // for each of these external references.
    private def inputClosure(inputs: Vector[WdlBlockInput],
                             subBlocks: Vector[WdlBlock]): Map[String, (WdlTypes.T, Boolean)] = {
      // remove the regular inputs
      val inputNames = inputs.map(_.name).toSet
      subBlocks.flatMap { block =>
        block.inputs.collect {
          case blockInput if !inputNames.contains(blockInput.name) =>
            blockInput.name -> (blockInput.wdlType, WdlBlockInput.isOptional(blockInput))
        }
      }.toMap
    }

    // split a part of a workflow
    private def splitWorkflowElements(
        statements: Vector[TAT.WorkflowElement]
    ): (Vector[WdlBlockInput], Vector[WdlBlock], Vector[TAT.OutputParameter]) = {
      val (inputs, outputs) = WdlUtils.getInputOutputClosure(statements)
      val subBlocks = WdlBlock.createBlocks(statements)
      (WdlBlockInput.create(inputs), subBlocks, outputs.values.toVector)
    }

    /**
      * A block inside a conditional or scatter. If it is simple, we can use a
      * direct call. Otherwise, recursively call into the asssemble-backbone
      * method, and get a locked subworkflow.
      * @param wfName workflow name
      * @param statements statements in the Block
      * @param blockPath block path
      * @param env env
      * @return the generated callable and a Vector of any auxillary callables
      */
    private def translateNestedBlock(wfName: String,
                                     statements: Vector[TAT.WorkflowElement],
                                     blockPath: Vector[Int],
                                     env: CallEnv): (Callable, Vector[Callable]) = {
      val (inputs, subBlocks, outputs) = splitWorkflowElements(statements)
      assert(subBlocks.nonEmpty)

      if (subBlocks.size == 1) {
        val block = subBlocks.head
        if (Set(BlockKind.CallDirect, BlockKind.CallWithSubexpressions)
              .contains(block.kind)) {
          throw new RuntimeException("Single call not expected in nested block")
        }
        // At runtime, we will need to execute a workflow fragment. This requires an applet.
        // This is a recursive call, to compile a potentially complex sub-block. It could
        // have many calls generating many applets and subworkflows.
        val (stage, aux) = translateWfFragment(wfName, block, blockPath :+ 0, env)
        val fragName = stage.calleeName
        val main = aux.find(_.name == fragName) match {
          case None    => throw new Exception(s"Could not find $fragName")
          case Some(x) => x
        }
        (main, aux)
      } else {
        // There are several subblocks, we need a subworkflow to string them together.
        // The subworkflow may access variables outside of its scope. For example,
        // stage-0.result, and stage-1.result are inputs to stage-2, that belongs to
        // a subworkflow. Because the subworkflow is locked, we need to make them
        // proper inputs.
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
        val closureInputs = inputClosure(inputs, subBlocks)
        logger.trace(
            s"""|compileNestedBlock
                |    inputs = $inputs
                |    closureInputs= $closureInputs
                |""".stripMargin
        )
        val blockName = s"${wfName}_block_${pathStr}"
        val (subwf, auxCallables, _) = translateWorkflowLocked(
            blockName,
            inputs,
            closureInputs,
            outputs,
            blockPath,
            subBlocks,
            Level.Sub
        )
        (subwf, auxCallables)
      }
    }

    /**
      * Builds an applet to evaluate a WDL workflow fragment.
      *
      * @param wfName the workflow name
      * @param block the Block to translate into a WfFragment
      * @param blockPath keeps track of which block this fragment represents;
      *                   a top level block is a number. A sub-block of a top-level
      *                   block is a vector of two numbers, etc.
      * @param env the environment
      * @return
      */
    private def translateWfFragment(wfName: String,
                                    block: WdlBlock,
                                    blockPath: Vector[Int],
                                    env: CallEnv): (Stage, Vector[Callable]) = {
      val stageName = block.getName match {
        case None       => EvalStage
        case Some(name) => name
      }
      logger.trace(s"Compiling fragment <$stageName> as stage")
      logger
        .withTraceIfContainsKey("GenerateIR")
        .trace(
            s"""|block:
                |${block.prettyFormat}
                |""".stripMargin
        )

      // Figure out the closure required for this block, out of the environment
      // Find the closure of a block, all the variables defined earlier
      // that are required for the calculation.
      //
      // Note: some referenced variables may be undefined. This could be because they are:
      // 1) optional
      // 2) defined -inside- the block
      val closure: Map[String, LinkedVar] = block.inputs.flatMap { i: WdlBlockInput =>
        env.lookup(i.name) match {
          case None               => None
          case Some((name, lVar)) => Some((name, lVar))
        }
      }.toMap

      val inputVars: Vector[Parameter] = closure.map {
        case (fqn, (param: Parameter, _)) =>
          param.copy(name = fqn)
      }.toVector

      // A reversible conversion mul.result --> mul___result. This
      // assumes the '___' symbol is not used anywhere in the original WDL script.
      //
      // This is a simplifying assumption, that is hopefully sufficient. It disallows
      // users from using variables with the ___ character sequence.
      val fqnDictTypes: Map[String, Type] = inputVars.map { param: Parameter =>
        param.dxName -> param.dxType
      }.toMap

      // Figure out the block outputs
      val outputs: Map[String, WdlTypes.T] = block.outputs.map {
        case TAT.OutputParameter(name, wdlType, _, _) => name -> wdlType
      }.toMap

      // create a cVar definition from each block output. The dx:stage
      // will output these cVars.
      val outputVars = outputs.map {
        case (fqn, wdlType) =>
          val irType = WdlUtils.toIRType(wdlType)
          Parameter(fqn, irType)
      }.toVector

      // The fragment runner can only handle a single call. If the
      // block already has exactly one call, then we are good. If
      // it contains a scatter/conditional with several calls,
      // then compile the inner block into a sub-workflow. Also
      // Figure out the name of the callable - we need to link with
      // it when we get to the native phase.
      logger
        .withTraceIfContainsKey("GenerateIR")
        .trace(s"category : ${block.kind}")

      val (innerCall, auxCallables): (Option[String], Vector[Callable]) =
        block.kind match {
          case BlockKind.ExpressionsOnly =>
            (None, Vector.empty)
          case BlockKind.CallDirect =>
            throw new Exception(s"a direct call should not reach this stage")
          case BlockKind.CallWithSubexpressions | BlockKind.CallFragment |
              BlockKind.ConditionalOneCall | BlockKind.ScatterOneCall =>
            // a block with no nested sub-blocks, and a single call, or
            // a conditional/scatter with exactly one call in the sub-block
            (Some(block.call.unqualifiedName), Vector.empty)
          case BlockKind.ConditionalComplex =>
            // a conditional/scatter with multiple calls or other nested elements
            // in the sub-block
            val conditional = block.conditional
            val (callable, aux) = translateNestedBlock(wfName, conditional.body, blockPath, env)
            (Some(callable.name), aux :+ callable)
          case BlockKind.ScatterComplex =>
            val scatter = block.scatter
            // add the iteration variable to the inner environment
            val varType = scatter.expr.wdlType match {
              case WdlTypes.T_Array(t, _) => WdlUtils.toIRType(t)
              case _ =>
                throw new Exception("scatter doesn't have an array expression")
            }
            val param = Parameter(scatter.identifier, varType)
            val innerEnv = env.add(scatter.identifier, (param, EmptyInput))
            val (callable, aux) = translateNestedBlock(wfName, scatter.body, blockPath, innerEnv)
            (Some(callable.name), aux :+ callable)
          case _ =>
            throw new Exception(s"unexpected block ${block.prettyFormat}")
        }

      val applet = Application(
          s"${wfName}_frag_${genFragId()}",
          inputVars,
          outputVars,
          DefaultInstanceType,
          NoImage,
          ExecutableKindWfFragment(innerCall.toVector, blockPath, fqnDictTypes),
          standAloneWorkflow
      )

      val stageInputs: Vector[StageInput] = closure.values.map {
        case (_, stageInput) => stageInput
      }.toVector

      (Stage(stageName, genFragId(), applet.name, stageInputs, outputVars), auxCallables :+ applet)
    }

    /**
      * Assembles the backbone of a workflow, having compiled the independent tasks.
      * This is shared between locked and unlocked workflows. Some of the inputs may
      * have default values that are complex expressions, necessitating an initial
      * fragment that performs the evaluation - this only applies to locked workflows,
      * since unlocked workflows always have a "common" applet to handle such expressions.
      *
      * @param wfName workflow name
      * @param wfInputs workflow inputs
      * @param blockPath the path to the current (sub)workflow, as a vector of block indices
      * @param subBlocks the sub-blocks of the current block
      * @param locked whether the workflow is locked
      * @return A tuple (Vector[(Stage, Vector[Callable])], CallEnv), whose first element
      *         is a Vector of stages and the callables included in each stage, and the
      *         second element is a Map of all the input and output variables of the
      *         workflow and its stages.
      */
    private def createWorkflowStages(
        wfName: String,
        wfInputs: Vector[LinkedVar],
        blockPath: Vector[Int],
        subBlocks: Vector[WdlBlock],
        locked: Boolean
    ): (Vector[(Stage, Vector[Callable])], CallEnv) = {
      logger.trace(s"Assembling workflow backbone $wfName")

      val inputEnv: CallEnv = CallEnv.fromLinkedVars(wfInputs)

      val logger2 = logger.withIncTraceIndent()
      logger2.trace(s"inputs: ${inputEnv.keys}")

      // link together all the stages into a linear workflow
      val (allStageInfo, stageEnv): (Vector[(Stage, Vector[Callable])], CallEnv) =
        subBlocks.zipWithIndex.foldLeft((Vector.empty[(Stage, Vector[Callable])], inputEnv)) {
          case ((stages, beforeEnv), (block: WdlBlock, blockNum: Int)) =>
            if (block.kind == BlockKind.CallDirect) {
              block.target match {
                case Some(call: TAT.Call) =>
                  // The block contains exactly one call, with no extra variables.
                  // All the variables are already in the environment, so there is no
                  // need to do any extra work. Compile directly into a workflow stage.
                  logger2.trace(s"Translating call ${call.actualName} as stage")
                  val stage = translateCall(call, beforeEnv, locked)
                  // Add bindings for the output variables. This allows later calls to refer
                  // to these results.
                  val afterEnv = stage.outputs.foldLeft(beforeEnv) {
                    case (env, param: Parameter) =>
                      val fqn = s"${call.actualName}.${param.name}"
                      val paramFqn = param.copy(name = fqn)
                      env.add(fqn, (paramFqn, LinkInput(stage.dxStage, param.dxName)))
                  }
                  (stages :+ (stage, Vector.empty[Callable]), afterEnv)
                case _ =>
                  throw new Exception(s"invalid DirectCall block ${block}")
              }
            } else {
              // A simple block that requires just one applet, OR
              // a complex block that needs a subworkflow
              val (stage, auxCallables) =
                translateWfFragment(wfName, block, blockPath :+ blockNum, beforeEnv)
              val afterEnv = stage.outputs.foldLeft(beforeEnv) {
                case (env, param) =>
                  env.add(param.name, (param, LinkInput(stage.dxStage, param.dxName)))
              }
              (stages :+ (stage, auxCallables), afterEnv)
            }
        }

      if (logger2.containsKey("GenerateIR")) {
        logger2.trace(s"stages for workflow $wfName = [")
        val logger3 = logger2.withTraceIfContainsKey("GenerateIR", indentInc = 1)
        allStageInfo.foreach {
          case (stage, _) =>
            logger3.trace(
                s"${stage.description}, ${stage.dxStage.id} -> callee=${stage.calleeName}"
            )
        }
        logger2.trace("]")
      }

      (allStageInfo, stageEnv)
    }

    private def buildSimpleWorkflowOutput(output: TAT.OutputParameter, env: CallEnv): LinkedVar = {
      val irType = WdlUtils.toIRType(output.wdlType)
      val param = Parameter(output.name, irType)
      val stageInput: StageInput = if (env.contains(output.name)) {
        env(output.name)._2
      } else {
        try {
          // try to evaluate the output as a constant
          val v = evaluator.applyConstAndCoerce(output.expr, output.wdlType)
          StaticInput(WdlUtils.toIRValue(v, output.wdlType))
        } catch {
          case _: EvalException =>
            output.expr match {
              case TAT.ExprIdentifier(id, _, _) =>
                // The output is a reference to a previously defined variable
                env(id)._2
              case TAT.ExprGetName(TAT.ExprIdentifier(id2, _, _), id, _, _) =>
                // The output is a reference to a previously defined variable
                env(s"$id2.$id")._2
              case _ =>
                // An expression that requires evaluation
                throw new Exception(
                    s"""|Internal error: (${output.expr}) requires evaluation,
                        |which requires constructing an output applet and a stage""".stripMargin
                      .replaceAll("\n", " ")
                )
            }
        }
      }
      (param, stageInput)
    }

    /**
      * Build an applet + workflow stage for evaluating outputs. There are two reasons
      * to be build a special output section:
      * 1. Locked workflow: some of the workflow outputs are expressions.
      *    We need an extra applet+stage to evaluate them.
      * 2. Unlocked workflow: there are no workflow outputs, so we create
      *    them artificially with a separate stage that collects the outputs.
      * @param wfName the workflow name
      * @param outputs the outputs
      * @param env the environment
      * @return
      */
    private def createOutputStage(wfName: String,
                                  outputs: Vector[TAT.OutputParameter],
                                  env: CallEnv): (Stage, Application) = {
      // Figure out what variables from the environment we need to pass into the applet.
      // create inputs from outputs
      val outputInputVars: Map[String, LinkedVar] = outputs.collect {
        case TAT.OutputParameter(name, _, _, _) if env.contains(name) => name -> env(name)
      }.toMap
      // create inputs from the closure of the output nodes, which includes (recursively)
      // all the variables in the output node expressions
      val closureInputVars: Map[String, LinkedVar] =
        WdlUtils.getOutputClosure(outputs).keySet.map(name => name -> env(name)).toMap
      val inputVars = (outputInputVars ++ closureInputVars).values.toVector
      logger.trace(s"inputVars: ${inputVars.map(_._1)}")
      // build definitions of the output variables
      val outputVars: Vector[Parameter] = outputs.map {
        case TAT.OutputParameter(name, wdlType, expr, _) =>
          val value =
            try {
              val v = evaluator.applyConstAndCoerce(expr, wdlType)
              Some(WdlUtils.toIRValue(v, wdlType))
            } catch {
              case _: EvalException => None
            }
          val irType = WdlUtils.toIRType(wdlType)
          Parameter(name, irType, value)
      }
      // Determine kind of application. If a custom reorg app is used and this is a top-level
      // workflow (custom reorg applet doesn't apply to locked workflows), add an output
      // variable for reorg status.
      val (applicationKind, updatedOutputVars) = reorgAttrs match {
        case CustomReorgSettings(_, _, true) if !isLocked =>
          val updatedOutputVars = outputVars :+ Parameter(
              ReorgStatus,
              TString,
              Some(VString(ReorgStatusCompleted))
          )
          (ExecutableKindWfCustomReorgOutputs, updatedOutputVars)
        case _ =>
          (ExecutableKindWfOutputs, outputVars)
      }
      val application = Application(
          s"${wfName}_$OutputStage",
          inputVars.map(_._1),
          updatedOutputVars,
          DefaultInstanceType,
          NoImage,
          applicationKind,
          standAloneWorkflow
      )
      val stage = Stage(
          OutputStage,
          genFragId(Some(OutputStage)),
          application.name,
          inputVars.map(_._2),
          updatedOutputVars
      )
      (stage, application)
    }

    /**
      * Compile a locked workflow. This is called at the top level for locked workflows,
      * and it is always called for nested workflows regarless of whether the top level
      * is locked.
      * @param wfName workflow name
      * @param inputs formal workflow inputs
      * @param closureInputs inputs depended on by `inputs`; mapping of name to (type, optional)
      * @param outputs workflow outputs
      * @param blockPath the path to the current (sub)workflow, as a vector of block indices
      * @param subBlocks the sub-blocks of the current block
      * @param level the workflow level
      * @return
      */
    private def translateWorkflowLocked(
        wfName: String,
        inputs: Vector[WdlBlockInput],
        closureInputs: Map[String, (T, Boolean)],
        outputs: Vector[TAT.OutputParameter],
        blockPath: Vector[Int],
        subBlocks: Vector[WdlBlock],
        level: Level.Level
    ): (Workflow, Vector[Callable], Vector[LinkedVar]) = {
      // translate wf inputs, and also get a Vector of any non-constant
      // expressions that need to be evaluated in the common stage
      val (wfInputParams, dynamicDefaults): (Vector[Parameter], Vector[Boolean]) =
        inputs.map(createWorkflowInput).unzip
      // inputs that are a result of accessing variables in an encompassing
      // WDL workflow.
      val closureInputParams: Vector[Parameter] = closureInputs.map {
        case (name, (wdlType, false)) =>
          // no default value
          val irType = WdlUtils.toIRType(wdlType)
          if (isOptional(irType)) {
            throw new Exception(s"Required input ${name} cannot have optional type ${wdlType}")
          }
          Parameter(name, irType)
        case (name, (wdlType, true)) =>
          // there is a default value. This input is de facto optional.
          // We change the type of the Parameter and make sure it is optional.
          // no default value
          val irType = WdlUtils.toIRType(wdlType)
          Parameter(name, Type.ensureOptional(irType))
      }.toVector
      val allWfInputParameters = wfInputParams ++ closureInputParams

      // If the workflow has inputs that are defined with complex expressions,
      // we need to build an initial applet to evaluate those.
      val (backboneInputs, initialStageInfo) = if (dynamicDefaults.exists(b => b)) {
        val commonAppletInputs = allWfInputParameters
        val commonStageInputs = allWfInputParameters.map(p => WorkflowInput(p))
        val inputOutputs: Vector[Parameter] = inputs.map {
          case OverridableBlockInputWithDynamicDefault(name, wdlType, _) =>
            val nonOptType = WdlUtils.toIRType(wdlType)
            Parameter(name, nonOptType)
          case i: WdlBlockInput =>
            val irType = WdlUtils.toIRType(i.wdlType)
            Parameter(i.name, irType)
        }
        val closureOutputs: Vector[Parameter] = closureInputParams
        val (commonStage, commonApplet) =
          createCommonApplet(wf.name,
                             commonAppletInputs,
                             commonStageInputs,
                             inputOutputs ++ closureOutputs)
        val fauxWfInputs: Vector[LinkedVar] = commonStage.outputs.map { param =>
          val link = LinkInput(commonStage.dxStage, param.dxName)
          (param, link)
        }
        (fauxWfInputs, Vector((commonStage, Vector(commonApplet))))
      } else {
        (allWfInputParameters.map(p => (p, WorkflowInput(p))), Vector.empty)
      }
      // translate the Block(s) into workflow stages
      val (backboneStageInfo, env) =
        createWorkflowStages(wfName, backboneInputs, blockPath, subBlocks, locked = true)
      val (stages, auxCallables) = (initialStageInfo ++ backboneStageInfo).unzip

      // additional values needed to create the Workflow
      val wfInputLinks: Vector[LinkedVar] = allWfInputParameters.map(p => (p, WorkflowInput(p)))
      val wfSource = WdlWorkflowSource(wf, wdlBundle.version)
      // translate workflow-level metadata to IR
      val attributes = meta.translate

      // check if all expressions can be resolved without evaluation (i.e. is a constant
      // or a reference to a defined variable)
      val allSimpleOutputs = outputs.forall {
        case TAT.OutputParameter(name, _, _, _) if env.contains(name) =>
          // the environment has a stage with this output - we can get it by linking
          true
        case TAT.OutputParameter(_, _, expr, _) if WdlUtils.isTrivialExpression(expr) =>
          // A constant or a reference to a variable
          true
        case TAT.OutputParameter(_, _, TAT.ExprIdentifier(id, _, _), _) if env.contains(id) =>
          // An identifier that is in scope
          true
        case TAT.OutputParameter(
            _,
            _,
            TAT.ExprGetName(TAT.ExprIdentifier(id2, _, _), id, _, _),
            _
            ) =>
          // Access to the results of a call. For example,
          // c1 is call, and the output section is:
          //  output {
          //     Int? result1 = c1.result
          //     Int? result2 = c2.result
          //  }
          env.contains(s"$id2.$id")
        case _ => false
      }
      // Is an output used directly as an input? For example, in the small workflow
      // below, 'lane' is used in such a manner.
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
      // In locked workflows, it is illegal to access a workflow input directly from
      // a workflow output. It is only allowed to access a stage input/output.
      val noInputsUsedAsOutputs =
        inputs.map(_.name).toSet.intersect(WdlUtils.getOutputClosure(outputs).keySet).isEmpty
      // if all inputs are simple and no inputs are used as outputs, then we do not
      // need a separate workflow stage to evaluate inputs
      if (allSimpleOutputs && noInputsUsedAsOutputs) {
        val simpleWfOutputs = outputs.map(output => buildSimpleWorkflowOutput(output, env))
        val irwf =
          Workflow(wfName,
                   wfInputLinks,
                   simpleWfOutputs,
                   stages,
                   wfSource,
                   locked = true,
                   level,
                   attributes)
        (irwf, auxCallables.flatten, simpleWfOutputs)
      } else {
        // Some of the outputs are expressions. We need an extra applet+stage
        // to evaluate them.
        val (outputStage, outputApplet) = createOutputStage(wfName, outputs, env)
        val wfOutputs = outputStage.outputs.map { param =>
          (param, LinkInput(outputStage.dxStage, param.dxName))
        }
        val irwf = Workflow(wfName,
                            wfInputLinks,
                            wfOutputs,
                            stages :+ outputStage,
                            wfSource,
                            locked = true,
                            level,
                            attributes)
        (irwf, auxCallables.flatten :+ outputApplet, wfOutputs)
      }
    }

    /**
      * Compile a top-level locked workflow.
      */
    private def translateTopWorkflowLocked(
        inputs: Vector[WdlBlockInput],
        outputs: Vector[TAT.OutputParameter],
        subBlocks: Vector[WdlBlock]
    ): (Workflow, Vector[Callable], Vector[LinkedVar]) = {
      translateWorkflowLocked(wf.name,
                              inputs,
                              Map.empty,
                              outputs,
                              Vector.empty,
                              subBlocks,
                              Level.Top)
    }

    /**
      * Compile a "regular" (i.e. unlocked) workflow. This function only gets
      * called at the top-level.
      */
    private def translateTopWorkflowUnlocked(
        inputs: Vector[WdlBlockInput],
        outputs: Vector[TAT.OutputParameter],
        subBlocks: Vector[WdlBlock]
    ): (Workflow, Vector[Callable], Vector[LinkedVar]) = {
      // Create a special applet+stage for the inputs. This is a substitute for
      // workflow inputs. We now call the workflow inputs, "fauxWfInputs" since
      // they are references to the outputs of this first applet.
      val commonAppletInputs: Vector[Parameter] =
        inputs.map(input => createWorkflowInput(input)._1)
      val commonStageInputs: Vector[StageInput] = inputs.map(_ => EmptyInput)
      val (commonStg, commonApplet) =
        createCommonApplet(wf.name, commonAppletInputs, commonStageInputs, commonAppletInputs)
      val fauxWfInputs: Vector[LinkedVar] = commonStg.outputs.map { param =>
        val stageInput = LinkInput(commonStg.dxStage, param.dxName)
        (param, stageInput)
      }

      val (allStageInfo, env) =
        createWorkflowStages(wf.name, fauxWfInputs, Vector.empty, subBlocks, locked = false)
      val (stages, auxCallables) = allStageInfo.unzip

      // convert the outputs into an applet+stage
      val (outputStage, outputApplet) = createOutputStage(wf.name, outputs, env)

      val wfInputs = commonAppletInputs.map(param => (param, EmptyInput))
      val wfOutputs =
        outputStage.outputs.map(param => (param, LinkInput(outputStage.dxStage, param.dxName)))
      val wfAttr = meta.translate
      val wfSource = WdlWorkflowSource(wf, wdlBundle.version)
      val irwf = Workflow(
          wf.name,
          wfInputs,
          wfOutputs,
          commonStg +: stages :+ outputStage,
          wfSource,
          locked = false,
          Level.Top,
          wfAttr
      )
      (irwf, commonApplet +: auxCallables.flatten :+ outputApplet, wfOutputs)
    }

    /**
      * Creates an applet to reorganize the output files. We want to
      * move the intermediate results to a subdirectory.  The applet
      * needs to process all the workflow outputs, to find the files
      * that belong to the final results.
      * @param wfName workflow name
      * @param wfOutputs workflow outputs
      * @return
      */
    private def createReorgStage(wfName: String,
                                 wfOutputs: Vector[LinkedVar]): (Stage, Application) = {
      val applet = Application(
          s"${wfName}_$ReorgStage",
          wfOutputs.map(_._1),
          Vector.empty,
          DefaultInstanceType,
          NoImage,
          ExecutableKindWorkflowOutputReorg,
          standAloneWorkflow
      )
      logger.trace(s"Creating output reorganization applet ${applet.name}")
      // Link to the X.y original variables
      val inputs: Vector[StageInput] = wfOutputs.map(_._2)
      val stage =
        Stage(ReorgStage, genFragId(Some(ReorgStage)), applet.name, inputs, Vector.empty[Parameter])
      (stage, applet)
    }

    private def createCustomReorgStage(wfOutputs: Vector[LinkedVar],
                                       appletId: String,
                                       reorgConfigFile: Option[String]): (Stage, Application) = {
      logger.trace(s"Creating custom output reorganization applet ${appletId}")
      val (statusParam, statusStageInput): LinkedVar = wfOutputs.filter {
        case (x, _) => x.name == ReorgStatus
      } match {
        case Vector(lvar) => lvar
        case other =>
          throw new Exception(
              s"Expected exactly one output with name ${ReorgStatus}, found ${other}"
          )
      }
      val configFile: Option[VFile] = reorgConfigFile.map(VFile)
      val appInputs = Vector(
          statusParam,
          Parameter(Constants.ReorgConfig, TFile, configFile)
      )
      val appletKind = ExecutableKindWorkflowCustomReorg(appletId)
      val applet = Application(
          appletId,
          appInputs,
          Vector.empty,
          DefaultInstanceType,
          NoImage,
          appletKind,
          standAloneWorkflow
      )
      // Link to the X.y original variables
      val inputs: Vector[StageInput] = configFile match {
        case Some(x) => Vector(statusStageInput, StaticInput(x))
        case _       => Vector(statusStageInput)
      }
      val stage =
        Stage(ReorgStage, genFragId(Some(ReorgStage)), applet.name, inputs, Vector.empty[Parameter])
      (stage, applet)
    }

    def apply: Vector[Callable] = {
      logger.trace(s"Translating workflow ${wf.name}")
      // Create a stage per workflow body element (variable block, call,
      // scatter block, conditional block)
      val subBlocks = WdlBlock.createBlocks(wf.body)
      // translate workflow inputs/outputs to equivalent classes defined in Block
      val inputs = wf.inputs.map(WdlBlockInput.translate)
      val (irWf, irCallables, irOutputs) =
        if (isLocked) {
          translateTopWorkflowLocked(inputs, wf.outputs, subBlocks)
        } else {
          translateTopWorkflowUnlocked(inputs, wf.outputs, subBlocks)
        }
      // add a reorg applet if necessary
      val (updatedWf, updatedCallables) = reorgAttrs match {
        case DefaultReorgSettings(true) =>
          val (reorgStage, reorgApl) = createReorgStage(wf.name, irOutputs)
          (irWf.copy(stages = irWf.stages :+ reorgStage), irCallables :+ reorgApl)
        case CustomReorgSettings(appUri, reorgConfigFile, true) if !isLocked =>
          val (reorgStage, reorgApl) = createCustomReorgStage(irOutputs, appUri, reorgConfigFile)
          (irWf.copy(stages = irWf.stages :+ reorgStage), irCallables :+ reorgApl)
        case _ =>
          (irWf, irCallables)
      }
      // validate workflow stages
      val allCallableNames = updatedCallables.map(_.name).toSet ++ dependencies.keySet
      val invalidStages =
        updatedWf.stages.filterNot(stage => allCallableNames.contains(stage.calleeName))
      if (invalidStages.nonEmpty) {
        val invalidStageDesc = invalidStages
          .map(stage => s"$stage.description, $stage.id.getId -> ${stage.calleeName}")
          .mkString("    ")
        throw new Exception(s"""|One or more stages reference missing tasks:
                                |stages: $invalidStageDesc
                                |callables: $allCallableNames
                                |""".stripMargin)
      }
      updatedCallables :+ updatedWf
    }
  }

  def translateCallable(callable: TAT.Callable,
                        dependencies: Map[String, Callable]): Vector[Callable] = {
    callable match {
      case task: TAT.Task =>
        val taskTranslator = WdlTaskTranslator(task)
        Vector(taskTranslator.apply)
      case wf: TAT.Workflow =>
        val wfTranslator = WdlWorkflowTranslator(wf, dependencies)
        wfTranslator.apply
    }
  }
}
