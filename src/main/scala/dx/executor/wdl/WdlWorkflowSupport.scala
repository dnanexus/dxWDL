package dx.executor.wdl

import dx.AppInternalException
import dx.core.Native
import dx.core.io.DxWorkerPaths
import dx.core.ir.{Block, BlockKind, ParameterLink, ParameterLinkExec, Type, Value}
import dx.core.languages.wdl.{BlockInput, Utils, WdlBlock, Utils => WdlUtils}
import dx.executor.{BlockContext, JobMeta, WorkflowSupport, WorkflowSupportFactory}
import spray.json.JsValue
import wdlTools.eval.{Eval, EvalPaths, WdlValueBindings, WdlValues}
import wdlTools.exec.InputOutput
import wdlTools.syntax.WdlVersion
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT, Utils => TUtils}
import wdlTools.types.WdlTypes._
import wdlTools.eval.WdlValues._
import wdlTools.util.{Logger, TraceLevel}

case class WorkflowInputOutput(workflow: TAT.Workflow, logger: Logger)
    extends InputOutput(workflow, logger) {
  // TODO: implement graph building from workflow

  override protected def inputOrder: Vector[String] = workflow.inputs.map(_.name)

  override protected def outputOrder: Vector[String] = workflow.outputs.map(_.name)
}

case class WdlWorkflowSupport(workflow: TAT.Workflow,
                              wdlVersion: WdlVersion,
                              tasks: Map[String, TAT.Task],
                              wdlTypeAliases: Map[String, T_Struct],
                              jobMeta: JobMeta,
                              workerPaths: DxWorkerPaths)
    extends WorkflowSupport(jobMeta) {
  private val logger = jobMeta.logger
  private lazy val evaluator = Eval(
      EvalPaths(workerPaths.homeDir, workerPaths.tmpDir),
      Some(wdlVersion),
      jobMeta.fileResolver,
      Logger.Quiet
  )
  private lazy val workflowIO = WorkflowInputOutput(workflow, jobMeta.logger)

  override def typeAliases: Map[String, Type] = WdlUtils.toIRTypeMap(wdlTypeAliases)

  override def evaluateInputs(
      jobInputs: Map[String, (Type, Value)]
  ): Map[String, (Type, Value)] = {
    if (logger.isVerbose) {
      logger.trace(workflow.inputs.map(TUtils.prettyFormatInput).mkString("\n"))
    }
    val workflowInputs = workflow.inputs.map(inp => inp.name -> inp).toMap
    // convert IR to WDL values
    val inputWdlValues: Map[String, V] = jobInputs.collect {
      case (name, (_, value)) =>
        val wdlType = workflowInputs(name).wdlType
        name -> WdlUtils.fromIRValue(value, wdlType, name)
    }
    // evaluate
    val evalauatedInputValues =
      workflowIO.inputsFromValues(inputWdlValues, evaluator, strict = true)
    // convert back to IR
    evalauatedInputValues.bindings.map {
      case (name, value) =>
        val wdlType = workflowInputs(name).wdlType
        val irType = WdlUtils.toIRType(wdlType)
        val irValue = WdlUtils.toIRValue(value, wdlType)
        name -> (irType, irValue)
    }
  }

  override def evaluateOutputs(
      jobInputs: Map[String, (Type, Value)],
      addReorgStatus: Boolean
  ): Map[String, (Type, Value)] = {
    if (logger.isVerbose) {
      logger.trace(workflow.outputs.map(TUtils.prettyFormatOutput).mkString("\n"))
    }
    // convert IR to WDL
    // Some of the inputs could be optional. If they are missing, add in a None value.
    val outputWdlValues: Map[String, V] =
      WdlUtils.getOutputClosure(workflow.outputs).collect {
        case (name, wdlType) if jobInputs.contains(name) =>
          val (_, value) = jobInputs(name)
          name -> WdlUtils.fromIRValue(value, wdlType, name)
        case (name, T_Optional(_)) =>
          name -> V_Null
      }
    // evaluate
    val evaluatedOutputValues =
      workflowIO.evaluateOutputs(evaluator, WdlValueBindings(outputWdlValues))
    // convert back to IR
    val workflowOutputs = workflow.outputs.map(inp => inp.name -> inp).toMap
    val irOutputs = evaluatedOutputValues.bindings.map {
      case (name, value) =>
        val wdlType = workflowOutputs(name).wdlType
        val irType = WdlUtils.toIRType(wdlType)
        val irValue = WdlUtils.toIRValue(value, wdlType)
        name -> (irType, irValue)
    }
    if (addReorgStatus) {
      irOutputs + (Native.ReorgStatus -> (Type.TString, Value.VString(Native.ReorgStatusCompleted)))
    } else {
      irOutputs
    }
  }

  private def evaluateExpression(expr: TAT.Expr, wdlType: T, env: Map[String, (T, V)]): V = {
    evaluator.applyExprAndCoerce(expr, wdlType, Eval.createBindingsFromEnv(env))
  }

  private def getBlockOutputs(elements: Vector[TAT.WorkflowElement]): Map[String, T] = {
    val (_, outputs) = Utils.getInputOutputClosure(elements)
    outputs.values.map {
      case TAT.OutputDefinition(name, wdlType, _, _) => name -> wdlType
    }.toMap
  }

  // This method is exposed so that we can unit-test it.
  private[wdl] def evaluateWorkflowElements(
      elements: Seq[TAT.WorkflowElement],
      env: Map[String, (T, V)]
  ): Map[String, (T, V)] = {
    elements.foldLeft(Map.empty[String, (T, V)]) {
      case (accu, TAT.Declaration(name, wdlType, exprOpt, _)) =>
        val value = exprOpt match {
          case Some(expr) =>
            evaluateExpression(expr, wdlType, accu ++ env)
          case None => V_Null
        }
        accu + (name -> (wdlType, value))
      case (accu, TAT.Conditional(expr, body, _)) =>
        // evaluate the condition
        val results = evaluateExpression(expr, expr.wdlType, accu ++ env) match {
          case V_Boolean(true) =>
            // condition is true, evaluate the internal block.
            evaluateWorkflowElements(body, accu ++ env).map {
              case (key, (t, value)) => key -> (T_Optional(t), V_Optional(value))
            }
          case V_Boolean(false) =>
            // condition is false, return V_Null for all the values
            getBlockOutputs(body).map {
              case (key, wdlType) => key -> (T_Optional(wdlType), V_Null)
            }
          case other =>
            throw new AppInternalException(s"Unexpected condition expression value ${other}")
        }
        accu ++ results
      case (accu, TAT.Scatter(id, expr, body, _)) =>
        val collection: Vector[V] =
          evaluateExpression(expr, expr.wdlType, accu ++ env) match {
            case V_Array(array) => array
            case other =>
              throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
          }
        val outputTypes: Map[String, T] = getBlockOutputs(body)
        val outputNames: Vector[String] = outputTypes.keys.toVector
        // iterate on the collection, evaluate the body N times,
        // transpose the results into M vectors of N items
        val outputValues: Vector[Vector[V]] =
          collection.map { v =>
            val envInner = accu ++ env + (id -> (expr.wdlType, v))
            val bodyValues = evaluateWorkflowElements(body, envInner)
            outputNames.map(bodyValues(_)._2)
          }.transpose
        // Add the WDL array type to each vector
        val results = outputNames.zip(outputValues).map {
          case (name, values) =>
            val arrayType = T_Array(outputTypes(name), nonEmpty = false)
            val arrayValue = V_Array(values)
            name -> (arrayType, arrayValue)
        }
        accu ++ results
      case (_, other) =>
        throw new Exception(s"type ${other.getClass} while evaluating expressions")
    }
  }

  case class WdlBlockContext(block: WdlBlock, env: Map[String, (T, V)]) extends BlockContext {
    private def call: TAT.Call = block.call

    private def evaluateCallInputs: Map[String, (T, V)] = {
      val calleeInputs = call.callee.input
      block.call.inputs.map {
        case (key, expr) =>
          val actualCalleeType: WdlTypes.T = calleeInputs.get(key) match {
            case Some((t, _)) => t
            case None =>
              throw new Exception(s"Callee ${call.callee.name} doesn't have input ${key}")

          }
          val value = evaluateExpression(expr, actualCalleeType, env)
          key -> (actualCalleeType, value)
      }
    }

    private def evaluateCallOutputs(outputs: Map[String, (T, V)]): Map[String, ParameterLink] = {
      val outputNames = block.outputNames
      logger.traceLimited(
          s"""|processOutputs
              |  env = ${env.keys}
              |  fragResults = ${outputs.keys}
              |  exportedVars = ${outputNames}
              |""".stripMargin,
          minLevel = TraceLevel.VVerbose
      )
      val inputFields = jobMeta.createOutputLinks(
          fragInputs.view.filterKeys(outputNames.contains).toMap
      )
      val outputFields = fragOutputs.view.filterKeys(outputNames.contains).toMap
      inputFields ++ outputFields
    }

    private def launchCall(): Map[String, ParameterLink] = {
      val callInputs = evaluateCallInputs
      logger.traceLimited(
          s"""|call = ${call}
              |callInputs = ${callInputs}
              |""".stripMargin,
          minLevel = TraceLevel.VVerbose
      )
      val executableLink = getExecutableLink(call.callee.name)
      val callName = call.actualName
      val callInputsIR = WdlUtils.toIR(callInputs)
      val instanceType = tasks.get(call.callee.name).map { task =>
      }
      val dxExecution = launchJob(executableLink, callName, callInputsIR, instanceType)
      executableLink.outputs.map {
        case (fieldName, irType) =>
          val fqn = s"${callName}.${fieldName}"
          fqn -> ParameterLinkExec(dxExecution, fieldName, irType)
      }
    }

    def launchConditionalCall(): Map[String, (T, V)] = {}

    def launchConditionalSubblock(): Map[String, (T, V)] = {}

    def launchScatterCall(identifier: String,
                          itemType: T,
                          collection: Vector[V]): Map[String, (T, V)] = {}

    def launchScatterSubblock(identifier: String,
                              itemType: T,
                              collection: Vector[V]): Map[String, (T, V)] = {}

    override def launch(): Map[String, ParameterLink] = {
      val outputs: Map[String, (T, V)] = block.category match {
        case BlockKind.CallWithSubexpressions | BlockKind.CallFragment =>
          launchCall()
        case BlockKind.ConditionalOneCall | BlockKind.ConditionalComplex =>
          val cond = block.target match {
            case TAT.Conditional(expr, _, _) =>
              evaluateExpression(expr, T_Boolean, env)
          }
          (cond, block.category) match {
            case (V_Boolean(true), BlockKind.ConditionalOneCall) =>
              launchConditionalCall()
            case (V_Boolean(true), BlockKind.ConditionalComplex) =>
              launchConditionalSubblock()
            case (V_Boolean(false), _) =>
              Map.empty
            case _ =>
              throw new Exception(s"invalid conditional value ${cond}")
          }
        case BlockKind.ScatterOneCall | BlockKind.ScatterComplex =>
          assert(scatterStart == 0)
          val (identifier, itemType, collection) = block.target match {
            case TAT.Scatter(identifier, expr, _, _) =>
              val itemType = expr.wdlType match {
                case T_Array(t, _) => t
                case _ =>
                  throw new Exception(s"scatter type ${expr.wdlType} is not an array")
              }
              val collection: Vector[V] = evaluateExpression(expr, expr.wdlType, env) match {
                case V_Array(array) => array
                case other =>
                  throw new AppInternalException(s"scatter value ${other} is not an array")
              }
              (identifier, itemType, collection)
          }
          if (block.category == BlockKind.ScatterOneCall) {
            launchScatterCall(identifier, itemType, collection)
          } else {
            launchScatterSubblock(identifier, itemType, collection)
          }
        case BlockKind.ExpressionsOnly =>
          Map.empty
        case BlockKind.CallDirect =>
          throw new RuntimeException("unreachable state")
      }
      evaluateCallOutputs(outputs)
    }

    override def continue(): Map[String, ParameterLink] = {
      //
      //  def continueScatter(): Map[String, ParameterLink] = {
      //    val blockCtx = evaluateFragInputs()
      //    blockCtx.block.category match {
      //      case BlockKind.ScatterOneCall | BlockKind.ScatterComplex =>
      //        workflowSupport.continue(blockCtx)
      //      case _ =>
      //        throw new Exception("invalid state")
      //    }
      //  }
      //
    }

    override def collect(): Map[String, ParameterLink] = {
      //  def collectScatter(): Map[String, ParameterLink] = {
      //    val blockCtx = evaluateFragInputs()
      //    blockCtx.block.category match {
      //      case BlockKind.ScatterOneCall | BlockKind.ScatterComplex =>
      //        workflowSupport.collect(blockCtx)
      //      case _ =>
      //        throw new Exception("invalid state")
      //    }
      //  }
    }
  }

  override def evaluateBlockInputs(
      jobInputs: Map[String, (Type, Value)]
  ): BlockContext = {
    val topBlocks = WdlBlock.createBlocks(workflow.body)
    val block = blockPath.tail.foldLeft(topBlocks(blockPath.head)) {
      case (subBlock, index) =>
        val innerBlocks = WdlBlock.createBlocks(subBlock.innerElements)
        innerBlocks(index)
    }
    // Some of the inputs could be optional. If they are missing,
    // add in a None value.
    val irInputEnv: Map[String, (Type, Value)] = block.inputs.collect {
      case blockInput: BlockInput if jobInputs.contains(blockInput.name) =>
        blockInput.name -> jobInputs(blockInput.name)
    }.toMap
    val inputEnv = WdlUtils.fromIR(irInputEnv, wdlTypeAliases)
    val envWithPrereqs = evaluateWorkflowElements(block.prerequisites, inputEnv)
    WdlBlockContext(block, envWithPrereqs)
  }
}

case class WdlWorkflowSupportFactory() extends WorkflowSupportFactory {
  override def create(jobMeta: JobMeta, workerPaths: DxWorkerPaths): Option[WdlWorkflowSupport] = {
    val (doc, typeAliases) =
      try {
        WdlUtils.parseSource(jobMeta.sourceCode, jobMeta.fileResolver)
      } catch {
        case _: Throwable =>
          return None
      }
    val workflow = doc.workflow.getOrElse(
        throw new RuntimeException("This document should have a workflow")
    )
    val tasks = doc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    Some(
        WdlWorkflowSupport(workflow,
                           doc.version.value,
                           tasks,
                           typeAliases.bindings,
                           jobMeta,
                           workerPaths)
    )
  }
}
