// Execute a WDL workflow with no calls, and no expressions, only inputs and outputs.
// It could have expressions in the output section.

package dxWDL.exec

import spray.json._
import wdlTools.types.{TypedAbstractSyntax => TAT}

import dxWDL.base.{Utils, Verbose}
import dxWDL.base.WomCompat._
import dxWDL.util._

case class WfOutputs(wf: TAT.Workflow,
                     document : TAT.Document,
                     typeAliases: Map[String, WdlTypes.T],
                     dxPathConfig: DxPathConfig,
                     dxIoFunctions: DxIoFunctions,
                     runtimeDebugLevel: Int) {
  private val verbose = runtimeDebugLevel >= 1
  //private val maxVerboseLevel = (runtimeDebugLevel == 2)
  private val utlVerbose = Verbose(runtimeDebugLevel >= 1, false, Set.empty)
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(utlVerbose, dxIoFunctions.fileInfoDir, typeAliases)

  private val evaluator : wdlTools.eval.Eval = {
    val evalOpts = wdlTools.util.Options(typeChecking = wdlTools.util.TypeCheckingRegime.Strict,
                                         antlr4Trace = false,
                                         localDirectories = Vector.empty,
                                         verbosity = wdlTools.util.Verbosity.Quiet)
    val evalCfg = wdlTools.util.EvalConfig(dxIoFunctions.config.homeDir,
                                           dxIoFunctions.config.tmpDir,
                                           dxIoFunctions.config.stdout,
                                           dxIoFunctions.config.stderr)
    wdlTools.eval.Eval(evalOpts, evalCfg, doc.version.value, None)
  }

  private def evaluateWomExpression(expr: WomExpression,
                                    womType: WdlTypes.T,
                                    env: Map[String, WdlValues.V]): WdlValues.V = {
    evaluator.applyExprAndCoerce(expr, womType, env)
  }

  def apply(envInitial: Map[String, WdlValues.V], addStatus: Boolean = false): Map[String, JsValue] = {
    Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
    Utils.appletLog(verbose, s"Environment: ${envInitial}")
    val outputNodes: Vector[GraphOutputNode] = wf.innerGraph.outputNodes.toVector
    Utils.appletLog(
        verbose,
        s"""|Evaluating workflow outputs
            |${WomPrettyPrintApproxWdl.graphOutputs(outputNodes)}
            |""".stripMargin
    )

    // Some of the inputs could be optional. If they are missing,
    // add in a None value.
    val allInputs = Block.closure(Block(wf.innerGraph.outputNodes.toVector))
    val envInitialFilled: Map[String, WdlValues.V] = allInputs.flatMap {
      case (name, (womType, hasDefaultVal)) =>
        (envInitial.get(name), womType) match {
          case (None, WdlTypes.T_Optional(t)) =>
            Some(name -> WdlValues.V_OptionalValue(t, None))
          case (None, _) if hasDefaultVal =>
            None
          case (None, _) =>
            // input is missing, and there is no default at the callee,
            Utils.warning(utlVerbose,
                          s"input is missing for ${name}, and there is no default at the callee")
            None
          case (Some(x), _) =>
            Some(name -> x)
        }
    }.toMap

    // Evaluate the output declarations. Add outputs evaluated to
    // the environment, so they can be referenced by expressions in the next
    // lines.
    var envFull = envInitialFilled
    val outputs: Map[String, WdlValues.V] = outputNodes.map {
      case PortBasedGraphOutputNode(id, womType, sourcePort) =>
        val value = envFull.get(sourcePort.name) match {
          case None =>
            throw new Exception(s"could not find ${sourcePort}")
          case Some(value) =>
            value
        }
        val name = id.workflowLocalName
        envFull += (name -> value)
        name -> value

      case expr: ExpressionBasedGraphOutputNode =>
        val value = evaluateWomExpression(expr.womExpression, expr.womType, envFull)
        val name = expr.graphOutputPort.name
        envFull += (name -> value)
        name -> value

      case other =>
        throw new Exception(s"unhandled output ${other}")
    }.toMap

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = outputs
      .map {
        case (outputVarName, womValue) =>
          val wvl = wdlVarLinksConverter.importFromWDL(womValue.womType, womValue)
          wdlVarLinksConverter.genFields(wvl, outputVarName)
      }
      .toList
      .flatten
      .toMap

    if (addStatus) {
      outputFields + (Utils.REORG_STATUS -> JsString(Utils.REORG_STATUS_COMPLETE))
    } else {
      outputFields
    }
  }
}
