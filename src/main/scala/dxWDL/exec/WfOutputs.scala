// Execute a WDL workflow with no calls, and no expressions, only inputs and outputs.
// It could have expressions in the output section.

package dxWDL.exec

import spray.json._
import wdlTools.eval.{WdlValues, Context => EvalContext}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import dxWDL.base.{Utils, Verbose}
import dxWDL.util._

case class WfOutputs(wf: TAT.Workflow,
                     document: TAT.Document,
                     typeAliases: Map[String, WdlTypes.T],
                     dxPathConfig: DxPathConfig,
                     dxIoFunctions: DxIoFunctions,
                     runtimeDebugLevel: Int) {
  private val verbose = runtimeDebugLevel >= 1
  //private val maxVerboseLevel = (runtimeDebugLevel == 2)
  private val utlVerbose = Verbose(runtimeDebugLevel >= 1, quiet = false, Set.empty)
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(utlVerbose, dxIoFunctions.fileInfoDir, typeAliases)

  private val evaluator = WdlEvaluator.make(dxIoFunctions, document.version.value)

  private def evaluateWdlExpression(expr: TAT.Expr,
                                    wdlType: WdlTypes.T,
                                    env: Map[String, WdlValues.V]): WdlValues.V = {
    evaluator.applyExprAndCoerce(expr, wdlType, EvalContext(env))
  }

  def apply(envInitial: Map[String, (WdlTypes.T, WdlValues.V)],
            addStatus: Boolean = false): Map[String, JsValue] = {
    Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion}")
    Utils.appletLog(verbose, s"Environment: ${envInitial}")
    Utils.appletLog(
        verbose,
        s"""|Evaluating workflow outputs
            |${WdlPrettyPrintApprox.graphOutputs(wf.outputs)}
            |""".stripMargin
    )

    // Some of the inputs could be optional. If they are missing,
    // add in a None value.
    val wfOutputs = wf.outputs.map(Block.translate)
    val allInputs = Block.outputClosure(wfOutputs)
    val envInitialFilled: Map[String, WdlValues.V] = allInputs.flatMap {
      case (name, wdlType) =>
        (envInitial.get(name), wdlType) match {
          case (None, WdlTypes.T_Optional(_)) =>
            Some(name -> WdlValues.V_Null)
          case (None, _) =>
            // input is missing, and there is no default at the callee,
            Utils.warning(utlVerbose, s"value is missing for ${name}")
            None
          case (Some((_, v)), _) =>
            Some(name -> v)
        }
    }

    // Evaluate the output declarations. Add outputs evaluated to
    // the environment, so they can be referenced by expressions in the next
    // lines.
    var envFull = envInitialFilled
    val outputs: Map[String, (WdlTypes.T, WdlValues.V)] = wfOutputs.map {
      case Block.OutputDefinition(name, wdlType, expr) =>
        val value = evaluateWdlExpression(expr, wdlType, envFull)
        envFull += (name -> value)
        name -> (wdlType, value)
    }.toMap

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = outputs
      .map {
        case (outputVarName, (wdlType, wdlValue)) =>
          val wvl = wdlVarLinksConverter.importFromWDL(wdlType, wdlValue)
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
