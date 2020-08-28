package dx.executor

import dx.api.DxApi
import dx.core.{ReorgStatus, ReorgStatusCompleted}
import dx.core.languages.wdl.{Block, PrettyPrintApprox, ParameterLinkSerde}
import dx.core.getVersion
import spray.json.{JsString, JsValue}
import wdlTools.eval.{Eval, WdlValues}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT, Utils => TUtils}
import wdlTools.util.Bindings

object WfOutputs {
  def prettyPrintOutputs(outputs: Vector[TAT.OutputDefinition]): String = {
    outputs
      .map {
        case TAT.OutputDefinition(name, wdlType, expr, _) =>
          s"${TUtils.prettyFormatType(wdlType)} ${name} = ${TUtils.prettyFormatExpr(expr)}"
      }
      .mkString("\n")
  }
}

case class WfOutputs(wf: TAT.Workflow,
                     document: TAT.Document,
                     wdlVarLinksConverter: ParameterLinkSerde,
                     dxApi: DxApi,
                     evaluator: Eval) {
  private def evaluateWdlExpression(expr: TAT.Expr,
                                    wdlType: WdlTypes.T,
                                    env: Map[String, WdlValues.V]): WdlValues.V = {
    evaluator.applyExprAndCoerce(expr, wdlType, Bindings(env))
  }

  def apply(envInitial: Map[String, (WdlTypes.T, WdlValues.V)],
            addStatus: Boolean = false): Map[String, JsValue] = {
    dxApi.logger.traceLimited(s"dxWDL version: ${getVersion}")
    dxApi.logger.traceLimited(s"Environment: ${envInitial}")
    dxApi.logger.traceLimited(
        s"""|Evaluating workflow outputs
            |${WfOutputs.prettyPrintOutputs(wf.outputs)}
            |""".stripMargin
    )

    // Some of the inputs could be optional. If they are missing,
    // add in a None value.
    val allInputs = Block.outputClosure(wf.outputs)
    val envInitialFilled: Map[String, WdlValues.V] = allInputs.flatMap {
      case (name, wdlType) =>
        (envInitial.get(name), wdlType) match {
          case (None, WdlTypes.T_Optional(_)) =>
            Some(name -> WdlValues.V_Null)
          case (None, _) =>
            // input is missing, and there is no default at the callee,
            dxApi.logger.warning(s"value is missing for ${name}")
            None
          case (Some((_, v)), _) =>
            Some(name -> v)
        }
    }

    // Evaluate the output declarations. Add outputs evaluated to
    // the environment, so they can be referenced by expressions in the next
    // lines.
    var envFull = envInitialFilled
    val outputs: Map[String, (WdlTypes.T, WdlValues.V)] = wf.outputs.map {
      case TAT.OutputDefinition(name, wdlType, expr, _) =>
        val value = evaluateWdlExpression(expr, wdlType, envFull)
        envFull += (name -> value)
        name -> (wdlType, value)
    }.toMap

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = outputs
      .map {
        case (outputVarName, (wdlType, wdlValue)) =>
          val wvl = wdlVarLinksConverter.createLink(wdlType, wdlValue)
          wdlVarLinksConverter.createFields(wvl, outputVarName)
      }
      .toVector
      .flatten
      .toMap

    if (addStatus) {
      outputFields + (ReorgStatus -> JsString(ReorgStatusCompleted))
    } else {
      outputFields
    }
  }
}
