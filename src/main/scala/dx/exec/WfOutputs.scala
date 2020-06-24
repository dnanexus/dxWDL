package dx.exec

import dx.core.io.DxPathConfig
import dx.core.languages.wdl.{
  DxFileAccessProtocol,
  Evaluator,
  PrettyPrintApprox,
  WdlVarLinksConverter
}
import dx.util.{Logger, Verbose}
import spray.json.{JsString, JsValue}
import wdlTools.eval.WdlValues
import wdlTools.types.WdlTypes

case class WfOutputs(wf: TAT.Workflow,
                     document: TAT.Document,
                     typeAliases: Map[String, WdlTypes.T],
                     dxPathConfig: DxPathConfig,
                     dxIoFunctions: DxFileAccessProtocol,
                     runtimeDebugLevel: Int) {
  private val verbose = runtimeDebugLevel >= 1
  //private val maxVerboseLevel = (runtimeDebugLevel == 2)
  private val utlVerbose = Verbose(runtimeDebugLevel >= 1, quiet = false, Set.empty)
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(utlVerbose, dxIoFunctions.fileInfoDir, typeAliases)

  private val evaluator = Evaluator.make(dxIoFunctions, document.version.value)

  private def evaluateWdlExpression(expr: TAT.Expr,
                                    wdlType: WdlTypes.T,
                                    env: Map[String, WdlValues.V]): WdlValues.V = {
    evaluator.applyExprAndCoerce(expr, wdlType, EvalContext(env))
  }

  def apply(envInitial: Map[String, (WdlTypes.T, WdlValues.V)],
            addStatus: Boolean = false): Map[String, JsValue] = {
    Logger.appletLog(verbose, s"dxWDL version: ${getVersion}")
    Logger.appletLog(verbose, s"Environment: ${envInitial}")
    Logger.appletLog(
        verbose,
        s"""|Evaluating workflow outputs
            |${PrettyPrintApprox.graphOutputs(wf.outputs)}
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
            Logger.warning(utlVerbose, s"value is missing for ${name}")
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
      outputFields + (REORG_STATUS -> JsString(REORG_STATUS_COMPLETE))
    } else {
      outputFields
    }
  }
}
