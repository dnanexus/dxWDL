package dx.exec

import dx.api.DxApi
import dx.core.languages.wdl.{PrettyPrintApprox, WdlDxLinkSerde}
import dx.core.getVersion
import spray.json.JsValue
import wdlTools.eval.{Context => EvalContext, Eval, WdlValues}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}

case class WfInputs(wf: TAT.Workflow,
                    document: TAT.Document,
                    wdlVarLinksConverter: WdlDxLinkSerde,
                    dxApi: DxApi,
                    evaluator: Eval) {
  def apply(inputs: Map[String, (WdlTypes.T, WdlValues.V)]): Map[String, JsValue] = {
    dxApi.logger.traceLimited(s"dxWDL version: ${getVersion}")
    dxApi.logger.traceLimited(s"Environment: ${inputs}")
    dxApi.logger.traceLimited(
        s"""|Artificial applet for unlocked workflow inputs
            |${PrettyPrintApprox.graphInputs(wf.inputs)}
            |""".stripMargin
    )

    // evaluate any non-overridden default expressions
    var ctx = EvalContext(inputs.map { case (name, (_, value)) => name -> value })
    val evaluatedInputs: Vector[(String, (WdlTypes.T, WdlValues.V))] = wf.inputs.flatMap {
      case i: TAT.InputDefinition if inputs.contains(i.name) =>
        // input with user-provided value
        Some(i.name -> inputs(i.name))
      case TAT.RequiredInputDefinition(name, _, _) if !inputs.contains(name) =>
        // input is required, so we expect it to be in env
        throw new Exception(s"Missing required input ${name}")
      case TAT.OverridableInputDefinitionWithDefault(name, wdlType, defaultExpr, _)
          if !inputs.contains(name) =>
        // optional input with a default that needs to be evaluated
        // TODO: order inputs by dependencies, since input expressions may
        //  reference each other
        val value = evaluator.applyExprAndCoerce(defaultExpr, wdlType, ctx)
        ctx = ctx.addBinding(name, value)
        Some(name -> (wdlType, value))
      case i: TAT.InputDefinition =>
        dxApi.logger.trace(s"Optional input ${i.name} not provided")
        None
    }

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = evaluatedInputs.flatMap {
      case (outputVarName, (wdlType, wdlValue)) =>
        val wvl = wdlVarLinksConverter.createLink(wdlType, wdlValue)
        wdlVarLinksConverter.createFields(wvl, outputVarName)
    }.toMap

    outputFields
  }
}
