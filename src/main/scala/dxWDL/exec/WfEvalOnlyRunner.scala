// Execute a WDL workflow with no calls, and no expressions, only inputs and outputs.
// It could have expressions in the output section.

package dxWDL.exec

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.callable.Callable.OutputDefinition
import wom.expression._
import wom.values._
import wom.types._

import dxWDL.util._

case class WfEvalOnlyRunner(wf: WorkflowDefinition,
                            wfSourceCode: String,
                            instanceTypeDB: InstanceTypeDB,
                            execLinkInfo: Map[String, ExecLinkInfo],
                            dxPathConfig : DxPathConfig,
                            dxIoFunctions : DxIoFunctions,
                            inputsRaw : JsValue,
                            fragInputOutput : WfFragInputOutput,
                            runtimeDebugLevel: Int) {
    private val verbose = runtimeDebugLevel >= 1
    //private val maxVerboseLevel = (runtimeDebugLevel == 2)

    private def evaluateWomExpression(expr: WomExpression,
                                      womType: WomType,
                                      env: Map[String, WomValue]) : WomValue = {
        val result: ErrorOr[WomValue] =
            expr.evaluateValue(env, dxIoFunctions)
        val value = result match {
            case Invalid(errors) => throw new Exception(
                s"Failed to evaluate expression ${expr} with ${errors}")
            case Valid(x: WomValue) => x
        }

        // cast the result value to the correct type
        // For example, an expression like:
        //   Float x = "3.2"
        // requires casting from string to float
        womType.coerceRawValue(value).get
    }

    def apply(envInitial: Map[String, WomValue]) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
        Utils.appletLog(verbose, s"link info=${execLinkInfo}")
        Utils.appletLog(verbose, s"Environment: ${envInitial}")

        assert(wf.innerGraph.nodes.isEmpty)

        // Evaluate the output declarations. Add outputs evaluated to
        // the environment, so they can be referenced by expressions in the next
        // lines.
        var envFull = envInitial
        val outputs: Map[String, WomValue] = wf.outputs.map{
            case (outDef: OutputDefinition) =>
                val value = evaluateWomExpression(outDef.expression,
                                                  outDef.womType,
                                                  envFull)
                envFull += (outDef.name -> value)
                outDef.name -> value
        }.toMap

        // convert the WDL values to JSON
        val outputFields:Map[String, JsValue] = outputs.map {
            case (fullOutputVarName, womValue) =>
                // strip the prefix "xyz_"
                assert(fullOutputVarName.startsWith(Utils.OUTPUT_VAR_PREFIX))
                val outputVarName = fullOutputVarName.substring(Utils.OUTPUT_VAR_PREFIX.length)

                val wvl = WdlVarLinks.importFromWDL(womValue.womType, womValue)
                WdlVarLinks.genFields(wvl, outputVarName)
        }.toList.flatten.toMap
        outputFields
    }
}
