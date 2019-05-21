// Execute a WDL workflow with no calls, and no expressions, only inputs and outputs.
// It could have expressions in the output section.

package dxWDL.exec

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.expression.WomExpression
import wom.graph._
import wom.values._
import wom.types._

import dxWDL.base.Utils
import dxWDL.util._

case class WfOutputs(wf: WorkflowDefinition,
                     wfSourceCode: String,
                     typeAliases : Map[String, WomType],
                     dxPathConfig : DxPathConfig,
                     dxIoFunctions : DxIoFunctions,
                     runtimeDebugLevel: Int) {
    private val verbose = runtimeDebugLevel >= 1
    //private val maxVerboseLevel = (runtimeDebugLevel == 2)
    private val wdlVarLinksConverter = WdlVarLinksConverter(dxIoFunctions.fileInfoDir, typeAliases)

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
        Utils.appletLog(verbose, s"Environment: ${envInitial}")
        val outputNodes : Vector[GraphOutputNode] = wf.innerGraph.outputNodes.toVector
        Utils.appletLog(verbose, s"""|Evaluating workflow outputs
                                     |${WomPrettyPrintApproxWdl.graphOutputs(outputNodes)}
                                     |""".stripMargin)

        // Evaluate the output declarations. Add outputs evaluated to
        // the environment, so they can be referenced by expressions in the next
        // lines.
        var envFull = envInitial
        val outputs: Map[String, WomValue] = outputNodes.map{
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

            case expr :ExpressionBasedGraphOutputNode =>
                val value = evaluateWomExpression(expr.womExpression,
                                                  expr.womType,
                                                  envFull)
                val name = expr.graphOutputPort.name
                envFull += (name -> value)
                name -> value

            case other =>
                throw new Exception(s"unhandled output ${other}")
        }.toMap

        // convert the WDL values to JSON
        val outputFields:Map[String, JsValue] = outputs.map {
            case (outputVarName, womValue) =>
                val wvl = wdlVarLinksConverter.importFromWDL(womValue.womType, womValue)
                wdlVarLinksConverter.genFields(wvl, outputVarName)
        }.toList.flatten.toMap
        outputFields
    }
}
