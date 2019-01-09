/*

Execute a limitied WDL workflow on the platform. The workflow can
contain declarations, nested if/scatter blocks, and one call
location. A simple example is `wf_scat`.

workflow wf_scat {
    String pattern
    Array[Int] numbers = [1, 3, 7, 15]
    Array[Int] index = range(length(numbers))

    scatter (i in index) {
        call inc {input: i=numbers[i]}
    }

  output {
    Array[Int] inc1_result = inc1.result
  }
}

A nested example:

workflow wf_cond {
   Int x

   if (x > 10) {
      scatter (b in [1, 3, 5, 7])
          call add {input: a=x, b=b}
   }
   output {
      Array[Int]? add_result = add.result
   }
}
*/

package dxWDL.runner

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.expression._
import wom.graph._
import wom.graph.expression._
import wom.values._

import dxWDL.util._

case class WfFragRunner(wf: WorkflowDefinition,
                        wfSourceCode: String,
                        instanceTypeDB: InstanceTypeDB,
                        execLinkInfo: Map[String, ExecLinkInfo],
                        dxPathConfig : DxPathConfig,
                        dxIoFunctions : DxIoFunctions,
                        runtimeDebugLevel: Int) {
    private val verbose = runtimeDebugLevel >= 1
    //private val maxVerboseLevel = (runtimeDebugLevel == 2)

    def evaluateWomExpression(expr: WomExpression, env: Map[String, WomValue]) : WomValue = {
        val result: ErrorOr[WomValue] =
            expr.evaluateValue(env, dxIoFunctions)
        result match {
            case Invalid(errors) => throw new Exception(
                s"Failed to evaluate expression ${expr} with ${errors}")
            case Valid(x: WomValue) => x
        }
    }

    def processOutputs(womOutputs: Map[String, WomValue],
                       //wvlVarOutputs: Map[String, WdlVarLinks], // TODO: support WdlVarLinks
                       outputNodes: Vector[GraphOutputNode]) : Map[String, JsValue] = {
        // convert from WVL to JSON
        womOutputs.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, value)) =>
                val wvl = WdlVarLinks.importFromWDL(value.womType, value)
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
        }.toMap
    }

    def apply(subBlockNr: Int,
              env: Map[String, WomValue]) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
        Utils.appletLog(verbose, s"link info=${execLinkInfo}")
        Utils.appletLog(verbose, s"Workflow source code:")
        Utils.appletLog(verbose, wfSourceCode, 10000)
        Utils.appletLog(verbose, s"Environment: ${env}")

        val graph = wf.innerGraph
        val (inputNodes, subBlocks, outputNodes) = Block.splitIntoBlocks(graph)

        val block = subBlocks(subBlockNr)

        val finalEnv : Map[String, WomValue] = block.nodes.foldLeft(env) {
            case (env, node : GraphNode) =>
                node match {
                    case eNode: ExposedExpressionNode =>
                        val value : WomValue = evaluateWomExpression(eNode.womExpression, env)
                        env + (node.identifier.workflowLocalName -> value)
                    case other =>
                        throw new Exception(s"${other.getClass} not implemented yet")
                }
        }

        processOutputs(finalEnv, outputNodes)
    }
}
