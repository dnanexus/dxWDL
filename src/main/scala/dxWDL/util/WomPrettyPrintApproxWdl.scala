/** Pretty printing WOM as, approximately, the original WDL.
  */
package dxWDL.util

import wom.graph._
import wom.graph.expression._

object WomPrettyPrintApproxWDL {

    def apply(node: GraphNode,
              indent : String = "") : Option[String] = {
        node match {
            case sct : ScatterNode =>
                val varNames = sct.scatterVariableNodes.map{svn => svn.identifier.localName.value}
                assert(varNames.size == 1)
                val varName = varNames.head
                val svNode: ScatterVariableNode = sct.scatterVariableNodes.head
                val collection = svNode.scatterExpressionNode.womExpression.sourceString
                val innerBlock =
                    sct.innerGraph.nodes.flatMap{ node =>
                        apply(node, indent + "  ")
                    }.mkString("\n")
                Some(s"""|${indent}scatter (${varName} in ${collection}) {
                         |${innerBlock}
                         |${indent}}""".stripMargin)

            case cnd : ConditionalNode =>
                val innerBlock =
                    cnd.innerGraph.nodes.flatMap{ node =>
                        apply(node, indent + "  ")
                    }.mkString("\n")
                Some(s"""|${indent}if (${cnd.conditionExpression.womExpression.sourceString}) {
                         |${innerBlock}
                         |${indent}}
                         |""".stripMargin)

            case call : CommandCallNode =>
                val inputNames = call.inputPorts.map(_.name)
                Some(s"${indent}call ${call.identifier.localName.value} { input: ${inputNames} }")

            case expr : ExposedExpressionNode =>
                Some(s"${indent}${expr.identifier.localName.value} =  ${expr.womExpression.sourceString}")

            case other =>
                None
        }
    }

    def apply(nodes: Seq[GraphNode]) : String = {
        nodes.flatMap(node => apply(node)).mkString("\n")
    }
}
