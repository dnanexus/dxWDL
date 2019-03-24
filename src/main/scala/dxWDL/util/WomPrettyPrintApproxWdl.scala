/** Pretty printing WOM as, approximately, the original WDL.
  */
package dxWDL.util

import wom.callable.Callable._
import wom.graph._
import wom.graph.expression._
import wom.types._

object WomPrettyPrintApproxWdl {

    def apply(node: GraphNode,
              indent : String = "") : String = {
        node match {
            case sct : ScatterNode =>
                val varNames = sct.scatterVariableNodes.map{svn => svn.identifier.localName.value}
                assert(varNames.size == 1)
                val varName = varNames.head
                val svNode: ScatterVariableNode = sct.scatterVariableNodes.head
                val collection = svNode.scatterExpressionNode.womExpression.sourceString
                val innerBlock =
                    sct.innerGraph.nodes.map{ node =>
                        apply(node, indent + "  ")
                    }.mkString("\n")
                s"""|${indent}scatter (${varName} in ${collection}) {
                    |${innerBlock}
                    |${indent}}""".stripMargin

            case cnd : ConditionalNode =>
                val innerBlock =
                    cnd.innerGraph.nodes.map{ node =>
                        apply(node, indent + "  ")
                    }.mkString("\n")
                s"""|${indent}if (${cnd.conditionExpression.womExpression.sourceString}) {
                    |${innerBlock}
                    |${indent}}
                    |""".stripMargin

            case call : CommandCallNode =>
                val inputNames = call.upstream.collect{
                    case exprNode: ExpressionNode =>
                        s"${exprNode.identifier.localName.value} = ${exprNode.womExpression.sourceString}"
                }.mkString(",")
                s"${indent}call ${call.identifier.localName.value} { input: ${inputNames} }"

            case expr :ExpressionBasedGraphOutputNode =>
                val exprSource = expr.womExpression.sourceString
                s"${indent}${expr.womType.stableName} ${expr.identifier.localName.value} = ${exprSource}"

            case expr : ExposedExpressionNode =>
                s"${indent}${expr.identifier.localName.value} =  ${expr.womExpression.sourceString}"

            case expr : ExpressionNode =>
                expr.womExpression.sourceString

            case _ : OuterGraphInputNode => ""

            case PortBasedGraphOutputNode(id, womType, sourcePort) =>
                s"${indent}${womType.stableName} ${id.localName.value} = ${sourcePort.name}"

            case other =>
                other.toString
        }
    }

    def apply(inputDef : InputDefinition) : String = {
        inputDef match {
            case RequiredInputDefinition(iName, womType, _, _) =>
                s"${womType.stableName} ${iName}"

            case OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
                s"${womType.stableName} ${iName} = ${defaultExpr.sourceString}"

            // An input whose value should always be calculated from the default, and is
            // not allowed to be overridden.
            case FixedInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
                s"${womType.stableName} ${iName} = ${defaultExpr.sourceString}"

            case OptionalInputDefinition(iName, WomOptionalType(womType), _, _) =>
                s"${womType.stableName}? ${iName}"

            case other =>
                throw new Exception(s"${other} not handled")
        }
    }
}
