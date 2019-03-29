/** Pretty printing WOM as, approximately, the original WDL.
  */
package dxWDL.util

import wom.callable.Callable._
import wom.graph._
import wom.graph.expression._
import wom.types._

object WomPrettyPrintApproxWdl {

    private def applyGNode(node: GraphNode,
                           indent : String) : Option[String] = {
        node match {
            case sct : ScatterNode =>
                val varNames = sct.scatterVariableNodes.map{svn => svn.identifier.localName.value}
                assert(varNames.size == 1)
                val varName = varNames.head
                val svNode: ScatterVariableNode = sct.scatterVariableNodes.head
                val collection = svNode.scatterExpressionNode.womExpression.sourceString
                val innerBlock =
                    sct.innerGraph.nodes.flatMap{ node =>
                        applyGNode(node, indent + "  ")
                    }.mkString("\n")
                Some(s"""|${indent}scatter (${varName} in ${collection}) {
                         |${innerBlock}
                         |${indent}}
                         |""".stripMargin)

            case cnd : ConditionalNode =>
                val innerBlock =
                    cnd.innerGraph.nodes.flatMap{ node =>
                        applyGNode(node, indent + "  ")
                    }.mkString("\n")
                Some(s"""|${indent}if (${cnd.conditionExpression.womExpression.sourceString}) {
                         |${innerBlock}
                         |${indent}}
                         |""".stripMargin)

            case call : CommandCallNode =>
                val inputNames = call.upstream.collect{
                    case exprNode: ExpressionNode =>
                        val paramName = Utils.getUnqualifiedName(exprNode.identifier.localName.value)
                        s"${paramName} = ${exprNode.womExpression.sourceString}"
                }.mkString(",")
                val calleeName = Utils.getUnqualifiedName(call.callable.name)
                val callName = call.identifier.localName.value
                val inputs =
                    if (inputNames.isEmpty) ""
                    else s"{ input: ${inputNames} }"
                if (callName == calleeName)
                    Some(s"${indent}call ${callName} ${inputs}")
                else
                    Some(s"${indent}call ${calleeName} as ${callName} ${inputs}")

            case expr :ExpressionBasedGraphOutputNode =>
                val exprSource = expr.womExpression.sourceString
                Some(s"${indent}${expr.womType.stableName} ${expr.identifier.localName.value} = ${exprSource}")

            case expr : ExposedExpressionNode =>
                Some(s"${indent}${expr.identifier.localName.value} =  ${expr.womExpression.sourceString}")

            case expr : ExpressionNode =>
                //Some(expr.womExpression.sourceString)
                None

            case _ : OuterGraphInputNode =>
                None

            case PortBasedGraphOutputNode(id, womType, sourcePort) =>
                //s"${indent}${womType.stableName} ${id.localName.value} = ${sourcePort.name}"
                None

            case _ : GraphInputNode =>
                None

            case other =>
                Some(other.toString)
        }
    }

    def apply(node: GraphNode) : String = {
        applyGNode(node, "") match {
            case None => ""
            case Some(x) => x
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
