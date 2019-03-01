/** Pretty printing WOM as, approximately, the original WDL.
  */
package dxWDL.util

import wom.callable.Callable._
import wom.graph._
import wom.graph.expression._
import wom.types._

object WomPrettyPrintApproxWdl {

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
                val inputNames = call.upstream.collect{
                    case exprNode: ExpressionNode =>
                        s"${exprNode.identifier.localName.value} = ${exprNode.womExpression.sourceString}"
                }.mkString(",")
                Some(s"${indent}call ${call.identifier.localName.value} { input: ${inputNames} }")

            case expr : ExposedExpressionNode =>
                Some(s"${indent}${expr.identifier.localName.value} =  ${expr.womExpression.sourceString}")

            case other =>
                None
        }
    }

    def apply(inputDef : InputDefinition) : String = {
        inputDef match {
            case RequiredInputDefinition(iName, womType, _, _) =>
                s"${womType.toDisplayString} ${iName}"

            case InputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
                s"${womType.toDisplayString} ${iName} = ${defaultExpr.sourceString}"

            // An input whose value should always be calculated from the default, and is
            // not allowed to be overridden.
            case FixedInputDefinition(iName, womType, defaultExpr, _, _) =>
                s"${womType.toDisplayString} ${iName} = ${defaultExpr.sourceString}"

            case OptionalInputDefinition(iName, WomOptionalType(womType), _, _) =>
                s"${womType.toDisplayString}? ${iName}"

            case other =>
                throw new Exception(s"${other} not handled")
        }
    }
}
