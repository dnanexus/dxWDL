/** Pretty printing for WOM structures
  */
package dxWDL.util

import wom.graph._
import wom.graph.expression._

object WomPrettyPrint {

    def apply(node: GraphNode) : String = {
        node match {
            case ssc : ScatterNode =>
                val ids = ssc.scatterVariableNodes.map{svn => svn.identifier.localName.value}
                s"Scatter(${ids})"

            case cond : ConditionalNode => s"Conditional(${cond.conditionExpression.womExpression.sourceString})"
            case call : CommandCallNode =>
                //val callInputs = call.upstream.map(apply)
                //s"CommandCall(${call.identifier.localName.value}, <${callInputs}>)"
                val inputNames = call.inputPorts.map(_.name)
                s"CommandCall(${call.identifier.localName.value}, ${inputNames})"

            // Expression Nodes
            case expr : PlainAnonymousExpressionNode =>
                s"PlainAnonymousExpressionNode(${expr.identifier.localName.value}, ${expr.womExpression.sourceString}, ${expr.womType})"

            case expr : TaskCallInputExpressionNode =>
                s"TaskCallInputExpressionNode(${expr.identifier.localName.value}, ${expr.womExpression.sourceString}, ${expr.womType}, ${expr.singleOutputPort})"

            case expr : ExpressionBasedGraphOutputNode =>
                s"ExpressionBasedGraphOutputNode(${expr.identifier.localName.value}, ${expr.womExpression.sourceString}, ${expr.womType})"

            case expr : ExpressionNode =>
                s"ExpressionNode(${expr.getClass}, ${expr.identifier.localName.value}, ${expr.womExpression.sourceString}, ${expr.womType})"

            // Graph input nodes
            case egin : ExternalGraphInputNode =>
                s"ExternalGraphInputNode(${egin.nameInInputSet})"

            // Graph output nodes
            case gon : PortBasedGraphOutputNode =>
                s"PortBasedGraphOutputNode"

            case other =>
                s"${other.getClass}"
        }
    }
}
