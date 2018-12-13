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

    def apply(iPort: GraphNodePort.InputPort) : String = {
        iPort match {
            case cip : GraphNodePort.ConnectedInputPort =>
                s"ConnectedInputPort(${cip.name})"
            case other =>
                s"(${other.getClass} ${other.name})"
        }
    }

    def apply(oPort: GraphNodePort.OutputPort) : String = {
        val ownerNode : String = apply(oPort.graphNode)
        oPort match {
            case gnop : GraphNodePort.GraphNodeOutputPort =>
                s"GraphNodeOutputPort(${gnop.identifier.localName.value}, ${ownerNode})"
            case ebop : GraphNodePort.ExpressionBasedOutputPort =>
                s"ExpressionBasedOutputPort(${ebop.identifier.localName.value}, ${ownerNode})"
            case other =>
                s"OutputPort(${other.getClass})"
        }
    }

    def apply(nodes: Seq[GraphNode]) : String = {
        nodes.map(apply).mkString("\n")
    }
}
