/** Pretty printing for WOM structures. This is used for debugging.
  */
package dxWDL.util

import wom.callable.Callable._
import wom.graph._
import wom.graph.expression._
import wom.types._

object WomPrettyPrint {

  def apply(node: GraphNode, indent: String = ""): String = {
    node match {
      case ssc: ScatterNode =>
        val ids = ssc.scatterVariableNodes.map { svn =>
          svn.identifier.localName.value
        }
        s"${indent}Scatter(${ids})"

      case cond: ConditionalNode =>
        val inner =
          cond.innerGraph.nodes
            .map { node =>
              apply(node, indent + "  ")
            }
            .mkString("\n")
        s"""|Conditional(${cond.conditionExpression.womExpression.sourceString}) {
            |${inner}
            |}
            |""".stripMargin

      case call: CommandCallNode =>
        val inputNames = call.inputPorts.map(_.name)
        s"CommandCall(${call.identifier.localName.value}, ${inputNames})"

      // Expression Nodes
      case expr: PlainAnonymousExpressionNode =>
        s"PlainAnonymousExpressionNode(${expr.identifier.localName.value}, ${expr.womExpression.sourceString}, ${expr.womType})"

      case expr: TaskCallInputExpressionNode =>
        s"TaskCallInputExpressionNode(${expr.identifier.localName.value}, ${expr.womExpression.sourceString}, ${expr.womType}, ${expr.singleOutputPort})"

      case expr: ExpressionBasedGraphOutputNode =>
        s"ExpressionBasedGraphOutputNode(${expr.identifier.localName.value}, ${expr.womExpression.sourceString}, ${expr.womType})"

      case expr: ExpressionNode =>
        s"ExpressionNode(${expr.getClass}, ${expr.identifier.localName.value}, ${expr.womExpression.sourceString}, ${expr.womType})"

      // Graph input nodes
      case egin: ExternalGraphInputNode =>
        s"ExternalGraphInputNode(${egin.nameInInputSet})"

      // Graph output nodes
      case pbgon: PortBasedGraphOutputNode =>
        s"PortBasedGraphOutputNode(${pbgon.identifier.localName.value})"

      case svNode: ScatterVariableNode =>
        val expr = svNode.scatterExpressionNode
        s"ScatterVariableNode[OGIN](${svNode.identifier.localName.value}, ${expr.womExpression.sourceString}, ${expr.womType})"

      case ogin: OuterGraphInputNode =>
        s"OuterGraphInputNode(${ogin.identifier.localName.value})"

      case other =>
        s"${other.getClass}"
    }
  }

  def apply(iPort: GraphNodePort.InputPort): String = {
    iPort match {
      case cip: GraphNodePort.ConnectedInputPort =>
        s"ConnectedInputPort(${cip.name})"
      case other =>
        s"(${other.getClass} ${other.name})"
    }
  }

  def apply(oPort: GraphNodePort.OutputPort): String = {
    val ownerNode: String = apply(oPort.graphNode)
    oPort match {
      case gnop: GraphNodePort.GraphNodeOutputPort =>
        s"GraphNodeOutputPort(${gnop.identifier.localName.value}, ${ownerNode})"
      case ebop: GraphNodePort.ExpressionBasedOutputPort =>
        s"ExpressionBasedOutputPort(${ebop.identifier.localName.value}, ${ownerNode})"
      case other =>
        s"OutputPort(${other.getClass})"
    }
  }

  def apply(inputDef: InputDefinition): String = {
    inputDef match {
      // A required input, no default.
      case RequiredInputDefinition(iName, womType, _, _) =>
        s"RequiredInputDefinition(${iName}, ${womType})"

      // An input definition that has a default value supplied.
      // Typical WDL example would be a declaration like: "Int x = 5"
      case OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
        s"InputDefinitionWithDefault(${iName}, ${womType})"

      // An input whose value should always be calculated from the default, and is
      // not allowed to be overridden.
      case FixedInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
        s"FixedInputDefinition(${iName}, ${womType},  default=${defaultExpr})"

      case OptionalInputDefinition(iName, WomOptionalType(womType), _, _) =>
        s"OptionalInputDefinition(${iName}, ${womType})"
    }
  }

  def apply(nodes: Seq[GraphNode]): String = {
    nodes.map(node => apply(node)).mkString("\n")
  }
}
