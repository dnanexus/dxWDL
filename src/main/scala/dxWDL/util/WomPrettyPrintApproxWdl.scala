/** Pretty printing WOM as, approximately, the original WDL.
  */
package dxWDL.util

import wom.callable.Callable._
import wom.graph._
import wom.graph.expression._
import wom.types._

import dxWDL.base.WomTypeSerialization.typeName

object WomPrettyPrintApproxWdl {

  // Convert a fully qualified name to a local name.
  // Examples:
  //   SOURCE         RESULT
  //   lib.concat     concat
  //   lib.sum_list   sum_list
  private def getUnqualifiedName(fqn: String): String = {
    if (fqn contains ".")
      fqn.split("\\.").last
    else
      fqn
  }

  private def applyGNode(node: GraphNode, indent: String): Option[String] = {
    node match {
      case sct: ScatterNode =>
        val varNames = sct.scatterVariableNodes.map { svn =>
          svn.identifier.localName.value
        }
        assert(varNames.size == 1)
        val varName = varNames.head
        val svNode: ScatterVariableNode = sct.scatterVariableNodes.head
        val collection = svNode.scatterExpressionNode.womExpression.sourceString
        val orderedNodes = Block.partialSortByDep(sct.innerGraph.nodes)
        val innerBlock = orderedNodes
          .flatMap { node =>
            applyGNode(node, indent + "  ")
          }
          .mkString("\n")
        Some(s"""|${indent}scatter (${varName} in ${collection}) {
                         |${innerBlock}
                         |${indent}}
                         |""".stripMargin)

      case cnd: ConditionalNode =>
        val orderedNodes = Block.partialSortByDep(cnd.innerGraph.nodes)
        val innerBlock =
          orderedNodes
            .flatMap { node =>
              applyGNode(node, indent + "  ")
            }
            .mkString("\n")
        Some(s"""|${indent}if ${cnd.conditionExpression.womExpression.sourceString} {
                         |${innerBlock}
                         |${indent}}
                         |""".stripMargin)

      case call: CommandCallNode =>
        val inputNames = call.upstream
          .collect {
            case exprNode: ExpressionNode =>
              val paramName = getUnqualifiedName(exprNode.identifier.localName.value)
              s"${paramName} = ${exprNode.womExpression.sourceString}"
          }
          .mkString(",")
        val calleeName = getUnqualifiedName(call.callable.name)
        val callName = call.identifier.localName.value
        val inputs =
          if (inputNames.isEmpty) ""
          else s"{ input: ${inputNames} }"
        if (callName == calleeName)
          Some(s"${indent}call ${callName} ${inputs}")
        else
          Some(s"${indent}call ${calleeName} as ${callName} ${inputs}")

      case expr: ExpressionBasedGraphOutputNode =>
        val exprSource = expr.womExpression.sourceString
        Some(
            s"${indent}${typeName(expr.womType)} ${expr.identifier.localName.value} = ${exprSource}"
        )

      case expr: ExposedExpressionNode =>
        Some(
            s"${indent}${typeName(expr.womType)} ${expr.identifier.localName.value} =  ${expr.womExpression.sourceString}"
        )

      case expr: ExpressionNode =>
        //Some(expr.womExpression.sourceString)
        None

      case _: OuterGraphInputNode =>
        None

      case PortBasedGraphOutputNode(id, womType, sourcePort) =>
        None

      case _: GraphInputNode =>
        None

      case other =>
        Some(other.toString)
    }
  }

  private def applyInput(iDef: InputDefinition): String = {
    iDef match {
      case RequiredInputDefinition(iName, womType, _, _) =>
        s"${typeName(womType)} ${iName}"

      case OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
        s"${typeName(womType)} ${iName} = ${defaultExpr.sourceString}"

      // An input whose value should always be calculated from the default, and is
      // not allowed to be overridden.
      case FixedInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
        s"${typeName(womType)} ${iName} = ${defaultExpr.sourceString}"

      case OptionalInputDefinition(iName, WomOptionalType(womType), _, _) =>
        s"${typeName(womType)}? ${iName}"

      case other =>
        throw new Exception(s"${other} not handled")
    }
  }

  def graphInputs(inputDefs: Seq[InputDefinition]): String = {
    inputDefs
      .map {
        applyInput(_)
      }
      .mkString("\n")
  }

  def graphOutputs(outputs: Seq[GraphOutputNode]): String = {
    outputs
      .flatMap {
        applyGNode(_, "    ")
      }
      .mkString("\n")
  }

  def block(block: Block): String = {
    block.nodes
      .flatMap {
        applyGNode(_, "    ")
      }
      .mkString("\n")
  }
}
