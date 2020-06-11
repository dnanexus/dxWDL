/** Pretty printing typed AST as, approximately, the original WDL.
  */
package dxWDL.util

import wdlTools.types.{TypedAbstractSyntax => TAT, Util => TUtil}

object TypedAstPrettyPrintApproxWdl {

  // TODO: this function should no longer be necessary - the wdlTools code formatter
  //  can be used instead. This function is currently only used in logging statements.
  def applyWorkflowElement(node: TAT.WorkflowElement, indent: String): String = {
    node match {
      case TAT.Scatter(varName, expr, body, _) =>
        val collection = TUtil.exprToString(expr)
        val innerBlock = body
          .map { node =>
            applyWorkflowElement(node, indent + "  ")
          }
          .mkString("\n")
        s"""|${indent}scatter (${varName} in ${collection}) {
            |${innerBlock}
            |${indent}}
            |""".stripMargin

      case TAT.Conditional(expr, body, _) =>
        val innerBlock =
          body
            .map { node =>
              applyWorkflowElement(node, indent + "  ")
            }
            .mkString("\n")
        s"""|${indent}if (${TUtil.exprToString(expr)}) {
            |${innerBlock}
            |${indent}}
            |""".stripMargin

      case call: TAT.Call =>
        val inputNames = call.inputs
          .map {
            case (key, expr) =>
              s"${key} = ${TUtil.exprToString(expr)}"
          }
          .mkString(",")
        val inputs =
          if (inputNames.isEmpty) ""
          else s"{ input: ${inputNames} }"
        call.alias match {
          case None =>
            s"${indent}call ${call.fullyQualifiedName} ${inputs}"
          case Some(al) =>
            s"${indent}call ${call.fullyQualifiedName} as ${al} ${inputs}"
        }

      case TAT.Declaration(_, wdlType, None, _) =>
        s"${indent} ${TUtil.typeToString(wdlType)}"
      case TAT.Declaration(_, wdlType, Some(expr), _) =>
        s"${indent} ${TUtil.typeToString(wdlType)} = ${TUtil.exprToString(expr)}"
    }
  }

  private def applyInput(iDef: TAT.InputDefinition): String = {
    iDef match {
      case TAT.RequiredInputDefinition(iName, wdlType, _) =>
        s"${TUtil.typeToString(wdlType)} ${iName}"

      case TAT.OverridableInputDefinitionWithDefault(iName, wdlType, defaultExpr, _) =>
        s"${TUtil.typeToString(wdlType)} ${iName} = ${TUtil.exprToString(defaultExpr)}"

      case TAT.OptionalInputDefinition(iName, wdlType, _) =>
        s"${TUtil.typeToString(wdlType)} ${iName}"
    }
  }

  def graphInputs(inputDefs: Seq[TAT.InputDefinition]): String = {
    inputDefs.map(applyInput).mkString("\n")
  }

  def graphOutputs(outputs: Seq[TAT.OutputDefinition]): String = {
    outputs
      .map {
        case TAT.OutputDefinition(name, wdlType, expr, _) =>
          s"${TUtil.typeToString(wdlType)} ${name} = ${TUtil.exprToString(expr)}"
      }
      .mkString("\n")
  }

  def block(block: Block): String = {
    block.nodes
      .map { applyWorkflowElement(_, "    ") }
      .mkString("\n")
  }

  def apply(elements: Vector[TAT.WorkflowElement]): String = {
    elements.map(x => applyWorkflowElement(x, "  ")).mkString("\n")
  }
}
