package wdlTools.syntax

import AbstractSyntax._

object Util {
  // Utility function for writing an expression in a human readable form
  def exprToString(expr: Expr): String = {
    expr match {
      case ValueNull(_)                    => "null"
      case ValueString(value, _)           => value
      case ValueFile(value, _)             => value
      case ValueBoolean(value: Boolean, _) => value.toString
      case ValueInt(value, _)              => value.toString
      case ValueFloat(value, _)            => value.toString
      case ExprIdentifier(id: String, _)   => id

      case ExprCompoundString(value: Vector[Expr], _) =>
        val vec = value.map(exprToString).mkString(", ")
        s"ExprCompoundString(${vec})"
      case ExprPair(l, r, _) => s"(${exprToString(l)}, ${exprToString(r)})"
      case ExprArray(value: Vector[Expr], _) =>
        "[" + value.map(exprToString).mkString(", ") + "]"
      case ExprMap(value: Vector[ExprMapItem], _) =>
        val m = value
          .map(exprToString)
          .mkString(", ")
        "{ " + m + " }"
      case ExprMapItem(key, value, _) =>
        s"${exprToString(key)} : ${exprToString(value)}"
      case ExprObject(value: Vector[ExprObjectMember], _) =>
        val m = value
          .map(exprToString)
          .mkString(", ")
        s"object($m)"
      case ExprObjectMember(key, value, _) =>
        s"${key} : ${exprToString(value)}"
      // ~{true="--yes" false="--no" boolean_value}
      case ExprPlaceholderEqual(t: Expr, f: Expr, value: Expr, _) =>
        s"{true=${exprToString(t)} false=${exprToString(f)} ${exprToString(value)}"

      // ~{default="foo" optional_value}
      case ExprPlaceholderDefault(default: Expr, value: Expr, _) =>
        s"{default=${exprToString(default)} ${exprToString(value)}}"

      // ~{sep=", " array_value}
      case ExprPlaceholderSep(sep: Expr, value: Expr, _) =>
        s"{sep=${exprToString(sep)} ${exprToString(value)}"

      // operators on one argument
      case ExprUniraryPlus(value: Expr, _) =>
        s"+ ${exprToString(value)}"
      case ExprUniraryMinus(value: Expr, _) =>
        s"- ${exprToString(value)}"
      case ExprNegate(value: Expr, _) =>
        s"not(${exprToString(value)})"

      // operators on two arguments
      case ExprLor(a: Expr, b: Expr, _)    => s"${exprToString(a)} || ${exprToString(b)}"
      case ExprLand(a: Expr, b: Expr, _)   => s"${exprToString(a)} && ${exprToString(b)}"
      case ExprEqeq(a: Expr, b: Expr, _)   => s"${exprToString(a)} == ${exprToString(b)}"
      case ExprLt(a: Expr, b: Expr, _)     => s"${exprToString(a)} < ${exprToString(b)}"
      case ExprGte(a: Expr, b: Expr, _)    => s"${exprToString(a)} >= ${exprToString(b)}"
      case ExprNeq(a: Expr, b: Expr, _)    => s"${exprToString(a)} != ${exprToString(b)}"
      case ExprLte(a: Expr, b: Expr, _)    => s"${exprToString(a)} <= ${exprToString(b)}"
      case ExprGt(a: Expr, b: Expr, _)     => s"${exprToString(a)} > ${exprToString(b)}"
      case ExprAdd(a: Expr, b: Expr, _)    => s"${exprToString(a)} + ${exprToString(b)}"
      case ExprSub(a: Expr, b: Expr, _)    => s"${exprToString(a)} - ${exprToString(b)}"
      case ExprMod(a: Expr, b: Expr, _)    => s"${exprToString(a)} % ${exprToString(b)}"
      case ExprMul(a: Expr, b: Expr, _)    => s"${exprToString(a)} * ${exprToString(b)}"
      case ExprDivide(a: Expr, b: Expr, _) => s"${exprToString(a)} / ${exprToString(b)}"

      // Access an array element at [index]
      case ExprAt(array: Expr, index: Expr, _) =>
        s"${exprToString(array)}[${index}]"

      // conditional:
      // if (x == 1) then "Sunday" else "Weekday"
      case ExprIfThenElse(cond: Expr, tBranch: Expr, fBranch: Expr, _) =>
        s"if (${exprToString(cond)}) then ${exprToString(tBranch)} else ${exprToString(fBranch)}"

      // Apply a standard library function to arguments. For example:
      //   read_int("4")
      case ExprApply(funcName: String, elements: Vector[Expr], _) =>
        val args = elements.map(exprToString).mkString(", ")
        s"${funcName}(${args})"

      case ExprGetName(e: Expr, id: String, _) =>
        s"${exprToString(e)}.${id}"
    }
  }
}
