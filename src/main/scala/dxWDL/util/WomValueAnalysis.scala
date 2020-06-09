package dxWDL.util

import wdlTools.eval.WdlValues
import wdlTools.types.{TypedAbstractSyntax => TAT, Util => TUtil, WdlTypes}
import wdlTools.eval.Coercion

import dxWDL.base.Utils

object WomValueAnalysis {

  private class ExprNotConst(message: String) extends RuntimeException(message)

  def isMutableFile(constantFileStr: String): Boolean = {
    constantFileStr match {
      case path if path.startsWith(Utils.DX_URL_PREFIX) =>
        // platform files are immutable
        false
      case path if path contains "://" =>
        throw new Exception(s"protocol not supported, cannot access ${path}")
      case _ =>
        // anything else might be mutable
        true
    }
  }

  // Evaluate a constant wdl expression. Throw an exception if it isn't a constant.
  //
  // Examine task foo:
  //
  // task foo {
  //    command {
  //       # create file mut.vcf
  //    }
  //    output {
  //       File mutations = "mut.vcf"
  //    }
  // }
  //
  // File 'mutations' is not a constant output. It is generated,
  // read from disk, and uploaded to the platform.  A file can't
  // have a constant string as an input, this has to be a dnanexus
  // link.
  def evalConst(expr: TAT.Expr): WdlValues.V = {
    expr match {
      // Base case: primitive types.
      case _: TAT.ValueNull    => WdlValues.V_Null
      case _: TAT.ValueNone    => WdlValues.V_Null
      case b: TAT.ValueBoolean => WdlValues.V_Boolean(b.value)
      case i: TAT.ValueInt     => WdlValues.V_Int(i.value)
      case x: TAT.ValueFloat   => WdlValues.V_Float(x.value)
      case s: TAT.ValueString  => WdlValues.V_String(s.value)
      case fl: TAT.ValueFile =>
        if (isMutableFile(fl.value))
          throw new ExprNotConst(s"file ${fl.value} is mutable")
        WdlValues.V_File(fl.value)

      // compound values
      case TAT.ExprPair(l, r, _, _) =>
        WdlValues.V_Pair(evalConst(l), evalConst(r))
      case TAT.ExprArray(elems, _, _) =>
        WdlValues.V_Array(elems.map(evalConst(_)).toVector)
      case TAT.ExprMap(m, _, _) =>
        WdlValues.V_Map(m.map {
          case (k, v) => evalConst(k) -> evalConst(v)
        })
      case TAT.ExprObject(m, wdlType, _) =>
        val m2 = m.map {
          case (k, v) => k -> evalConst(v)
        }
        wdlType match {
          case WdlTypes.T_Struct(name, _) => WdlValues.V_Struct(name, m2)
          case _                          => WdlValues.V_Object(m2)
        }
      case expr =>
        // anything else require evaluation
        throw new ExprNotConst(s"${TUtil.exprToString(expr)}")
    }
  }

  def checkForLocalFiles(v: WdlValues.V): Unit = {
    v match {
      case WdlValues.V_Null       => ()
      case WdlValues.V_Boolean(_) => ()
      case WdlValues.V_Int(_)     => ()
      case WdlValues.V_Float(_)   => ()
      case WdlValues.V_String(_)  => ()
      case WdlValues.V_File(value) =>
        if (isMutableFile(value))
          throw new ExprNotConst(s"file ${value} is mutable")
        ()
      case WdlValues.V_Pair(l, r) =>
        checkForLocalFiles(l)
        checkForLocalFiles(r)

      case WdlValues.V_Array(value) =>
        value.foreach(checkForLocalFiles(_))
      case WdlValues.V_Map(value) =>
        value.foreach {
          case (k, v) =>
            checkForLocalFiles(k)
            checkForLocalFiles(v)
        }
      case WdlValues.V_Optional(value) =>
        checkForLocalFiles(value)
      case WdlValues.V_Struct(_, members) =>
        members.values.foreach(checkForLocalFiles(_))
      case WdlValues.V_Object(members) =>
        members.values.foreach(checkForLocalFiles(_))
      case other =>
        throw new Exception(s"unhandled case ${other}")
    }
  }

  // A trivial expression has no operators, it is either a constant WdlValues.V
  // or a single identifier. For example: '5' and 'x' are trivial. 'x + y'
  // is not.
  def isTrivialExpression(expr: TAT.Expr): Boolean = {
    expr match {
      case _: TAT.ValueNull      => true
      case _: TAT.ValueNone      => true
      case _: TAT.ValueBoolean   => true
      case _: TAT.ValueInt       => true
      case _: TAT.ValueFloat     => true
      case _: TAT.ValueString    => true
      case _: TAT.ValueFile      => true
      case _: TAT.ValueDirectory => true
      case _: TAT.ExprIdentifier => true

      // Access a field in a call
      //   Int z = eliminateDuplicate.fields
      case TAT.ExprGetName(TAT.ExprIdentifier(_, _: WdlTypes.T_Call, _), _, _, _) =>
        true

      case _ =>
        false
    }
  }

  // Check if the WDL expression is a constant. If so, calculate and return it.
  // Otherwise, return None.
  //
  def ifConstEval(wdlType: WdlTypes.T, expr: TAT.Expr): Option[WdlValues.V] = {
    try {
      val value = evalConst(expr)
      val coercion = new Coercion(None)
      val valueWithCorrectType = coercion.coerceTo(wdlType, value, expr.text)
      checkForLocalFiles(valueWithCorrectType)
      Some(valueWithCorrectType)
    } catch {
      case _: ExprNotConst =>
        None
    }
  }

  def isExpressionConst(wdlType: WdlTypes.T, expr: TAT.Expr): Boolean = {
    ifConstEval(wdlType, expr) match {
      case None    => false
      case Some(_) => true
    }
  }

  def evalConst(wdlType: WdlTypes.T, expr: TAT.Expr): WdlValues.V = {
    ifConstEval(wdlType, expr) match {
      case None           => throw new Exception(s"Expression ${expr} is not a WDL constant")
      case Some(wdlValue) => wdlValue
    }
  }

}
