package dxWDL.util

import wdlTools.eval.WdlValues
import wdlTools.types.{TypedAbstractSyntax => TAT, Util => TUtil, WdlTypes}
import wdlTools.eval.Coercion

import dxWDL.base.Utils

object WomValueAnalysis {

  private class ExprNotConst(message: String) extends RuntimeException(message)

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

    expr match {
      // Base case: primitive types.
      case _ : TAT.ValueNull => WdlValues.V_Null
      case _ : TAT.ValueNone => WdlValues.V_Null
      case b : TAT.ValueBoolean =>  WdlValues.V_Boolean(b.value)
      case i : TAT.ValueInt => WdlValues.V_Int(i.value)
      case x : TAT.ValueFloat => WdlValues.V_Float(x.value)
      case s : TAT.ValueString => WdlValues.V_String(s.value)
      case fl : TAT.ValueFile =>
        if (isMutableFile(fl.value))
          throw new Exception(s"file ${fl.value} is mutable")
        WdlValues.V_File(fl.value)

      // compound values
      case TAT.ExprPair(l, r, _, _) =>
        WdlValues.V_Pair(evalConst(l), evalConst(r))
      case TAT.ExprArray(elems, _, _) =>
        WdlValues.V_Array(elems.map(evalConst(_)).toVector)
      case TAT.ExprMap(m, _, _) =>
        WdlValues.V_Map(m.map{
                          case (k, v) => evalConst(k) -> evalConst(v)
                        })
      case TAT.ExprObject(m, wdlType, _) =>
        val m2 = m.map{
          case (k, v) => k -> evalConst(v)
        }
        wdlType match {
          case WdlTypes.T_Struct(name, _) => WdlValues.V_Struct(name, m2)
          case WdlTypes.T_Object =>  WdlValues.V_Object(m2)
        }
      case expr =>
        // anything else require evaluation
        throw new ExprNotConst(s"${TUtil.exprToString(expr)}")
    }
  }

  // A trivial expression has no operators, it is either a constant WdlValues.V
  // or a single identifier. For example: '5' and 'x' are trivial. 'x + y'
  // is not.
  def isTrivialExpression(expr: TAT.Expr): Boolean = {
    expr match {
      case _ : TAT.ValueNull => true
      case _ : TAT.ValueNone => true
      case _ : TAT.ValueBoolean => true
      case _ : TAT.ValueInt => true
      case _ : TAT.ValueFloat => true
      case _ : TAT.ValueString => true
      case _ : TAT.ValueFile => true
      case _ : TAT.ValueDirectory => true
      case _ : TAT.ExprIdentifier => true
      case _  => false
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
      Some(valueWithCorrectType)
    } catch {
      case _ : ExprNotConst =>
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
