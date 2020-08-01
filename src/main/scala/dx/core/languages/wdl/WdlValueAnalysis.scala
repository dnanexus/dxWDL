package dx.core.languages.wdl

import dx.api.DxPath
import wdlTools.eval.{Coercion, Eval, EvalPaths, WdlValues}
import wdlTools.syntax.WdlVersion
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT, Utils => TUtils}

object WdlValueAnalysis {
  // create an evaluator that will only be used for constant expressions -
  // the WdlVersion doesn't matter since nothing to do with constants is
  // version-specific
  private lazy val constEvaluator: Eval = Eval(EvalPaths.empty, WdlVersion.V1)

  private def isMutableFile(constantFileStr: String): Boolean = {
    constantFileStr match {
      case path if path.startsWith(DxPath.DX_URL_PREFIX) =>
        // platform files are immutable
        false
      case path if path contains "://" =>
        throw new Exception(s"protocol not supported, cannot access ${path}")
      case _ =>
        // anything else might be mutable
        true
    }
  }

  def filterFiles()
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
        value.foreach(checkForLocalFiles)
      case WdlValues.V_Map(value) =>
        value.foreach {
          case (k, v) =>
            checkForLocalFiles(k)
            checkForLocalFiles(v)
        }
      case WdlValues.V_Optional(value) =>
        checkForLocalFiles(value)
      case WdlValues.V_Struct(_, members) =>
        members.values.foreach(checkForLocalFiles)
      case WdlValues.V_Object(members) =>
        members.values.foreach(checkForLocalFiles)
      case other =>
        throw new Exception(s"unhandled case ${other}")
    }
  }

  // A trivial expression has no operators, it is either (1) a
  // constant, (2) a single identifier, or (3) an access to a call
  // field.
  //
  // For example, `5`, `['a', 'b', 'c']`, and `true` are trivial.
  // 'x + y'  is not.
  def isTrivialExpression(expr: TAT.Expr): Boolean = {
    expr match {
      case expr if TUtils.isPrimitiveValue(expr) => true
      case _: TAT.ExprIdentifier                 => true

      // A collection of constants
      case TAT.ExprPair(l, r, _, _)   => Vector(l, r).forall(TUtils.isPrimitiveValue)
      case TAT.ExprArray(value, _, _) => value.forall(TUtils.isPrimitiveValue)
      case TAT.ExprMap(value, _, _) =>
        value.forall {
          case (k, v) => TUtils.isPrimitiveValue(k) && TUtils.isPrimitiveValue(v)
        }
      case TAT.ExprObject(value, _, _) => value.values.forall(TUtils.isPrimitiveValue)

      // Access a field in a call or a struct
      //   Int z = eliminateDuplicate.fields
      case TAT.ExprGetName(_: TAT.ExprIdentifier, _, _, _) => true

      case _ => false
    }
  }

  // Check if the WDL expression is a constant. If so, calculate and return it.
  // Otherwise, return None.
  //
  def ifConstEval(wdlType: WdlTypes.T, expr: TAT.Expr): Option[WdlValues.V] = {
    try {
      val value = evalConst(expr)
      val valueWithCorrectType = Coercion.coerceTo(wdlType, value, expr.loc)
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
