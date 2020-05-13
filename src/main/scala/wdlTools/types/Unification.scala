package wdlTools.types

import java.net.URL

import wdlTools.syntax.TextSource
import wdlTools.types.Util.{typeToString, isPrimitive}
import wdlTools.types.WdlTypes._
import wdlTools.util.Options
import wdlTools.util.TypeCheckingRegime._

case class Unification(conf: Options) {
  // Type checking rules. Are we lenient or strict in checking coercions?
  private val regime = conf.typeChecking

  // A value for each type variable.
  //
  // This is used when we have polymorphic types,
  // such as when calling standard library functions. We need to keep
  // track of the latest value for each type variable.
  type TypeUnificationContext = Map[T_Var, T]

  private def isCoercibleTo2(left: T, right: T): Boolean = {
    //System.out.println(s"isCoercibleTo2 ${left} ${right} regime=${regime}")
    (left, right) match {
      case (T_Boolean, T_Boolean)                                     => true
      case (T_Int, T_Int)                                             => true
      case (T_Int, T_Int | T_Float | T_String) if regime == Lenient   => true
      case (T_Float, T_Int | T_Float)                                 => true
      case (T_Float, T_Int | T_Float | T_String) if regime == Lenient => true

      // only the file -> string conversion is documented in spec 1.0
      case (T_String, T_Boolean | T_Int | T_Float | T_String | T_File) => true

      case (T_File, T_String | T_File) => true

      case (T_Optional(l), T_Optional(r)) => isCoercibleTo2(l, r)

      // T is coercible to T?
      case (T_Optional(l), r) if regime <= Moderate => isCoercibleTo2(l, r)

      case (T_Array(l, _), T_Array(r, _))   => isCoercibleTo2(l, r)
      case (T_Map(kl, vl), T_Map(kr, vr))   => isCoercibleTo2(kl, kr) && isCoercibleTo2(vl, vr)
      case (T_Pair(l1, l2), T_Pair(r1, r2)) => isCoercibleTo2(l1, r1) && isCoercibleTo2(l2, r2)

      // structures are equivalent iff they have the same name
      //
      case (T_Struct(sname1, _), T_Struct(sname2, _)) =>
        sname1 == sname2

      // coercions to objects and structs can fail at runtime. We
      // are not thoroughly checking them here.
      case (T_Object, T_Object)    => true
      case (_: T_Struct, T_Object) => true
      case (_: T_Struct, _: T_Map) => true

      case (T_Var(i), T_Var(j)) if i == j => true

      case (_, T_Any) => true
      case _          => false
    }
  }

  def isCoercibleTo(left: T, right: T): Boolean = {
    if (left.isInstanceOf[T_Identifier])
      throw new Exception(s"${left} should not appear here")
    if (right.isInstanceOf[T_Identifier])
      throw new Exception(s"${right} should not appear here")

    (left, right) match {
      // List of special cases goes here

      // a type T can be coerced to a T?
      // I don't think this is such a great idea.
      case (T_Optional(l), r) if l == r => true

      // normal cases
      case (_, _) =>
        isCoercibleTo2(left, right)
    }
  }

  // The least type that [x] and [y] are coercible to.
  // For example:
  //    [Int?, Int]  -> Int?
  //
  // But we don't want to have:
  //    Array[String] s = ["a", 1, 3.1]
  // even if that makes sense, we don't want to have:
  //    Array[Array[String]] = [[1], ["2"], [1.1]]
  //
  //
  // when calling a polymorphic function things get complicated.
  // For example:
  //    select_first([null, 6])
  // The signature for select first is:
  //    Array[X?] -> X
  // we need to figure out that X is Int.
  //
  //
  def unify(x: T, y: T, ctx: TypeUnificationContext): (T, TypeUnificationContext) = {
    (x, y) match {
      // base case, primitive types
      case (_, _) if isPrimitive(x) && isPrimitive(y) && isCoercibleTo(x, y) =>
        (x, ctx)
      case (T_Any, T_Any) => (T_Any, ctx)
      case (x, T_Any)     => (x, ctx)
      case (T_Any, x)     => (x, ctx)

      case (T_Object, T_Object)    => (T_Object, ctx)
      case (T_Object, _: T_Struct) => (T_Object, ctx)

      case (T_Optional(l), T_Optional(r)) =>
        val (t, ctx2) = unify(l, r, ctx)
        (T_Optional(t), ctx2)

      // These two cases are really questionable to me. We are allowing an X to
      // become an X?
      case (T_Optional(l), r) =>
        val (t, ctx2) = unify(l, r, ctx)
        (T_Optional(t), ctx2)
      case (l, T_Optional(r)) =>
        val (t, ctx2) = unify(l, r, ctx)
        (T_Optional(t), ctx2)

      case (T_Array(l, _), T_Array(r, _)) =>
        val (t, ctx2) = unify(l, r, ctx)
        (T_Array(t), ctx2)
      case (T_Map(k1, v1), T_Map(k2, v2)) =>
        val (kt, ctx2) = unify(k1, k2, ctx)
        val (vt, ctx3) = unify(v1, v2, ctx2)
        (T_Map(kt, vt), ctx3)
      case (T_Pair(l1, r1), T_Pair(l2, r2)) =>
        val (lt, ctx2) = unify(l1, l2, ctx)
        val (rt, ctx3) = unify(r1, r2, ctx2)
        (T_Pair(lt, rt), ctx3)
      case (T_Identifier(l), T_Identifier(r)) if l == r =>
        // a user defined type
        (T_Identifier(l), ctx)
      case (T_Var(i), T_Var(j)) if i == j =>
        (T_Var(i), ctx)

      case (a: T_Var, b: T_Var) =>
        // found a type equality between two variables
        val ctx3: TypeUnificationContext = (ctx.get(a), ctx.get(b)) match {
          case (None, None) =>
            ctx + (a -> b)
          case (None, Some(z)) =>
            ctx + (a -> z)
          case (Some(z), None) =>
            ctx + (b -> z)
          case (Some(z), Some(w)) =>
            val (_, ctx2) = unify(z, w, ctx)
            ctx2
        }
        (ctx3(a), ctx3)

      case (a: T_Var, z) =>
        if (!(ctx contains a)) {
          // found a binding for a type variable
          (z, ctx + (a -> z))
        } else {
          // a binding already exists, choose the more general
          // type
          val w = ctx(a)
          unify(w, z, ctx)
        }

      case _ =>
        throw new TypeUnificationException(s"Types $x and $y do not match")
    }
  }

  // Unify a set of type pairs, and return a solution for the type
  // variables. If the types cannot be unified throw a TypeUnification exception.
  //
  // For example the signature for zip is:
  //    Array[Pair(X,Y)] zip(Array[X], Array[Y])
  // In order to type check a declaration like:
  //    Array[Pair[Int, String]] x  = zip([1, 2, 3], ["a", "b", "c"])
  // we solve for the X and Y type variables on the right hand
  // side. This should yield: { X : Int, Y : String }
  //
  // The inputs in this example are:
  //    x = [ T_Array(T_Var(0)), T_Array(T_Var(1)) ]
  //    y = [ T_Array(T_Int),  T_Array(T_String) ]
  //
  // The result is:
  //    T_Var(0) -> T_Int
  //    T_Var(1) -> T_String
  //
  def unifyFunctionArguments(x: Vector[T],
                             y: Vector[T],
                             ctx: TypeUnificationContext): (Vector[T], TypeUnificationContext) = {
    (x zip y).foldLeft((Vector.empty[T], ctx)) {
      case ((tVec, ctx), (lt, rt)) =>
        val (t, ctx2) = unify(lt, rt, ctx)
        (tVec :+ t, ctx2)
    }
  }

  // Unify elements in a collection. For example, a vector of values.
  def unifyCollection(tVec: Iterable[T],
                      ctx: TypeUnificationContext): (T, TypeUnificationContext) = {
    assert(tVec.nonEmpty)
    tVec.tail.foldLeft((tVec.head, ctx)) {
      case ((t, ctx), t2) =>
        unify(t, t2, ctx)
    }
  }

  // substitute the type variables for the values in type 't'
  def substitute(t: T,
                 typeBindings: Map[T_Var, T],
                 srcText: TextSource,
                 docSourceUrl: Option[URL] = None): T = {
    def sub(t: T): T = {
      t match {
        case T_String | T_File | T_Boolean | T_Int | T_Float => t
        case a: T_Var if !(typeBindings contains a) =>
          throw new TypeException(s"type variable ${typeToString(a)} does not have a binding",
                                  srcText,
                                  docSourceUrl)
        case a: T_Var       => typeBindings(a)
        case x: T_Struct    => x
        case T_Pair(l, r)   => T_Pair(sub(l), sub(r))
        case T_Array(t, _)  => T_Array(sub(t))
        case T_Map(k, v)    => T_Map(sub(k), sub(v))
        case T_Object       => T_Object
        case T_Optional(t1) => T_Optional(sub(t1))
        case T_Any          => T_Any
        case other =>
          throw new TypeException(s"Type ${typeToString(other)} should not appear in this context",
                                  srcText,
                                  docSourceUrl)
      }
    }
    sub(t)
  }
}
