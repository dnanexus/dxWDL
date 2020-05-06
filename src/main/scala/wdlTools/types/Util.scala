package wdlTools.types

import java.net.URL

import wdlTools.syntax.TextSource
import wdlTools.util.Options
import wdlTools.util.TypeCheckingRegime._
import WdlTypes._

// This is the WDL typesystem
case class Util(conf: Options) {
  // Type checking rules, are we lenient or strict in checking coercions.
  private val regime = conf.typeChecking

  // check if the right hand side of an assignment matches the left hand side
  //
  // Negative examples:
  //    Int i = "hello"
  //    Array[File] files = "8"
  //
  // Positive examples:
  //    Int k =  3 + 9
  //    Int j = k * 3
  //    String s = "Ford model T"
  //    String s2 = 5
  def isPrimitive(t: WT): Boolean = {
    t match {
      case WT_String | WT_File | WT_Boolean | WT_Int | WT_Float => true
      case _                                                    => false
    }
  }

  private def isCoercibleTo2(left: WT, right: WT): Boolean = {
    //System.out.println(s"isCoercibleTo2 ${left} ${right} regime=${regime}")
    (left, right) match {
      case (WT_Boolean, WT_Boolean)                                       => true
      case (WT_Int, WT_Int)                                               => true
      case (WT_Int, WT_Int | WT_Float | WT_String) if regime == Lenient   => true
      case (WT_Float, WT_Int | WT_Float)                                  => true
      case (WT_Float, WT_Int | WT_Float | WT_String) if regime == Lenient => true

      // only the file -> string conversion is documented in spec 1.0
      case (WT_String, WT_Boolean | WT_Int | WT_Float | WT_String | WT_File) => true

      case (WT_File, WT_String | WT_File) => true

      case (WT_Optional(l), WT_Optional(r)) => isCoercibleTo2(l, r)

      // T is coercible to T?
      case (WT_Optional(l), r) if regime <= Moderate => isCoercibleTo2(l, r)

      case (WT_Array(l), WT_Array(r))         => isCoercibleTo2(l, r)
      case (WT_Map(kl, vl), WT_Map(kr, vr))   => isCoercibleTo2(kl, kr) && isCoercibleTo2(vl, vr)
      case (WT_Pair(l1, l2), WT_Pair(r1, r2)) => isCoercibleTo2(l1, r1) && isCoercibleTo2(l2, r2)

      // structures are equivalent iff they have the same name
      //
      case (WT_Struct(sname1, _), WT_Struct(sname2, _)) =>
        sname1 == sname2

      // coercions to objects and structs can fail at runtime. We
      // are not thoroughly checking them here.
      case (WT_Object, WT_Object)    => true
      case (_: WT_Struct, WT_Object) => true
      case (_: WT_Struct, _: WT_Map) => true

      case (WT_Var(i), WT_Var(j)) if i == j => true

      case (_, WT_Any) => true
      case _           => false
    }
  }

  def isCoercibleTo(left: WT, right: WT): Boolean = {
    if (left.isInstanceOf[WT_Identifier])
      throw new Exception(s"${left} should not appear here")
    if (right.isInstanceOf[WT_Identifier])
      throw new Exception(s"${right} should not appear here")

    (left, right) match {
      // List of special cases goes here

      // a type T can be coerced to a T?
      // I don't think this is such a great idea.
      case (WT_Optional(l), r) if l == r => true

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
  def unify(x: WT, y: WT, ctx: TypeUnificationContext): (WT, TypeUnificationContext) = {
    (x, y) match {
      // base case, primitive types
      case (_, _) if isPrimitive(x) && isPrimitive(y) && isCoercibleTo(x, y) =>
        (x, ctx)
      case (WT_Any, WT_Any) => (WT_Any, ctx)
      case (x, WT_Any)      => (x, ctx)
      case (WT_Any, x)      => (x, ctx)

      case (WT_Object, WT_Object)    => (WT_Object, ctx)
      case (WT_Object, _: WT_Struct) => (WT_Object, ctx)

      case (WT_Optional(l), WT_Optional(r)) =>
        val (t, ctx2) = unify(l, r, ctx)
        (WT_Optional(t), ctx2)

      // These two cases are really questionable to me. We are allowing an X to
      // become an X?
      case (WT_Optional(l), r) =>
        val (t, ctx2) = unify(l, r, ctx)
        (WT_Optional(t), ctx2)
      case (l, WT_Optional(r)) =>
        val (t, ctx2) = unify(l, r, ctx)
        (WT_Optional(t), ctx2)

      case (WT_Array(l), WT_Array(r)) =>
        val (t, ctx2) = unify(l, r, ctx)
        (WT_Array(t), ctx2)
      case (WT_Map(k1, v1), WT_Map(k2, v2)) =>
        val (kt, ctx2) = unify(k1, k2, ctx)
        val (vt, ctx3) = unify(v1, v2, ctx2)
        (WT_Map(kt, vt), ctx3)
      case (WT_Pair(l1, r1), WT_Pair(l2, r2)) =>
        val (lt, ctx2) = unify(l1, l2, ctx)
        val (rt, ctx3) = unify(r1, r2, ctx2)
        (WT_Pair(lt, rt), ctx3)
      case (WT_Identifier(l), WT_Identifier(r)) if l == r =>
        // a user defined type
        (WT_Identifier(l), ctx)
      case (WT_Var(i), WT_Var(j)) if i == j =>
        (WT_Var(i), ctx)

      case (a: WT_Var, b: WT_Var) =>
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

      case (a: WT_Var, z) =>
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
  //    x = [ WT_Array(WT_Var(0)), WT_Array(WT_Var(1)) ]
  //    y = [ WT_Array(WT_Int),  WT_Array(WT_String) ]
  //
  // The result is:
  //    WT_Var(0) -> WT_Int
  //    WT_Var(1) -> WT_String
  //
  def unifyFunctionArguments(x: Vector[WT],
                             y: Vector[WT],
                             ctx: TypeUnificationContext): (Vector[WT], TypeUnificationContext) = {
    (x zip y).foldLeft((Vector.empty[WT], ctx)) {
      case ((tVec, ctx), (lt, rt)) =>
        val (t, ctx2) = unify(lt, rt, ctx)
        (tVec :+ t, ctx2)
    }
  }

  // Unify elements in a collection. For example, a vector of values.
  def unifyCollection(tVec: Iterable[WT],
                      ctx: TypeUnificationContext): (WT, TypeUnificationContext) = {
    assert(tVec.nonEmpty)
    tVec.tail.foldLeft((tVec.head, ctx)) {
      case ((t, ctx), t2) =>
        unify(t, t2, ctx)
    }
  }

  // substitute the type variables for the values in type 't'
  def substitute(t: WT,
                 typeBindings: Map[WT_Var, WT],
                 srcText: TextSource,
                 docSourceUrl: Option[URL] = None): WT = {
    def sub(t: WT): WT = {
      t match {
        case WT_String | WT_File | WT_Boolean | WT_Int | WT_Float => t
        case a: WT_Var if !(typeBindings contains a) =>
          throw new TypeException(s"type variable ${toString(a)} does not have a binding",
                                  srcText,
                                  docSourceUrl)
        case a: WT_Var       => typeBindings(a)
        case x: WT_Struct    => x
        case WT_Pair(l, r)   => WT_Pair(sub(l), sub(r))
        case WT_Array(t)     => WT_Array(sub(t))
        case WT_Map(k, v)    => WT_Map(sub(k), sub(v))
        case WT_Object       => WT_Object
        case WT_Optional(t1) => WT_Optional(sub(t1))
        case WT_Any          => WT_Any
        case other =>
          throw new TypeException(s"Type ${toString(other)} should not appear in this context",
                                  srcText,
                                  docSourceUrl)
      }
    }
    sub(t)
  }

  def toString(t: WT): String = {
    t match {
      case WT_Boolean        => "Boolean"
      case WT_Int            => "Int"
      case WT_Float          => "Float"
      case WT_String         => "String"
      case WT_File           => "File"
      case WT_Any            => "Any"
      case WT_Var(i)         => s"Var($i)"
      case WT_Identifier(id) => s"Id(${id})"
      case WT_Pair(l, r)     => s"Pair[${toString(l)}, ${toString(r)}]"
      case WT_Array(t)       => s"Array[${toString(t)}]"
      case WT_Map(k, v)      => s"Map[${toString(k)}, ${toString(v)}]"
      case WT_Object         => "Object"
      case WT_Optional(t)    => s"Optional[${toString(t)}]"

      // a user defined structure
      case WT_Struct(name, _) => s"Struct($name)"

      case WT_Task(name, input, output) =>
        val inputs = input
          .map {
            case (name, (t, _)) =>
              s"$name -> ${toString(t)}"
          }
          .mkString(", ")
        val outputs = output
          .map {
            case (name, t) =>
              s"$name -> ${toString(t)}"
          }
          .mkString(", ")
        s"TaskSig($name, input=$inputs, outputs=${outputs})"

      case WT_Workflow(name, input, output) =>
        val inputs = input
          .map {
            case (name, (t, _)) =>
              s"$name -> ${toString(t)}"
          }
          .mkString(", ")
        val outputs = output
          .map {
            case (name, t) =>
              s"$name -> ${toString(t)}"
          }
          .mkString(", ")
        s"WorkflowSig($name, input={$inputs}, outputs={$outputs})"

      // The type of a call to a task or a workflow.
      case WT_Call(name, output: Map[String, WT]) =>
        val outputs = output
          .map {
            case (name, t) =>
              s"$name -> ${toString(t)}"
          }
          .mkString(", ")
        s"Call $name { $outputs }"

      // WT representation for an stdlib function.
      // For example, stdout()
      case WT_Function0(name, output) =>
        s"${name}() -> ${toString(output)}"

      // A function with one argument
      case WT_Function1(name, input, output) =>
        s"${name}(${toString(input)}) -> ${toString(output)}"

      // A function with two arguments. For example:
      // Float size(File, [String])
      case WT_Function2(name, arg1, arg2, output) =>
        s"${name}(${toString(arg1)}, ${toString(arg2)}) -> ${toString(output)}"

      // A function with three arguments. For example:
      // String sub(String, String, String)
      case WT_Function3(name, arg1, arg2, arg3, output) =>
        s"${name}(${toString(arg1)}, ${toString(arg2)}, ${toString(arg3)}) -> ${toString(output)}"
    }
  }
}
