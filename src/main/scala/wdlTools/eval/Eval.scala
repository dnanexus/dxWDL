package wdlTools.eval

import java.net.URL

import wdlTools.eval.WdlValues._
import wdlTools.syntax.{TextSource, WdlVersion}
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.util.{EvalConfig, Options}

case class Eval(opts: Options,
                evalCfg: EvalConfig,
                wdlVersion: WdlVersion,
                docSourceUrl: Option[URL]) {
  // choose the standard library implementation based on version
  private val stdlib = wdlVersion match {
    case WdlVersion.Draft_2 => StdlibDraft2(opts, evalCfg, docSourceUrl)
    case WdlVersion.V1      => StdlibV1(opts, evalCfg, docSourceUrl)
    case WdlVersion.V2      => throw new Exception("WDL V2 is not yet supported")
  }
  private val coercion = Coercion(docSourceUrl)

  private def getStringVal(value: WV, text: TextSource): String = {
    value match {
      case WV_String(s) => s
      case WV_File(s)   => s
      case other        => throw new EvalException(s"bad value ${other}", text, docSourceUrl)
    }
  }

  private def compareEqeq(a: WV, b: WV, text: TextSource): Boolean = {
    (a, b) match {
      case (WV_Null, WV_Null)               => true
      case (WV_Boolean(b1), WV_Boolean(b2)) => b1 == b2
      case (WV_Int(i1), WV_Int(i2))         => i1 == i2
      case (WV_Float(x1), WV_Float(x2))     => x1 == x2
      case (WV_String(s1), WV_String(s2))   => s1 == s2
      case (WV_File(p1), WV_File(p2))       => p1 == p2

      case (WV_Pair(l1, r1), WV_Pair(l2, r2)) =>
        compareEqeq(l1, l2, text) && compareEqeq(r1, r2, text)

      // arrays
      case (WV_Array(a1), WV_Array(a2)) if a1.size != a2.size => false
      case (WV_Array(a1), WV_Array(a2))                       =>
        // All array elements must be equal
        (a1 zip a2).forall {
          case (x, y) => compareEqeq(x, y, text)
        }

      // maps
      case (WV_Map(m1), WV_Map(m2)) if m1.size != m2.size => false
      case (WV_Map(m1), WV_Map(m2)) =>
        val keysEqual = (m1.keys.toSet zip m2.keys.toSet).forall {
          case (k1, k2) => compareEqeq(k1, k2, text)
        }
        if (!keysEqual) {
          false
        } else {
          // now we know the keys are all equal
          m1.keys.forall(k => compareEqeq(m1(k), m2(k), text))
        }

      // optionals
      case (WV_Optional(v1), WV_Optional(v2)) =>
        compareEqeq(v1, v2, text)
      case (WV_Optional(v1), v2) =>
        compareEqeq(v1, v2, text)
      case (v1, WV_Optional(v2)) =>
        compareEqeq(v1, v2, text)

      // structs
      case (WV_Struct(name1, _), WV_Struct(name2, _)) if name1 != name2 => false
      case (WV_Struct(name, members1), WV_Struct(_, members2))
          if members1.keys.toSet != members2.keys.toSet =>
        // We need the type definition here. The other option is to assume it has already
        // been cleared at compile time.
        throw new Exception(s"error: struct ${name} does not have the corrent number of members")
      case (WV_Struct(_, members1), WV_Struct(_, members2)) =>
        members1.keys.forall(k => compareEqeq(members1(k), members2(k), text))

      case (_: WV_Object, _: WV_Object) =>
        throw new Exception("objects not implemented")
    }
  }

  private def compareLt(a: WV, b: WV, text: TextSource): Boolean = {
    (a, b) match {
      case (WV_Null, WV_Null)             => false
      case (WV_Int(n1), WV_Int(n2))       => n1 < n2
      case (WV_Float(x1), WV_Int(n2))     => x1 < n2
      case (WV_Int(n1), WV_Float(x2))     => n1 < x2
      case (WV_Float(x1), WV_Float(x2))   => x1 < x2
      case (WV_String(s1), WV_String(s2)) => s1 < s2
      case (WV_File(p1), WV_File(p2))     => p1 < p2
      case (_, _) =>
        throw new EvalException("bad value should be a boolean", text, docSourceUrl)
    }
  }

  private def compareLte(a: WV, b: WV, text: TextSource): Boolean = {
    (a, b) match {
      case (WV_Null, WV_Null)             => true
      case (WV_Int(n1), WV_Int(n2))       => n1 <= n2
      case (WV_Float(x1), WV_Int(n2))     => x1 <= n2
      case (WV_Int(n1), WV_Float(x2))     => n1 <= x2
      case (WV_Float(x1), WV_Float(x2))   => x1 <= x2
      case (WV_String(s1), WV_String(s2)) => s1 <= s2
      case (WV_File(p1), WV_File(p2))     => p1 <= p2
      case (_, _) =>
        throw new EvalException("bad value should be a boolean", text, docSourceUrl)
    }
  }

  private def compareGt(a: WV, b: WV, text: TextSource): Boolean = {
    (a, b) match {
      case (WV_Null, WV_Null)             => false
      case (WV_Int(n1), WV_Int(n2))       => n1 > n2
      case (WV_Float(x), WV_Int(i))       => x > i
      case (WV_Int(i), WV_Float(x))       => i > x
      case (WV_Float(x1), WV_Float(x2))   => x1 > x2
      case (WV_String(s1), WV_String(s2)) => s1 > s2
      case (WV_File(p1), WV_File(p2))     => p1 > p2
      case (_, _) =>
        throw new EvalException("bad value should be a boolean", text, docSourceUrl)
    }
  }

  private def compareGte(a: WV, b: WV, text: TextSource): Boolean = {
    (a, b) match {
      case (WV_Null, WV_Null)             => true
      case (WV_Int(n1), WV_Int(n2))       => n1 >= n2
      case (WV_Float(x), WV_Int(i))       => x >= i
      case (WV_Int(i), WV_Float(x))       => i >= x
      case (WV_Float(x1), WV_Float(x2))   => x1 >= x2
      case (WV_String(s1), WV_String(s2)) => s1 >= s2
      case (WV_File(p1), WV_File(p2))     => p1 >= p2
      case (_, _) =>
        throw new EvalException("bad value should be a boolean", text, docSourceUrl)
    }
  }

  private def add(a: WV, b: WV, text: TextSource): WV = {
    (a, b) match {
      case (WV_Int(n1), WV_Int(n2))     => WV_Int(n1 + n2)
      case (WV_Float(x1), WV_Int(n2))   => WV_Float(x1 + n2)
      case (WV_Int(n1), WV_Float(x2))   => WV_Float(n1 + x2)
      case (WV_Float(x1), WV_Float(x2)) => WV_Float(x1 + x2)

      // if we are adding strings, the result is a string
      case (WV_String(s1), WV_String(s2)) => WV_String(s1 + s2)
      case (WV_String(s1), WV_Int(n2))    => WV_String(s1 + n2.toString)
      case (WV_String(s1), WV_Float(x2))  => WV_String(s1 + x2.toString)
      case (WV_Int(n1), WV_String(s2))    => WV_String(n1.toString + s2)
      case (WV_Float(x1), WV_String(s2))  => WV_String(x1.toString + s2)
      case (WV_String(s), WV_Null)        => WV_String(s)

      // files
      case (WV_File(s1), WV_String(s2)) => WV_File(s1 + s2)
      case (WV_File(s1), WV_File(s2))   => WV_File(s1 + s2)

      case (_, _) =>
        throw new EvalException("cannot add these values", text, docSourceUrl)
    }
  }

  private def sub(a: WV, b: WV, text: TextSource): WV = {
    (a, b) match {
      case (WV_Int(n1), WV_Int(n2))     => WV_Int(n1 - n2)
      case (WV_Float(x1), WV_Int(n2))   => WV_Float(x1 - n2)
      case (WV_Int(n1), WV_Float(x2))   => WV_Float(n1 - x2)
      case (WV_Float(x1), WV_Float(x2)) => WV_Float(x1 - x2)
      case (_, _) =>
        throw new EvalException(s"Expressions must be integers or floats", text, docSourceUrl)
    }
  }

  private def mod(a: WV, b: WV, text: TextSource): WV = {
    (a, b) match {
      case (WV_Int(n1), WV_Int(n2))     => WV_Int(n1 % n2)
      case (WV_Float(x1), WV_Int(n2))   => WV_Float(x1 % n2)
      case (WV_Int(n1), WV_Float(x2))   => WV_Float(n1 % x2)
      case (WV_Float(x1), WV_Float(x2)) => WV_Float(x1 % x2)
      case (_, _) =>
        throw new EvalException(s"Expressions must be integers or floats", text, docSourceUrl)
    }
  }

  private def multiply(a: WV, b: WV, text: TextSource): WV = {
    (a, b) match {
      case (WV_Int(n1), WV_Int(n2))     => WV_Int(n1 * n2)
      case (WV_Float(x1), WV_Int(n2))   => WV_Float(x1 * n2)
      case (WV_Int(n1), WV_Float(x2))   => WV_Float(n1 * x2)
      case (WV_Float(x1), WV_Float(x2)) => WV_Float(x1 * x2)
      case (_, _) =>
        throw new EvalException(s"Expressions must be integers or floats", text, docSourceUrl)
    }
  }

  private def divide(a: WV, b: WV, text: TextSource): WV = {
    (a, b) match {
      case (WV_Int(n1), WV_Int(n2)) =>
        if (n2 == 0)
          throw new EvalException("DivisionByZero", text, docSourceUrl)
        WV_Int(n1 / n2)
      case (WV_Float(x1), WV_Int(n2)) =>
        if (n2 == 0)
          throw new EvalException("DivisionByZero", text, docSourceUrl)
        WV_Float(x1 / n2)
      case (WV_Int(n1), WV_Float(x2)) =>
        if (x2 == 0)
          throw new EvalException("DivisionByZero", text, docSourceUrl)
        WV_Float(n1 / x2)
      case (WV_Float(x1), WV_Float(x2)) =>
        if (x2 == 0)
          throw new EvalException("DivisionByZero", text, docSourceUrl)
        WV_Float(x1 / x2)
      case (_, _) =>
        throw new EvalException(s"Expressions must be integers or floats", text, docSourceUrl)
    }
  }

  // Access a field in a struct or an object. For example:
  //   Int z = x.a
  private def exprGetName(value: WV, id: String, ctx: Context, text: TextSource): WV = {
    value match {
      case WV_Struct(name, members) =>
        members.get(id) match {
          case None =>
            throw new EvalException(s"Struct ${name} does not have member ${id}",
                                    text,
                                    docSourceUrl)
          case Some(t) => t
        }

      case WV_Object(members) =>
        members.get(id) match {
          case None =>
            throw new EvalException(s"Object does not have member ${id}", text, docSourceUrl)
          case Some(t) => t
        }

      case WV_Call(name, members) =>
        members.get(id) match {
          case None =>
            throw new EvalException(s"Call object ${name} does not have member ${id}",
                                    text,
                                    docSourceUrl)
          case Some(t) => t
        }

      // accessing a pair element
      case WV_Pair(l, _) if id.toLowerCase() == "left"  => l
      case WV_Pair(_, r) if id.toLowerCase() == "right" => r
      case WV_Pair(_, _) =>
        throw new EvalException(s"accessing a pair with (${id}) is illegal", text, docSourceUrl)

      case _ =>
        throw new EvalException(s"member access (${id}) in expression is illegal",
                                text,
                                docSourceUrl)
    }
  }

  private def apply(expr: TAT.Expr, ctx: Context): WdlValues.WV = {
    expr match {
      case _: TAT.ValueNull    => WV_Null
      case x: TAT.ValueBoolean => WV_Boolean(x.value)
      case x: TAT.ValueInt     => WV_Int(x.value)
      case x: TAT.ValueFloat   => WV_Float(x.value)
      case x: TAT.ValueString  => WV_String(x.value)
      case x: TAT.ValueFile    => WV_File(x.value)

      // accessing a variable
      case eid: TAT.ExprIdentifier if !(ctx.bindings contains eid.id) =>
        throw new EvalException(s"accessing undefined variable ${eid.id}")
      case eid: TAT.ExprIdentifier =>
        ctx.bindings(eid.id)

      // concatenate an array of strings inside a command block
      case ecs: TAT.ExprCompoundString =>
        val strArray: Vector[String] = ecs.value.map { x =>
          val xv = apply(x, ctx)
          getStringVal(xv, x.text)
        }
        WV_String(strArray.mkString(""))

      case ep: TAT.ExprPair => WV_Pair(apply(ep.l, ctx), apply(ep.r, ctx))
      case ea: TAT.ExprArray =>
        WV_Array(ea.value.map { x =>
          apply(x, ctx)
        })
      case em: TAT.ExprMap =>
        WV_Map(em.value.map {
          case (k, v) => apply(k, ctx) -> apply(v, ctx)
        })

      case eObj: TAT.ExprObject =>
        WV_Object(eObj.value.map {
          case (k, v) => k -> apply(v, ctx)
        }.toMap)

      // ~{true="--yes" false="--no" boolean_value}
      case TAT.ExprPlaceholderEqual(t, f, boolExpr, _, _) =>
        apply(boolExpr, ctx) match {
          case WV_Boolean(true)  => apply(t, ctx)
          case WV_Boolean(false) => apply(f, ctx)
          case other =>
            throw new EvalException(s"bad value ${other}, should be a boolean",
                                    expr.text,
                                    docSourceUrl)
        }

      // ~{default="foo" optional_value}
      case TAT.ExprPlaceholderDefault(defaultVal, optVal, _, _) =>
        apply(optVal, ctx) match {
          case WV_Null => apply(defaultVal, ctx)
          case other   => other
        }

      // ~{sep=", " array_value}
      case TAT.ExprPlaceholderSep(sep: TAT.Expr, arrayVal: TAT.Expr, _, _) =>
        val sep2 = getStringVal(apply(sep, ctx), sep.text)
        apply(arrayVal, ctx) match {
          case WV_Array(ar) =>
            val elements: Vector[String] = ar.map { x =>
              getStringVal(x, expr.text)
            }
            WV_String(elements.mkString(sep2))
          case other =>
            throw new EvalException(s"bad value ${other}, should be a string",
                                    expr.text,
                                    docSourceUrl)
        }

      // operators on one argument
      case e: TAT.ExprUniraryPlus =>
        apply(e.value, ctx) match {
          case WV_Float(f) => WV_Float(f)
          case WV_Int(k)   => WV_Int(k)
          case other =>
            throw new EvalException(s"bad value ${other}, should be a number",
                                    expr.text,
                                    docSourceUrl)
        }

      case e: TAT.ExprUniraryMinus =>
        apply(e.value, ctx) match {
          case WV_Float(f) => WV_Float(-1 * f)
          case WV_Int(k)   => WV_Int(-1 * k)
          case other =>
            throw new EvalException(s"bad value ${other}, should be a number",
                                    expr.text,
                                    docSourceUrl)
        }

      case e: TAT.ExprNegate =>
        apply(e.value, ctx) match {
          case WV_Boolean(b) => WV_Boolean(!b)
          case other =>
            throw new EvalException(s"bad value ${other}, should be a boolean",
                                    expr.text,
                                    docSourceUrl)
        }

      // operators on two arguments
      case TAT.ExprLor(a, b, _, _) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        (av, bv) match {
          case (WV_Boolean(a1), WV_Boolean(b1)) =>
            WV_Boolean(a1 || b1)
          case (WV_Boolean(_), other) =>
            throw new EvalException(s"bad value ${other}, should be a boolean",
                                    b.text,
                                    docSourceUrl)
          case (other, _) =>
            throw new EvalException(s"bad value ${other}, should be a boolean",
                                    a.text,
                                    docSourceUrl)
        }

      case TAT.ExprLand(a, b, _, _) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        (av, bv) match {
          case (WV_Boolean(a1), WV_Boolean(b1)) =>
            WV_Boolean(a1 && b1)
          case (WV_Boolean(_), other) =>
            throw new EvalException(s"bad value ${other}, should be a boolean",
                                    b.text,
                                    docSourceUrl)
          case (other, _) =>
            throw new EvalException(s"bad value ${other}, should be a boolean",
                                    a.text,
                                    docSourceUrl)
        }

      // recursive comparison
      case TAT.ExprEqeq(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        WV_Boolean(compareEqeq(av, bv, text))
      case TAT.ExprNeq(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        WV_Boolean(!compareEqeq(av, bv, text))

      case TAT.ExprLt(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        WV_Boolean(compareLt(av, bv, text))
      case TAT.ExprLte(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        WV_Boolean(compareLte(av, bv, text))
      case TAT.ExprGt(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        WV_Boolean(compareGt(av, bv, text))
      case TAT.ExprGte(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        WV_Boolean(compareGte(av, bv, text))

      // Add is overloaded, can be used to add numbers or concatenate strings
      case TAT.ExprAdd(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        add(av, bv, text)

      // Math operations
      case TAT.ExprSub(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        sub(av, bv, text)
      case TAT.ExprMod(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        mod(av, bv, text)
      case TAT.ExprMul(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        multiply(av, bv, text)
      case TAT.ExprDivide(a, b, _, text) =>
        val av = apply(a, ctx)
        val bv = apply(b, ctx)
        divide(av, bv, text)

      // Access an array element at [index]
      case TAT.ExprAt(array, index, _, text) =>
        val array_v = apply(array, ctx)
        val index_v = apply(index, ctx)
        (array_v, index_v) match {
          case (WV_Array(av), WV_Int(n)) if n < av.size =>
            av(n)
          case (WV_Array(av), WV_Int(n)) =>
            val arraySize = av.size
            throw new EvalException(
                s"array access out of bounds (size=${arraySize}, element accessed=${n})",
                text,
                docSourceUrl
            )
          case (_, _) =>
            throw new EvalException(s"array access requires an array and an integer",
                                    text,
                                    docSourceUrl)
        }

      // conditional:
      // if (x == 1) then "Sunday" else "Weekday"
      case TAT.ExprIfThenElse(cond, tBranch, fBranch, _, text) =>
        val cond_v = apply(cond, ctx)
        cond_v match {
          case WV_Boolean(true)  => apply(tBranch, ctx)
          case WV_Boolean(false) => apply(fBranch, ctx)
          case _ =>
            throw new EvalException(s"condition is not boolean", text, docSourceUrl)
        }

      // Apply a standard library function to arguments. For example:
      //   read_int("4")
      case TAT.ExprApply(funcName, elements, _, text) =>
        val funcArgs = elements.map(e => apply(e, ctx))
        stdlib.call(funcName, funcArgs, text)

      // Access a field in a struct or an object. For example:
      //   Int z = x.a
      case TAT.ExprGetName(e: TAT.Expr, fieldName, _, text) =>
        val ev = apply(e, ctx)
        exprGetName(ev, fieldName, ctx, text)

      case other =>
        throw new Exception(s"sanity: expression ${other} not implemented")
    }
  }

  // Evaluate all the declarations and return a context
  def applyDeclarations(decls: Vector[TAT.Declaration], ctx: Context): Context = {
    decls.foldLeft(ctx) {
      case (accu, TAT.Declaration(name, wdlType, Some(expr), text)) =>
        val value = apply(expr, accu)
        val value2 = coercion.coerceTo(wdlType, value, text)
        accu.addBinding(name, value2)
      case (_, ast) =>
        throw new Exception(s"Can not evaluate element ${ast.getClass}")
    }
  }

  // evaluate all the parts of a command section.
  //
  def applyCommand(command: TAT.CommandSection, ctx: Context): String = {
    command.parts
      .map { expr =>
        val value = apply(expr, ctx)
        val str = Serialize.primitiveValueToString(value, expr.text, docSourceUrl)
        str
      }
      .mkString("")
  }
}
