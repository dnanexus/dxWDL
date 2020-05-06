package wdlTools.types

import java.nio.file.Paths

import wdlTools.syntax.AbstractSyntax._
import wdlTools.syntax.Util.exprToString
import wdlTools.syntax.TextSource
import wdlTools.util.TypeCheckingRegime._
import wdlTools.util.{Util => UUtil}
import wdlTools.types.WdlTypes._
import wdlTools.types.{Util => TUtil}

case class TypeChecker(stdlib: Stdlib) {
  private val tUtil = TUtil(stdlib.conf)
  private val regime = stdlib.conf.typeChecking

  // A group of bindings. This is typically a part of the context. For example,
  // the body of a scatter.
  type Bindings = Map[String, WT]

  private def typeEvalMathOp(expr: Expr, ctx: Context): WT = {
    val t = typeEval(expr, ctx)
    t match {
      case _: TypeInt   => WT_Int
      case _: TypeFloat => WT_Float
      case _ =>
        throw new TypeException(s"${exprToString(expr)} must be an integer or a float",
                                expr.text,
                                ctx.docSourceUrl)
    }
  }

  // The add operation is overloaded.
  // 1) The result of adding two integers is an integer
  // 2)    -"-                   floats   -"-   float
  // 3)    -"-                   strings  -"-   string
  private def typeEvalAdd(a: Expr, b: Expr, ctx: Context): WT = {
    def isPseudoString(x: WT): Boolean = {
      x match {
        case WT_String              => true
        case WT_Optional(WT_String) => true
        case WT_File                => true
        case WT_Optional(WT_File)   => true
        case _                      => false
      }
    }
    val at = typeEval(a, ctx)
    val bt = typeEval(b, ctx)
    (at, bt) match {
      case (WT_Int, WT_Int)     => WT_Int
      case (WT_Float, WT_Int)   => WT_Float
      case (WT_Int, WT_Float)   => WT_Float
      case (WT_Float, WT_Float) => WT_Float

      // if we are adding strings, the result is a string
      case (WT_String, WT_String) => WT_String
      case (WT_String, WT_Int)    => WT_String
      case (WT_String, WT_Float)  => WT_String
      case (WT_Int, WT_String)    => WT_String
      case (WT_Float, WT_String)  => WT_String

      // NON STANDARD
      // there are WDL programs where we add optional strings
      case (WT_String, x) if isPseudoString(x)                     => WT_String
      case (WT_String, WT_Optional(WT_Int)) if regime == Lenient   => WT_String
      case (WT_String, WT_Optional(WT_Float)) if regime == Lenient => WT_String

      // adding files is equivalent to concatenating paths
      case (WT_File, WT_String | WT_File) => WT_File

      case (_, _) =>
        throw new TypeException(
            s"Expressions ${exprToString(a)} and ${exprToString(b)} cannot be added",
            a.text,
            ctx.docSourceUrl
        )
    }
  }

  private def typeEvalMathOp(a: Expr, b: Expr, ctx: Context): WT = {
    val at = typeEval(a, ctx)
    val bt = typeEval(b, ctx)
    (at, bt) match {
      case (WT_Int, WT_Int)     => WT_Int
      case (WT_Float, WT_Int)   => WT_Float
      case (WT_Int, WT_Float)   => WT_Float
      case (WT_Float, WT_Float) => WT_Float
      case (_, _) =>
        throw new TypeException(
            s"Expressions ${exprToString(a)} and ${exprToString(b)} must be integers or floats",
            a.text,
            ctx.docSourceUrl
        )
    }
  }

  private def typeEvalLogicalOp(expr: Expr, ctx: Context): WT = {
    val t = typeEval(expr, ctx)
    t match {
      case WT_Boolean => WT_Boolean
      case other =>
        throw new TypeException(
            s"${exprToString(expr)} must be a boolean, it is ${tUtil.toString(other)}",
            expr.text,
            ctx.docSourceUrl
        )
    }
  }

  private def typeEvalLogicalOp(a: Expr, b: Expr, ctx: Context): WT = {
    val at = typeEval(a, ctx)
    val bt = typeEval(b, ctx)
    (at, bt) match {
      case (WT_Boolean, WT_Boolean) => WT_Boolean
      case (_, _) =>
        throw new TypeException(s"${exprToString(a)} and ${exprToString(b)} must have boolean type",
                                a.text,
                                ctx.docSourceUrl)
    }
  }

  private def typeEvalCompareOp(a: Expr, b: Expr, ctx: Context): WT = {
    val at = typeEval(a, ctx)
    val bt = typeEval(b, ctx)
    if (at == bt) {
      // These could be complex types, such as Array[Array[Int]].
      return WT_Boolean
    }

    // Even if the types are not the same, there are cases where they can
    // be compared.
    (at, bt) match {
      case (WT_Int, WT_Float) => WT_Boolean
      case (WT_Float, WT_Int) => WT_Boolean
      case (_, _) =>
        throw new TypeException(
            s"Expressions ${exprToString(a)} and ${exprToString(b)} must have the same type",
            a.text,
            ctx.docSourceUrl
        )
    }
  }

  private def typeTranslate(t: Type, text: TextSource, ctx: Context): WT = {
    t match {
      case TypeOptional(t, _) => WT_Optional(typeTranslate(t, text, ctx))
      case TypeArray(t, _, _) => WT_Array(typeTranslate(t, text, ctx))
      case TypeMap(k, v, _)   => WT_Map(typeTranslate(k, text, ctx), typeTranslate(v, text, ctx))
      case TypePair(l, r, _)  => WT_Pair(typeTranslate(l, text, ctx), typeTranslate(r, text, ctx))
      case _: TypeString      => WT_String
      case _: TypeFile        => WT_File
      case _: TypeBoolean     => WT_Boolean
      case _: TypeInt         => WT_Int
      case _: TypeFloat       => WT_Float
      case TypeIdentifier(id, _) =>
        ctx.structs.get(id) match {
          case None =>
            throw new TypeException(s"struct ${id} has not been defined", text, ctx.docSourceUrl)
          case Some(struct) => struct
        }
      case _: TypeObject => WT_Object
      case TypeStruct(name, members, _) =>
        WT_Struct(name, members.map {
          case StructMember(name, t2, _) => name -> typeTranslate(t2, text, ctx)
        }.toMap)
    }
  }

  // Figure out what the type of an expression is.
  //
  private def typeEval(expr: Expr, ctx: Context): WT = {
    expr match {
      // base cases, primitive types
      case _: ValueString  => WT_String
      case _: ValueFile    => WT_File
      case _: ValueBoolean => WT_Boolean
      case _: ValueInt     => WT_Int
      case _: ValueFloat   => WT_Float

      // an identifier has to be bound to a known type
      case ExprIdentifier(id, _) =>
        ctx.declarations.get(id) match {
          case None =>
            throw new TypeException(s"Identifier ${id} is not defined", expr.text, ctx.docSourceUrl)
          case Some(t) => t
        }

      // All the sub-exressions have to be strings, or coercible to strings
      case ExprCompoundString(vec, _) =>
        vec foreach { subExpr =>
          val t = typeEval(subExpr, ctx)
          if (!tUtil.isCoercibleTo(WT_String, t))
            throw new TypeException(
                s"expression ${exprToString(expr)} of type ${t} is not coercible to string",
                expr.text,
                ctx.docSourceUrl
            )
        }
        WT_String

      case ExprPair(l, r, _)                => WT_Pair(typeEval(l, ctx), typeEval(r, ctx))
      case ExprArray(vec, _) if vec.isEmpty =>
        // The array is empty, we can't tell what the array type is.
        // TODO: replace the Any type with a type-parameter
        WT_Array(WT_Any)

      case ExprArray(vec, _) =>
        val vecTypes = vec.map(typeEval(_, ctx))
        val (t, _) =
          try {
            tUtil.unifyCollection(vecTypes, Map.empty)
          } catch {
            case _: TypeUnificationException =>
              throw new TypeException(
                  "array elements must have the same type, or be coercible to one",
                  expr.text,
                  ctx.docSourceUrl
              )
          }
        WT_Array(t)

      case _: ExprObject =>
        WT_Object

      case ExprMap(m, _) if m.isEmpty =>
        // The map type is unknown.
        // TODO: replace the Any type with a type-parameter
        WT_Map(WT_Any, WT_Any)

      case ExprMap(m, _) =>
        // figure out the types from the first element
        val mTypes: Map[WT, WT] = m.map { item: ExprMapItem =>
          typeEval(item.key, ctx) -> typeEval(item.value, ctx)
        }.toMap
        val (tk, _) =
          try {
            tUtil.unifyCollection(mTypes.keys, Map.empty)
          } catch {
            case _: TypeUnificationException =>
              throw new TypeException("map keys must have the same type, or be coercible to one",
                                      expr.text,
                                      ctx.docSourceUrl)
          }
        val (tv, _) =
          try {
            tUtil.unifyCollection(mTypes.values, Map.empty)
          } catch {
            case _: TypeUnificationException =>
              throw new TypeException("map values must have the same type, or be coercible to one",
                                      expr.text,
                                      ctx.docSourceUrl)
          }
        WT_Map(tk, tv)

      // These are expressions like:
      // ${true="--yes" false="--no" boolean_value}
      case ExprPlaceholderEqual(t: Expr, f: Expr, value: Expr, _) =>
        val tType = typeEval(t, ctx)
        val fType = typeEval(f, ctx)
        if (fType != tType)
          throw new TypeException(s"""|subexpressions ${exprToString(t)} and ${exprToString(f)}
                                      |in ${exprToString(expr)} must have the same type""".stripMargin
                                    .replaceAll("\n", " "),
                                  expr.text,
                                  ctx.docSourceUrl)
        val tv = typeEval(value, ctx)
        if (tv != WT_Boolean)
          throw new TypeException(
              s"${value} in ${exprToString(expr)} should have boolean type, it has type ${tUtil.toString(tv)} instead",
              expr.text,
              ctx.docSourceUrl
          )
        tType

      // An expression like:
      // ${default="foo" optional_value}
      case ExprPlaceholderDefault(default: Expr, value: Expr, _) =>
        val vt = typeEval(value, ctx)
        val dt = typeEval(default, ctx)
        vt match {
          case WT_Optional(vt2) if tUtil.isCoercibleTo(dt, vt2) => dt
          case vt2 if tUtil.isCoercibleTo(dt, vt2)              =>
            // another unsavory case. The optional_value is NOT optional.
            dt
          case _ =>
            throw new TypeException(
                s"""|Expression (${exprToString(value)}) must have type coercible to
                    |(${tUtil.toString(dt)}), it has type (${vt}) instead
                    |""".stripMargin.replaceAll("\n", " "),
                expr.text,
                ctx.docSourceUrl
            )
        }

      // An expression like:
      // ${sep=", " array_value}
      case ExprPlaceholderSep(sep: Expr, value: Expr, _) =>
        val sepType = typeEval(sep, ctx)
        if (sepType != WT_String)
          throw new TypeException(s"separator ${sep} in ${expr} must have string type",
                                  expr.text,
                                  ctx.docSourceUrl)
        val vt = typeEval(value, ctx)
        vt match {
          case WT_Array(x) if tUtil.isCoercibleTo(WT_String, x) =>
            WT_String
          case other =>
            throw new TypeException(
                s"expression ${value} should be coercible to Array[String], but it is ${other}",
                expr.text,
                ctx.docSourceUrl
            )
        }

      // math operators on one argument
      case ExprUniraryPlus(value, _)  => typeEvalMathOp(value, ctx)
      case ExprUniraryMinus(value, _) => typeEvalMathOp(value, ctx)

      // logical operators
      case ExprLor(a: Expr, b: Expr, _)  => typeEvalLogicalOp(a, b, ctx)
      case ExprLand(a: Expr, b: Expr, _) => typeEvalLogicalOp(a, b, ctx)
      case ExprNegate(value: Expr, _)    => typeEvalLogicalOp(value, ctx)

      // equality comparisons
      case ExprEqeq(a: Expr, b: Expr, _) => typeEvalCompareOp(a, b, ctx)
      case ExprNeq(a: Expr, b: Expr, _)  => typeEvalCompareOp(a, b, ctx)
      case ExprLt(a: Expr, b: Expr, _)   => typeEvalCompareOp(a, b, ctx)
      case ExprGte(a: Expr, b: Expr, _)  => typeEvalCompareOp(a, b, ctx)
      case ExprLte(a: Expr, b: Expr, _)  => typeEvalCompareOp(a, b, ctx)
      case ExprGt(a: Expr, b: Expr, _)   => typeEvalCompareOp(a, b, ctx)

      // add is overloaded, it is a special case
      case ExprAdd(a: Expr, b: Expr, _) => typeEvalAdd(a, b, ctx)

      // math operators on two arguments
      case ExprSub(a: Expr, b: Expr, _)    => typeEvalMathOp(a, b, ctx)
      case ExprMod(a: Expr, b: Expr, _)    => typeEvalMathOp(a, b, ctx)
      case ExprMul(a: Expr, b: Expr, _)    => typeEvalMathOp(a, b, ctx)
      case ExprDivide(a: Expr, b: Expr, _) => typeEvalMathOp(a, b, ctx)

      // Access an array element at [index]
      case ExprAt(array: Expr, index: Expr, _) =>
        val idxt = typeEval(index, ctx)
        if (idxt != WT_Int)
          throw new TypeException(s"${index} must be an integer", expr.text, ctx.docSourceUrl)
        val arrayt = typeEval(array, ctx)
        arrayt match {
          case WT_Array(elemType) => elemType
          case _ =>
            throw new TypeException(s"subexpression ${array} in (${expr}) must be an array",
                                    expr.text,
                                    ctx.docSourceUrl)
        }

      // conditional:
      // if (x == 1) then "Sunday" else "Weekday"
      case ExprIfThenElse(cond: Expr, tBranch: Expr, fBranch: Expr, _) =>
        val condType = typeEval(cond, ctx)
        if (condType != WT_Boolean)
          throw new TypeException(s"condition ${exprToString(cond)} must be a boolean",
                                  expr.text,
                                  ctx.docSourceUrl)
        val tBranchT = typeEval(tBranch, ctx)
        val fBranchT = typeEval(fBranch, ctx)
        try {
          val (t, _) = tUtil.unify(tBranchT, fBranchT, Map.empty)
          t
        } catch {
          case _: TypeUnificationException =>
            throw new TypeException(
                s"""|The branches of a conditional expression must be coercable to the same type
                    |expression: ${exprToString(expr)}
                    |  true branch: ${tUtil.toString(tBranchT)}
                    |  flase branch: ${tUtil.toString(fBranchT)}
                    |""".stripMargin,
                expr.text,
                ctx.docSourceUrl
            )
        }

      // Apply a standard library function to arguments. For example:
      //   read_int("4")
      case ExprApply(funcName: String, elements: Vector[Expr], _) =>
        val elementTypes = elements.map(typeEval(_, ctx))
        stdlib.apply(funcName, elementTypes, expr)

      // Access a field in a struct or an object. For example "x.a" in:
      //   Int z = x.a
      case ExprGetName(e: Expr, id: String, _) =>
        val et = typeEval(e, ctx)
        et match {
          case WT_Struct(name, members) =>
            members.get(id) match {
              case None =>
                throw new TypeException(
                    s"Struct ${name} does not have member ${id} in expression",
                    expr.text,
                    ctx.docSourceUrl
                )
              case Some(t) =>
                t
            }

          case WT_Call(name, members) =>
            members.get(id) match {
              case None =>
                throw new TypeException(
                    s"Call object ${name} does not have member ${id} in expression",
                    expr.text,
                    ctx.docSourceUrl
                )
              case Some(t) =>
                t
            }

          // An identifier is a struct, and we want to access
          // a field in it.
          // Person p = census.p
          // String name = p.name
          case WT_Identifier(structName) =>
            // produce the struct definition
            val members = ctx.structs.get(structName) match {
              case None =>
                throw new TypeException(s"unknown struct ${structName}",
                                        expr.text,
                                        ctx.docSourceUrl)
              case Some(WT_Struct(_, members)) => members
              case other =>
                throw new TypeException(s"not a struct ${other}", expr.text, ctx.docSourceUrl)
            }
            members.get(id) match {
              case None =>
                throw new TypeException(s"Struct ${structName} does not have member ${id}",
                                        expr.text,
                                        ctx.docSourceUrl)
              case Some(t) => t
            }

          // accessing a pair element
          case WT_Pair(l, _) if id.toLowerCase() == "left"  => l
          case WT_Pair(_, r) if id.toLowerCase() == "right" => r
          case WT_Pair(_, _) =>
            throw new TypeException(s"accessing a pair with (${id}) is illegal",
                                    expr.text,
                                    ctx.docSourceUrl)

          case _ =>
            throw new TypeException(s"member access (${id}) in expression is illegal",
                                    expr.text,
                                    ctx.docSourceUrl)
        }
    }
  }

  // check the declaration and add a binding for its (variable -> wdlType)
  //
  // In a declaration the right hand type must be coercible to
  // the left hand type.
  //
  // Examples:
  //   Int x
  //   Int x = 5
  //   Int x = 7 + y
  private def applyDecl(decl: Declaration, ctx: Context): (String, WT) = {
    val lhsType: WT = typeTranslate(decl.wdlType, decl.text, ctx)
    (lhsType, decl.expr) match {
      // Int x
      case (_, None) =>
        ()

      case (_, Some(expr)) =>
        val rhsType = typeEval(expr, ctx)
        if (!tUtil.isCoercibleTo(lhsType, rhsType)) {
          throw new TypeException(s"""|${decl.name} is of type ${tUtil.toString(lhsType)}
                                      |but is assigned ${tUtil.toString(rhsType)}
                                      |${expr}
                                      |""".stripMargin.replaceAll("\n", " "),
                                  decl.text,
                                  ctx.docSourceUrl)
        }
    }
    (decl.name, lhsType)
  }

  // type check the input section and return bindings for all of the input variables.
  private def applyInputSection(inputSection: InputSection, ctx: Context): Bindings = {
    inputSection.declarations.foldLeft(Map.empty[String, WT]) {
      case (accu, decl) =>
        val (varName, typ) = applyDecl(decl, ctx.bindVarList(accu, inputSection.text))
        accu + (varName -> typ)
    }
  }

  // type check the input section and return bindings for all of the output variables.
  private def applyOutputSection(outputSection: OutputSection, ctx: Context): Bindings = {
    outputSection.declarations.foldLeft(Map.empty[String, WT]) {
      case (accu, decl) =>
        // check the declaration and add a binding for its (variable -> wdlType)
        val (varName, typ) = applyDecl(decl, ctx.bindVarList(accu, outputSection.text))
        accu + (varName -> typ)
    }
  }

  // calculate the type signature of a workflow or a task
  private def calcSignature(inputSection: Option[InputSection],
                            outputSection: Option[OutputSection],
                            ctx: Context): (Map[String, (WT, Boolean)], Map[String, WT]) = {

    val inputType: Map[String, (WT, Boolean)] = inputSection match {
      case None => Map.empty
      case Some(InputSection(decls, _)) =>
        decls.map {
          case Declaration(name, wdlType, Some(_), text) =>
            // input has a default value, caller may omit it.
            val t = typeTranslate(wdlType, text, ctx)
            name -> (t, true)

          case Declaration(name, TypeOptional(wdlType, _), _, text) =>
            // input is optional, caller can omit it.
            val t = typeTranslate(wdlType, text, ctx)
            name -> (WT_Optional(t), true)

          case Declaration(name, wdlType, _, text) =>
            // input is compulsory
            val t = typeTranslate(wdlType, text, ctx)
            name -> (t, false)
        }.toMap
    }
    val outputType: Map[String, WT] = outputSection match {
      case None => Map.empty
      case Some(OutputSection(decls, _)) =>
        decls.map(decl => decl.name -> typeTranslate(decl.wdlType, decl.text, ctx)).toMap
    }
    (inputType, outputType)
  }

  // The runtime section can make use of values defined in declarations
  private def applyRuntime(rtSection: RuntimeSection, ctx: Context): Unit = {
    rtSection.kvs.foreach {
      case RuntimeKV(_, expr, _) =>
        val _ = typeEval(expr, ctx)
    }
  }

  // TASK
  //
  // - An inputs type has to match the type of its default value (if any)
  // - Check the declarations
  // - Assignments to an output variable must match
  //
  // We can't check the validity of the command section.
  private def applyTask(task: Task, ctxOuter: Context): WT_Task = {
    val ctx: Context = task.input match {
      case None => ctxOuter
      case Some(inpSection) =>
        val bindings = applyInputSection(inpSection, ctxOuter)
        ctxOuter.bindVarList(bindings, task.text)
    }

    // check the declarations, and accumulate context
    val ctxDecl = task.declarations.foldLeft(ctx) {
      case (accu: Context, decl) =>
        val (varName, typ) = applyDecl(decl, accu)
        accu.bindVar(varName, typ, decl.text)
    }

    // check the runtime section
    task.runtime.foreach(rtSection => applyRuntime(rtSection, ctxDecl))

    // check that all expressions can be coereced to a string inside
    // the command section
    task.command.parts.foreach { expr =>
      val t = typeEval(expr, ctxDecl)
      val valid = t match {
        case x if tUtil.isPrimitive(x)              => true
        case WT_Optional(x) if tUtil.isPrimitive(x) => true
        case _                                      => false
      }
      if (!valid)
        throw new TypeException(
            s"Expression ${exprToString(expr)} in the command section is not coercible to a string",
            expr.text,
            ctx.docSourceUrl
        )
    }

    // check the output section. We don't need the returned context.
    task.output.map(x => applyOutputSection(x, ctxDecl))

    // calculate the type signature of the task
    val (inputType, outputType) = calcSignature(task.input, task.output, ctxOuter)
    WT_Task(task.name, inputType, outputType)
  }

  //
  // 1. all the caller arguments have to exist with the correct types
  //    in the callee
  // 2. all the compulsory callee arguments must be specified. Optionals
  //    and arguments that have defaults can be skipped.
  private def applyCall(call: Call, ctx: Context): (String, WT_Call) = {
    val callerInputs: Map[String, WT] = call.inputs match {
      case Some(CallInputs(value, _)) =>
        value.map { inp =>
          inp.name -> typeEval(inp.expr, ctx)
        }.toMap
      case None => Map.empty
    }

    val (calleeInputs, calleeOutputs) = ctx.callables.get(call.name) match {
      case None =>
        throw new TypeException(s"called task/workflow ${call.name} is not defined",
                                call.text,
                                ctx.docSourceUrl)
      case Some(WT_Task(_, input, output)) =>
        (input, output)
      case Some(WT_Workflow(_, input, output)) =>
        (input, output)
      case _ =>
        throw new TypeException(s"callee ${call.name} is not a task or workflow",
                                call.text,
                                ctx.docSourceUrl)
    }

    // type-check input arguments
    callerInputs.foreach {
      case (argName, wdlType) =>
        calleeInputs.get(argName) match {
          case None =>
            throw new TypeException(
                s"call ${call} has argument ${argName} that does not exist in the callee",
                call.text,
                ctx.docSourceUrl
            )
          case Some((calleeType, _)) if regime == Strict =>
            if (calleeType != wdlType)
              throw new TypeException(
                  s"argument ${argName} has wrong type ${wdlType}, expecting ${calleeType}",
                  call.text,
                  ctx.docSourceUrl
              )
          case Some((calleeType, _)) if regime >= Moderate =>
            if (!tUtil.isCoercibleTo(calleeType, wdlType))
              throw new TypeException(
                  s"argument ${argName} has type ${wdlType}, it is not coercible to ${calleeType}",
                  call.text,
                  ctx.docSourceUrl
              )
          case _ => ()
        }
    }

    // check that all the compulsory arguments are provided
    calleeInputs.foreach {
      case (argName, (_, false)) =>
        callerInputs.get(argName) match {
          case None =>
            throw new TypeException(
                s"compulsory argument ${argName} to task/workflow ${call.name} is missing",
                call.text,
                ctx.docSourceUrl
            )
          case Some(_) => ()
        }
      case (_, (_, _)) =>
        // an optional argument, it may not be provided
        ()
    }

    // The name of the call may not contain dots. Examples:
    //
    // call lib.concat as concat     concat
    // call add                      add
    // call a.b.c                    c
    val callName = call.alias match {
      case None if !(call.name contains ".") =>
        call.name
      case None =>
        val parts = call.name.split("\\.")
        parts.last
      case Some(alias) => alias.name
    }

    if (ctx.declarations contains callName)
      throw new TypeException(s"call ${callName} shadows an existing definition",
                              call.text,
                              ctx.docSourceUrl)

    // build a type for the resulting object
    callName -> WT_Call(callName, calleeOutputs)
  }

  // The body of the scatter becomes accessible to statements that come after it.
  // The iterator is not visible outside the scatter body.
  //
  // for (i in [1, 2, 3]) {
  //    call A
  // }
  //
  // Variable "i" is not visible after the scatter completes.
  // A's members are arrays.
  private def applyScatter(scatter: Scatter, ctxOuter: Context): Bindings = {
    val collectionType = typeEval(scatter.expr, ctxOuter)
    val elementType = collectionType match {
      case WT_Array(elementType) => elementType
      case _ =>
        throw new Exception(s"Collection in scatter (${scatter}) is not an array type")
    }
    // add a binding for the iteration variable
    val ctxInner = ctxOuter.bindVar(scatter.identifier, elementType, scatter.text)

    // Add an array type to all variables defined in the scatter body
    val bodyBindings: Bindings = scatter.body.foldLeft(Map.empty[String, WT]) {
      case (accu: Bindings, decl: Declaration) =>
        val (varName, typ) = applyDecl(decl, ctxInner.bindVarList(accu, decl.text))
        accu + (varName -> typ)

      case (accu: Bindings, call: Call) =>
        val (callName, callType) = applyCall(call, ctxInner.bindVarList(accu, call.text))
        accu + (callName -> callType)

      case (accu: Bindings, subSct: Scatter) =>
        // a nested scatter
        val sctBindings = applyScatter(subSct, ctxInner.bindVarList(accu, subSct.text))
        accu ++ sctBindings

      case (accu: Bindings, cond: Conditional) =>
        // a nested conditional
        val condBindings = applyConditional(cond, ctxInner.bindVarList(accu, cond.text))
        accu ++ condBindings

      case (_, other) =>
        throw new Exception(s"Sanity: ${other}")
    }

    // The iterator identifier is not exported outside the scatter
    bodyBindings.map {
      case (callName, callType: WT_Call) =>
        val callOutput = callType.output.map {
          case (name, t) => name -> WT_Array(t)
        }
        callName -> WT_Call(callType.name, callOutput)
      case (varName, typ: WT) =>
        varName -> WT_Array(typ)
    }
  }

  // Ensure that a type is optional, but avoid making it doubly so.
  // For example:
  //   Int --> Int?
  //   Int? --> Int?
  // Avoid this:
  //   Int?  --> Int??
  private def makeOptional(t: WT): WT = {
    t match {
      case WT_Optional(x) => x
      case x              => WT_Optional(x)
    }
  }

  // The body of a conditional is accessible to the statements that come after it.
  // This is different than the scoping rules for other programming languages.
  //
  // Add an optional modifier to all the types inside the body.
  private def applyConditional(cond: Conditional, ctxOuter: Context): Bindings = {
    val condType = typeEval(cond.expr, ctxOuter)
    if (condType != WT_Boolean)
      throw new Exception(s"Expression ${cond.expr} must have boolean type")

    // keep track of the inner/outer bindings. Within the block we need [inner],
    // [outer] is what we produce, which has the optional modifier applied to
    // everything.
    val bodyBindings = cond.body.foldLeft(Map.empty[String, WT]) {
      case (accu: Bindings, decl: Declaration) =>
        val (varName, typ) = applyDecl(decl, ctxOuter.bindVarList(accu, decl.text))
        accu + (varName -> typ)

      case (accu: Bindings, call: Call) =>
        val (callName, callType) = applyCall(call, ctxOuter.bindVarList(accu, call.text))
        accu + (callName -> callType)

      case (accu: Bindings, subSct: Scatter) =>
        // a nested scatter
        val sctBindings = applyScatter(subSct, ctxOuter.bindVarList(accu, subSct.text))
        accu ++ sctBindings

      case (accu: Bindings, cond: Conditional) =>
        // a nested conditional
        val condBindings = applyConditional(cond, ctxOuter.bindVarList(accu, cond.text))
        accu ++ condBindings

      case (_, other) =>
        throw new Exception(s"Sanity: ${other}")
    }

    bodyBindings.map {
      case (callName, callType: WT_Call) =>
        val callOutput = callType.output.map {
          case (name, t) => name -> makeOptional(t)
        }
        callName -> WT_Call(callType.name, callOutput)
      case (varName, typ: WT) =>
        varName -> makeOptional(typ)
    }
  }

  private def applyWorkflow(wf: Workflow, ctxOuter: Context): Context = {
    val ctx: Context = wf.input match {
      case None => ctxOuter
      case Some(inpSection) =>
        val inputs = applyInputSection(inpSection, ctxOuter)
        ctxOuter.bindVarList(inputs, inpSection.text)
    }

    val ctxBody = wf.body.foldLeft(ctx) {
      case (accu: Context, decl: Declaration) =>
        val (name, typ) = applyDecl(decl, accu)
        accu.bindVar(name, typ, decl.text)

      case (accu: Context, call: Call) =>
        val (callName, callType) = applyCall(call, accu)
        accu.bindVar(callName, callType, call.text)

      case (accu: Context, scatter: Scatter) =>
        val sctBindings = applyScatter(scatter, accu)
        accu.bindVarList(sctBindings, scatter.text)

      case (accu: Context, cond: Conditional) =>
        val condBindings = applyConditional(cond, accu)
        accu.bindVarList(condBindings, cond.text)

      case (_, other) =>
        throw new Exception(s"Sanity: ${other}")
    }

    // check the output section. We don't need the returned context.
    wf.output.map(x => applyOutputSection(x, ctxBody))

    // calculate the type signature of the workflow
    val (inputType, outputType) = calcSignature(wf.input, wf.output, ctxOuter)
    val wfSignature = WT_Workflow(wf.name, inputType, outputType)
    val ctxFinal = ctxOuter.bindCallable(wfSignature, wf.text)
    ctxFinal
  }

  // Main entry point
  //
  // check if the WDL document is correctly typed. Otherwise, throw an exception
  // describing the problem in a human readable fashion.
  def apply(doc: Document, ctxOuter: Option[Context] = None): Context = {
    val context: Context =
      doc.elements.foldLeft(ctxOuter.getOrElse(Context(Some(doc.docSourceUrl)))) {
        case (accu: Context, task: Task) =>
          val tt = applyTask(task, accu)
          accu.bindCallable(tt, task.text)

        case (accu: Context, iStat: ImportDoc) =>
          val iCtx = apply(iStat.doc.get)

          // Figure out what to name the sub-document
          val namespace = iStat.name match {
            case None =>
              // Something like
              //    import "http://example.com/lib/stdlib"
              //    import "A/B/C"
              // Where the user does not specify an alias. The namespace
              // will be named:
              //    stdlib
              //    C
              val url = UUtil.getUrl(iStat.addr.value, stdlib.conf.localDirectories)
              val nsName = Paths.get(url.getFile).getFileName.toString
              if (nsName.endsWith(".wdl"))
                nsName.dropRight(".wdl".length)
              else
                nsName
            case Some(x) => x.value
          }

          // add the externally visible definitions to the context
          accu.bindImportedDoc(namespace, iCtx, iStat.aliases, iStat.text)

        case (accu: Context, struct: TypeStruct) =>
          // Add the struct to the context
          val t = typeTranslate(struct, struct.text, accu)
          val t2 = t.asInstanceOf[WT_Struct]
          accu.bind(t2, struct.text)

        case (_, other) =>
          throw new Exception(s"sanity: wrong element type in workflow $other")
      }

    // now that we have types for everything else, we can check the workflow
    doc.workflow match {
      case None     => context
      case Some(wf) => applyWorkflow(wf, context)
    }
  }
}
