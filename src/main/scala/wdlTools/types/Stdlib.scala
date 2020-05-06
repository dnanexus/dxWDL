package wdlTools.types

import java.net.URL

import WdlTypes._
import wdlTools.util.Options
//import wdlTools.util.Verbosity._
import wdlTools.syntax.{AbstractSyntax, TextSource}
import wdlTools.types.{Util => TUtil}

case class Stdlib(conf: Options) {
  private val tUtil = TUtil(conf)

  private val stdlibV1_0: Vector[WT_StdlibFunc] = Vector(
      WT_Function0("stdout", WT_File),
      WT_Function0("stderr", WT_File),
      WT_Function1("read_lines", WT_File, WT_Array(WT_String)),
      WT_Function1("read_tsv", WT_File, WT_Array(WT_Array(WT_String))),
      WT_Function1("read_map", WT_File, WT_Map(WT_String, WT_String)),
      WT_Function1("read_object", WT_File, WT_Object),
      WT_Function1("read_objects", WT_File, WT_Array(WT_Object)),
      WT_Function1("read_json", WT_File, WT_Any),
      WT_Function1("read_int", WT_File, WT_Int),
      WT_Function1("read_string", WT_File, WT_String),
      WT_Function1("read_float", WT_File, WT_Float),
      WT_Function1("read_boolean", WT_File, WT_Boolean),
      WT_Function1("write_lines", WT_Array(WT_String), WT_File),
      WT_Function1("write_tsv", WT_Array(WT_Array(WT_String)), WT_File),
      WT_Function1("write_map", WT_Map(WT_String, WT_String), WT_File),
      WT_Function1("write_object", WT_Object, WT_File),
      WT_Function1("write_objects", WT_Array(WT_Object), WT_File),
      WT_Function1("write_json", WT_Any, WT_File),
      // Size can take several kinds of arguments.
      WT_Function1("size", WT_File, WT_Float),
      WT_Function1("size", WT_Optional(WT_File), WT_Float),
      WT_Function1("size", WT_Array(WT_File), WT_Float),
      WT_Function1("size", WT_Array(WT_Optional(WT_File)), WT_Float),
      // Size takes an optional units parameter (KB, KiB, MB, GiB, ...)
      WT_Function2("size", WT_File, WT_String, WT_Float),
      WT_Function2("size", WT_Optional(WT_File), WT_String, WT_Float),
      WT_Function2("size", WT_Array(WT_File), WT_String, WT_Float),
      WT_Function2("size", WT_Array(WT_Optional(WT_File)), WT_String, WT_Float),
      WT_Function3("sub", WT_String, WT_String, WT_String, WT_String),
      WT_Function1("range", WT_Int, WT_Array(WT_Int)),
      // Array[Array[X]] transpose(Array[Array[X]])
      WT_Function1("transpose", WT_Array(WT_Array(WT_Var(0))), WT_Array(WT_Array(WT_Var(0)))),
      // Array[Pair(X,Y)] zip(Array[X], Array[Y])
      WT_Function2("zip",
                   WT_Array(WT_Var(0)),
                   WT_Array(WT_Var(1)),
                   WT_Array(WT_Pair(WT_Var(0), WT_Var(1)))),
      // Array[Pair(X,Y)] cross(Array[X], Array[Y])
      WT_Function2("cross",
                   WT_Array(WT_Var(0)),
                   WT_Array(WT_Var(1)),
                   WT_Array(WT_Pair(WT_Var(0), WT_Var(1)))),
      // Integer length(Array[X])
      WT_Function1("length", WT_Array(WT_Var(0)), WT_Int),
      // Array[X] flatten(Array[Array[X]])
      WT_Function1("flatten", WT_Array(WT_Array(WT_Var(0))), WT_Array(WT_Var(0))),
      WT_Function2("prefix", WT_String, WT_Array(WT_Var(0)), WT_Array(WT_String)),
      WT_Function1("select_first", WT_Array(WT_Optional(WT_Var(0))), WT_Var(0)),
      WT_Function1("select_all", WT_Array(WT_Optional(WT_Var(0))), WT_Array(WT_Var(0))),
      WT_Function1("defined", WT_Optional(WT_Var(0)), WT_Boolean),
      // simple functions again
      // basename has two variants
      WT_Function1("basename", WT_String, WT_String),
      WT_Function2("basename", WT_String, WT_String, WT_String),
      WT_Function1("floor", WT_Float, WT_Int),
      WT_Function1("ceil", WT_Float, WT_Int),
      WT_Function1("round", WT_Float, WT_Int),
      // not mentioned in the specification
      WT_Function1("glob", WT_String, WT_Array(WT_File))
  )

  // build a mapping from a function name to all of its prototypes.
  // Some functions are overloaded, so they may have several.
  private val funcProtoMap: Map[String, Vector[WT_StdlibFunc]] = {
    stdlibV1_0.foldLeft(Map.empty[String, Vector[WT_StdlibFunc]]) {
      case (accu, funcDesc: WT_StdlibFunc) =>
        accu.get(funcDesc.name) match {
          case None =>
            accu + (funcDesc.name -> Vector(funcDesc))
          case Some(protoVec: Vector[WT_StdlibFunc]) =>
            accu + (funcDesc.name -> (protoVec :+ funcDesc))
        }
    }
  }

  // evaluate the output type of a function. This may require calculation because
  // some functions are polymorphic in their inputs.
  private def evalOnePrototype(funcDesc: WT_StdlibFunc,
                               inputTypes: Vector[WT],
                               text: TextSource,
                               docSourceUrl: Option[URL]): Option[WT] = {
    val args = funcDesc match {
      case WT_Function0(_, _) if inputTypes.isEmpty => Vector.empty
      case WT_Function1(_, arg1, _)                 => Vector(arg1)
      case WT_Function2(_, arg1, arg2, _)           => Vector(arg1, arg2)
      case WT_Function3(_, arg1, arg2, arg3, _)     => Vector(arg1, arg2, arg3)
      case _                                        => throw new TypeException(s"${funcDesc.name} is not a function", text, docSourceUrl)
    }
    try {
      val (_, ctx) = tUtil.unifyFunctionArguments(args, inputTypes, Map.empty)
      val t = tUtil.substitute(funcDesc.output, ctx, text)
      Some(t)
    } catch {
      case _: TypeUnificationException =>
        None
    }
  }

  def apply(funcName: String,
            inputTypes: Vector[WT],
            expr: AbstractSyntax.Expr,
            docSourceUrl: Option[URL] = None): WT = {
    val candidates = funcProtoMap.get(funcName) match {
      case None =>
        throw new TypeException(s"No function named ${funcName} in the standard library",
                                expr.text,
                                docSourceUrl)
      case Some(protoVec) =>
        protoVec
    }

    // The function may be overloaded, taking several types of inputs. Try to
    // match all of them against the input.
    val allCandidatePrototypes: Vector[Option[WT]] = candidates.map {
      evalOnePrototype(_, inputTypes, expr.text, docSourceUrl)
    }
    val result: Vector[WT] = allCandidatePrototypes.flatten
    result.size match {
      case 0 =>
        val inputsStr = inputTypes.map(tUtil.toString).mkString("\n")
        val candidatesStr = candidates.map(tUtil.toString(_)).mkString("\n")
        throw new TypeException(s"""|Invoking stdlib function ${funcName} with badly typed arguments
                                    |${candidatesStr}
                                    |inputs: ${inputsStr}
                                    |""".stripMargin, expr.text, docSourceUrl)
      //Util.warning(e.getMessage, conf.verbosity)
      case 1 =>
        result.head
      case n =>
        // Match more than one prototype. If they all have the same output type, then it doesn't matter
        // though.
        val possibleOutputTypes: Set[WT] = result.toSet
        if (possibleOutputTypes.size > 1)
          throw new TypeException(s"""|Call to ${funcName} matches ${n} prototypes with different
                                      |output types (${possibleOutputTypes})""".stripMargin
                                    .replaceAll("\n", " "),
                                  expr.text,
                                  docSourceUrl)
        possibleOutputTypes.toVector.head
    }
  }
}
