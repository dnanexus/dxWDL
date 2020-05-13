package wdlTools.types

import java.net.URL

import WdlTypes._
import wdlTools.util.Options
//import wdlTools.util.Verbosity._
import wdlTools.syntax.{TextSource, WdlVersion}

case class Stdlib(conf: Options, version: WdlVersion) {
  private val unify = Unification(conf)

  private val draft2Prototypes: Vector[T_StdlibFunc] = Vector(
      T_Function0("stdout", T_File),
      T_Function0("stderr", T_File),
      T_Function1("read_lines", T_File, T_Array(T_String)),
      T_Function1("read_tsv", T_File, T_Array(T_Array(T_String))),
      T_Function1("read_map", T_File, T_Map(T_String, T_String)),
      T_Function1("read_object", T_File, T_Object),
      T_Function1("read_objects", T_File, T_Array(T_Object)),
      T_Function1("read_json", T_File, T_Any),
      T_Function1("read_int", T_File, T_Int),
      T_Function1("read_string", T_File, T_String),
      T_Function1("read_float", T_File, T_Float),
      T_Function1("read_boolean", T_File, T_Boolean),
      T_Function1("write_lines", T_Array(T_String), T_File),
      T_Function1("write_tsv", T_Array(T_Array(T_String)), T_File),
      T_Function1("write_map", T_Map(T_String, T_String), T_File),
      T_Function1("write_object", T_Object, T_File),
      T_Function1("write_objects", T_Array(T_Object), T_File),
      T_Function1("write_json", T_Any, T_File),
      // Size can take several kinds of arguments.
      T_Function1("size", T_File, T_Float),
      T_Function1("size", T_Optional(T_File), T_Float),
      // Size takes an optional units parameter (KB, KiB, MB, GiB, ...)
      T_Function2("size", T_File, T_String, T_Float),
      T_Function2("size", T_Optional(T_File), T_String, T_Float),
      T_Function3("sub", T_String, T_String, T_String, T_String),
      T_Function1("range", T_Int, T_Array(T_Int)),
      // Array[Array[X]] transpose(Array[Array[X]])
      T_Function1("transpose", T_Array(T_Array(T_Var(0))), T_Array(T_Array(T_Var(0)))),
      // Array[Pair(X,Y)] zip(Array[X], Array[Y])
      T_Function2("zip", T_Array(T_Var(0)), T_Array(T_Var(1)), T_Array(T_Pair(T_Var(0), T_Var(1)))),
      // Array[Pair(X,Y)] cross(Array[X], Array[Y])
      T_Function2("cross",
                  T_Array(T_Var(0)),
                  T_Array(T_Var(1)),
                  T_Array(T_Pair(T_Var(0), T_Var(1)))),
      // Integer length(Array[X])
      T_Function1("length", T_Array(T_Var(0)), T_Int),
      // Array[X] flatten(Array[Array[X]])
      T_Function1("flatten", T_Array(T_Array(T_Var(0))), T_Array(T_Var(0))),
      T_Function2("prefix", T_String, T_Array(T_Var(0)), T_Array(T_String)),
      T_Function1("select_first", T_Array(T_Optional(T_Var(0))), T_Var(0)),
      T_Function1("select_all", T_Array(T_Optional(T_Var(0))), T_Array(T_Var(0))),
      T_Function1("defined", T_Optional(T_Var(0)), T_Boolean),
      // simple functions again
      // basename has two variants
      T_Function1("basename", T_String, T_String),
      T_Function1("floor", T_Float, T_Int),
      T_Function1("ceil", T_Float, T_Int),
      T_Function1("round", T_Float, T_Int),
      // not mentioned in the specification
      T_Function1("glob", T_String, T_Array(T_File))
  )

  // Add the signatures for draft2 and v2 here
  private val v1Prototypes: Vector[T_StdlibFunc] = Vector(
      T_Function0("stdout", T_File),
      T_Function0("stderr", T_File),
      T_Function1("read_lines", T_File, T_Array(T_String)),
      T_Function1("read_tsv", T_File, T_Array(T_Array(T_String))),
      T_Function1("read_map", T_File, T_Map(T_String, T_String)),
      T_Function1("read_object", T_File, T_Object),
      T_Function1("read_objects", T_File, T_Array(T_Object)),
      T_Function1("read_json", T_File, T_Any),
      T_Function1("read_int", T_File, T_Int),
      T_Function1("read_string", T_File, T_String),
      T_Function1("read_float", T_File, T_Float),
      T_Function1("read_boolean", T_File, T_Boolean),
      T_Function1("write_lines", T_Array(T_String), T_File),
      T_Function1("write_tsv", T_Array(T_Array(T_String)), T_File),
      T_Function1("write_map", T_Map(T_String, T_String), T_File),
      T_Function1("write_object", T_Object, T_File),
      T_Function1("write_objects", T_Array(T_Object), T_File),
      T_Function1("write_json", T_Any, T_File),
      // Size can take several kinds of arguments.
      T_Function1("size", T_File, T_Float),
      T_Function1("size", T_Optional(T_File), T_Float),
      T_Function1("size", T_Array(T_File), T_Float),
      T_Function1("size", T_Array(T_Optional(T_File)), T_Float),
      // Size takes an optional units parameter (KB, KiB, MB, GiB, ...)
      T_Function2("size", T_File, T_String, T_Float),
      T_Function2("size", T_Optional(T_File), T_String, T_Float),
      T_Function2("size", T_Array(T_File), T_String, T_Float),
      T_Function2("size", T_Array(T_Optional(T_File)), T_String, T_Float),
      T_Function3("sub", T_String, T_String, T_String, T_String),
      T_Function1("range", T_Int, T_Array(T_Int)),
      // Array[Array[X]] transpose(Array[Array[X]])
      T_Function1("transpose", T_Array(T_Array(T_Var(0))), T_Array(T_Array(T_Var(0)))),
      // Array[Pair(X,Y)] zip(Array[X], Array[Y])
      T_Function2("zip", T_Array(T_Var(0)), T_Array(T_Var(1)), T_Array(T_Pair(T_Var(0), T_Var(1)))),
      // Array[Pair(X,Y)] cross(Array[X], Array[Y])
      T_Function2("cross",
                  T_Array(T_Var(0)),
                  T_Array(T_Var(1)),
                  T_Array(T_Pair(T_Var(0), T_Var(1)))),
      // Integer length(Array[X])
      T_Function1("length", T_Array(T_Var(0)), T_Int),
      // Array[X] flatten(Array[Array[X]])
      T_Function1("flatten", T_Array(T_Array(T_Var(0))), T_Array(T_Var(0))),
      T_Function2("prefix", T_String, T_Array(T_Var(0)), T_Array(T_String)),
      T_Function1("select_first", T_Array(T_Optional(T_Var(0))), T_Var(0)),
      T_Function1("select_all", T_Array(T_Optional(T_Var(0))), T_Array(T_Var(0))),
      T_Function1("defined", T_Optional(T_Var(0)), T_Boolean),
      // simple functions again
      // basename has two variants
      T_Function1("basename", T_String, T_String),
      T_Function2("basename", T_String, T_String, T_String),
      T_Function1("floor", T_Float, T_Int),
      T_Function1("ceil", T_Float, T_Int),
      T_Function1("round", T_Float, T_Int),
      // not mentioned in the specification
      T_Function1("glob", T_String, T_Array(T_File))
  )

  // choose the standard library prototypes according to the WDL version
  private val protoTable: Vector[T_StdlibFunc] = version match {
    case WdlVersion.Draft_2 => draft2Prototypes
    case WdlVersion.V1      => v1Prototypes
    case WdlVersion.V2      => throw new Exception("Wdl version 2 not implemented yet")
  }

  // build a mapping from a function name to all of its prototypes.
  // Some functions are overloaded, so they may have several.
  private val funcProtoMap: Map[String, Vector[T_StdlibFunc]] = {
    protoTable.foldLeft(Map.empty[String, Vector[T_StdlibFunc]]) {
      case (accu, funcDesc: T_StdlibFunc) =>
        accu.get(funcDesc.name) match {
          case None =>
            accu + (funcDesc.name -> Vector(funcDesc))
          case Some(protoVec: Vector[T_StdlibFunc]) =>
            accu + (funcDesc.name -> (protoVec :+ funcDesc))
        }
    }
  }

  // evaluate the output type of a function. This may require calculation because
  // some functions are polymorphic in their inputs.
  private def evalOnePrototype(funcDesc: T_StdlibFunc,
                               inputTypes: Vector[T],
                               text: TextSource,
                               docSourceUrl: Option[URL]): Option[T] = {
    val args = funcDesc match {
      case T_Function0(_, _) if inputTypes.isEmpty => Vector.empty
      case T_Function1(_, arg1, _)                 => Vector(arg1)
      case T_Function2(_, arg1, arg2, _)           => Vector(arg1, arg2)
      case T_Function3(_, arg1, arg2, arg3, _)     => Vector(arg1, arg2, arg3)
      case _                                       => throw new TypeException(s"${funcDesc.name} is not a function", text, docSourceUrl)
    }
    try {
      val (_, ctx) = unify.unifyFunctionArguments(args, inputTypes, Map.empty)
      val t = unify.substitute(funcDesc.output, ctx, text)
      Some(t)
    } catch {
      case _: TypeUnificationException =>
        None
    }
  }

  def apply(funcName: String,
            inputTypes: Vector[T],
            text: TextSource,
            docSourceUrl: Option[URL] = None): T = {
    val candidates = funcProtoMap.get(funcName) match {
      case None =>
        throw new TypeException(s"No function named ${funcName} in the standard library",
                                text,
                                docSourceUrl)
      case Some(protoVec) =>
        protoVec
    }

    // The function may be overloaded, taking several types of inputs. Try to
    // match all of them against the input.
    val allCandidatePrototypes: Vector[Option[T]] = candidates.map {
      evalOnePrototype(_, inputTypes, text, docSourceUrl)
    }
    val result: Vector[T] = allCandidatePrototypes.flatten
    result.size match {
      case 0 =>
        val inputsStr = inputTypes.map(Util.typeToString).mkString("\n")
        val candidatesStr = candidates.map(Util.typeToString(_)).mkString("\n")
        throw new TypeException(s"""|Invoking stdlib function ${funcName} with badly typed arguments
                                    |${candidatesStr}
                                    |inputs: ${inputsStr}
                                    |""".stripMargin, text, docSourceUrl)
      //Util.warning(e.getMessage, conf.verbosity)
      case 1 =>
        result.head
      case n =>
        // Match more than one prototype. If they all have the same output type, then it doesn't matter
        // though.
        val possibleOutputTypes: Set[T] = result.toSet
        if (possibleOutputTypes.size > 1)
          throw new TypeException(s"""|Call to ${funcName} matches ${n} prototypes with different
                                      |output types (${possibleOutputTypes})""".stripMargin
                                    .replaceAll("\n", " "),
                                  text,
                                  docSourceUrl)
        possibleOutputTypes.toVector.head
    }
  }
}
