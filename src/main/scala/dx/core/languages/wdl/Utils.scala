package dx.core.languages.wdl

import java.nio.file.Path

import dx.compiler.wdl.Utils
import dx.core.ir.{Type, Value}
import dx.core.ir.Type.{
  TArray,
  TBoolean,
  TDirectory,
  TFile,
  TFloat,
  THash,
  TInt,
  TMap,
  TSchema,
  TString
}
import dx.core.ir.Value.{VArray, VBoolean, VDirectory, VFile, VFloat, VHash, VInt, VNull, VString}
import dx.core.languages.{Language, wdl}
import dx.core.languages.Language.Language
import wdlTools.eval.Coercion
import wdlTools.eval.WdlValues.{
  V,
  V_Array,
  V_Boolean,
  V_Directory,
  V_File,
  V_Float,
  V_Int,
  V_Map,
  V_Null,
  V_Object,
  V_String
}
import wdlTools.syntax.{Parsers, SourceLocation, SyntaxException, WdlParser, WdlVersion}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.types.WdlTypes.{
  T,
  T_Any,
  T_Array,
  T_Boolean,
  T_Directory,
  T_File,
  T_Float,
  T_Int,
  T_Map,
  T_Object,
  T_Optional,
  T_String,
  T_Struct
}
import wdlTools.types.{
  TypeCheckingRegime,
  TypeException,
  TypeInfer,
  WdlTypes,
  TypedAbstractSyntax => TAT
}
import wdlTools.util.{Bindings, FileSource, FileSourceResolver, Logger}

object Utils {
  val locPlaceholder: SourceLocation = SourceLocation.empty

  // A self contained WDL workflow
  def getWdlVersion(language: Language): WdlVersion = {
    language match {
      case Language.WdlVDraft2 => WdlVersion.Draft_2
      case Language.WdlV1_0    => WdlVersion.V1
      case Language.WdlV2_0    => WdlVersion.V2
      case other =>
        throw new Exception(s"Unsupported language version ${other}")
    }
  }

  // create a wdl-value of a specific type.
  def genDefaultValueOfType(wdlType: WdlTypes.T,
                            loc: SourceLocation = Utils.locPlaceholder): TAT.Expr = {
    wdlType match {
      case WdlTypes.T_Boolean => TAT.ValueBoolean(value = true, wdlType, loc)
      case WdlTypes.T_Int     => TAT.ValueInt(0, wdlType, loc)
      case WdlTypes.T_Float   => TAT.ValueFloat(0.0, wdlType, loc)
      case WdlTypes.T_String  => TAT.ValueString("", wdlType, loc)
      case WdlTypes.T_File    => TAT.ValueString("placeholder.txt", wdlType, loc)

      // We could convert an optional to a null value, but that causes
      // problems for the pretty printer.
      // WdlValues.V_OptionalValue(wdlType, None)
      case WdlTypes.T_Optional(t) => genDefaultValueOfType(t)

      // The WdlValues.V_Map type HAS to appear before the array types, because
      // otherwise it is coerced into an array. The map has to
      // contain at least one key-value pair, otherwise you get a type error.
      case WdlTypes.T_Map(keyType, valueType) =>
        val k = genDefaultValueOfType(keyType)
        val v = genDefaultValueOfType(valueType)
        TAT.ExprMap(Map(k -> v), wdlType, loc)

      // an empty array
      case WdlTypes.T_Array(_, false) =>
        TAT.ExprArray(Vector.empty, wdlType, loc)

      // Non empty array
      case WdlTypes.T_Array(t, true) =>
        TAT.ExprArray(Vector(genDefaultValueOfType(t)), wdlType, loc)

      case WdlTypes.T_Pair(lType, rType) =>
        TAT.ExprPair(genDefaultValueOfType(lType), genDefaultValueOfType(rType), wdlType, loc)

      case WdlTypes.T_Struct(_, typeMap) =>
        val members = typeMap.map {
          case (fieldName, t) =>
            val key: TAT.Expr = TAT.ValueString(fieldName, WdlTypes.T_String, loc)
            key -> genDefaultValueOfType(t)
        }
        TAT.ExprObject(members, wdlType, loc)

      case _ => throw new Exception(s"Unhandled type ${wdlType}")
    }
  }

  /**
    * Parses a top-level WDL file and all its imports.
    * @param path the path to the WDL file
    * @param fileResolver FileSourceResolver
    * @param regime TypeCheckingRegime
    * @param logger Logger
    * @return (document, aliases), where aliases is a mapping of all the (fully-qualified)
    *         alias names to values. Aliases include Structs defined in any file (which are
    *         *not* namespaced) and all aliases defined in import statements of all documents
    *         (which *are* namespaced).
    */
  def parseSource(path: Path,
                  fileResolver: FileSourceResolver = FileSourceResolver.get,
                  regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                  logger: Logger = Logger.get): (TAT.Document, Bindings[WdlTypes.T_Struct]) = {
    val sourceCode = fileResolver.fromPath(path)
    val parser = Parsers(followImports = true, fileResolver = fileResolver, logger = logger)
      .getParser(sourceCode)
    parseSource(parser, sourceCode, fileResolver, regime, logger)
  }

  def parseSource(parser: WdlParser,
                  sourceCode: FileSource,
                  fileResolver: FileSourceResolver = FileSourceResolver.get,
                  regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                  logger: Logger = Logger.get): (TAT.Document, Bindings[WdlTypes.T_Struct]) = {
    try {
      val doc = parser.parseDocument(sourceCode)
      val (tDoc, ctx) =
        TypeInfer(regime, fileResolver = fileResolver, logger = logger)
          .apply(doc)
      (tDoc, ctx.aliases)
    } catch {
      case se: SyntaxException =>
        logger.error(s"WDL code is syntactically invalid -----\n${sourceCode}")
        throw se
      case te: TypeException =>
        logger.error(
            s"WDL code is syntactically valid BUT it fails type-checking -----\n${sourceCode}"
        )
        throw te
    }
  }

  def toIRType(wdlType: T, optional: Boolean = false): (Type, Boolean) = {
    wdlType match {
      case T_Optional(t) => toIRType(t, optional = true)
      case nonOptType =>
        val t = nonOptType match {
          case T_Boolean         => TBoolean
          case T_Int             => TInt
          case T_Float           => TFloat
          case T_String          => TString
          case T_File            => TFile
          case T_Directory       => TDirectory
          case T_Object          => THash
          case T_Struct(name, _) => TSchema(name)
          case T_Array(t, nonEmpty) =>
            val innerType = toIRType(t) match {
              case (t, false) => t
              case (_, true) =>
                throw new Exception(s"Nested types cannot be optional")
            }
            TArray(innerType, nonEmpty)
          case T_Map(keyType, valueType) =>
            val (key, _) = toIRType(keyType)
            val (value, _) = toIRType(value)
            TMap()
          case _ =>
            throw new Exception(s"Cannot convert WDL type ${wdlType} to IR")
        }
        (t, optional)
    }
  }

  def fromIRType(irType: Type, optional: Boolean): T = {
    val wdlType = irType match {
      case TBoolean   => T_Boolean
      case TInt       => T_Int
      case TFloat     => T_Float
      case TString    => T_String
      case TFile      => T_File
      case TDirectory => T_Directory
      case TArray(t) =>
        T_Array(wdl.Utils.fromIRType(t, optional = false), nonEmpty = false)
      case THash => T_Object
      case _ =>
        throw new Exception(s"Cannot convert IR type ${irType} to WDL")
    }
    if (optional) {
      T_Optional(wdlType)
    } else {
      wdlType
    }
  }

  def toIRValue(wdlValue: V): Value = {
    wdlValue match {
      case V_Null            => VNull
      case V_String(s)       => VString(s)
      case V_Int(i)          => VInt(i)
      case V_Float(f)        => VFloat(f)
      case V_Boolean(b)      => VBoolean(value = b)
      case V_String(path)    => VFile(path)
      case V_File(path)      => VFile(path)
      case V_String(path)    => VDirectory(path)
      case V_Directory(path) => VDirectory(path)
      case V_Array(array) =>
        VArray(array.map(v => wdl.Utils.toIRValue(v)))
      case V_Object(m) =>
        VHash(m.map {
          case (key, value) => key -> wdl.Utils.toIRValue(value)
        })
      case V_Map(m) =>
        VHash(m.map {
          case (V_String(key), value) =>
            key -> wdl.Utils.toIRValue(value)
          case _ =>
            throw new Exception(s"WDL Map value must have String key ${m}")
        })
      case _ =>
        throw new Exception(s"Invalid WDL value ${wdlValue})")
    }
  }

  def toIRValue(wdlValue: V, wdlType: T): Value = {
    (wdlType, wdlValue) match {
      case (_, V_Null)                      => VNull
      case (T_String, V_String(s))          => VString(s)
      case (T_Int, V_Int(i))                => VInt(i)
      case (T_Float, V_Float(f))            => VFloat(f)
      case (T_Boolean, V_Boolean(b))        => VBoolean(value = b)
      case (T_File, V_String(path))         => VFile(path)
      case (T_File, V_File(path))           => VFile(path)
      case (T_Directory, V_String(path))    => VDirectory(path)
      case (T_Directory, V_Directory(path)) => VDirectory(path)
      case (T_Array(t, _), V_Array(array)) =>
        VArray(array.map(v => wdl.Utils.toIRValue(v, t)))
      case (T_Object, o: V_Object) => wdl.Utils.toIRValue(o)
      case (T_Map(T_String, t), V_Map(m)) =>
        VHash(m.map {
          case (V_String(key), value) =>
            key -> wdl.Utils.toIRValue(value, t)
          case _ =>
            throw new Exception(s"WDL Map value must have String key ${m}")
        })
      case _ =>
        throw new Exception(s"Invalid (type, value) combination (${wdlType}, ${wdlValue})")
    }
  }

  def irValueToExpr(value: Value): TAT.Expr = {
    val loc = SourceLocation.empty
    value match {
      case VNull            => TAT.ValueNull(T_Any, loc)
      case VString(s)       => TAT.ValueString(s, T_String, loc)
      case VInt(i)          => TAT.ValueInt(i, T_Int, loc)
      case VFloat(f)        => TAT.ValueFloat(f, T_Float, loc)
      case VBoolean(b)      => TAT.ValueBoolean(b, T_Boolean, loc)
      case VFile(path)      => TAT.ValueFile(path, T_File, loc)
      case VDirectory(path) => TAT.ValueDirectory(path, T_Directory, loc)
      case VArray(array) =>
        val a = array.map(wdl.Utils.irValueToExpr)
        a.headOption.map(_.wdlType) match {
          case None =>
            TAT.ExprArray(Vector.empty, T_Any, loc)
          case Some(t) if a.tail.exists(_.wdlType != t) =>
            throw new Exception(s"Array ${a} contains non-homogeneous values")
          case Some(t) =>
            TAT.ExprArray(a, t, loc)
        }
      case VHash(members) =>
        val m: Map[TAT.Expr, TAT.Expr] = members.map {
          case (key, value) => TAT.ValueString(key, T_String, loc) -> wdl.Utils.irValueToExpr(value)
        }
        TAT.ExprObject(m, T_Object, loc)
      case _ =>
        throw new Exception(s"Cannot convert IR value ${value} to WDL")
    }
  }
}

case class ValueMap(values: Map[String, V]) {
  def contains(id: String): Boolean = values.contains(id)

  def get(id: String, wdlTypes: Vector[WdlTypes.T] = Vector.empty): Option[V] = {
    (values.get(id), wdlTypes) match {
      case (None, _)         => None
      case (value, Vector()) => value
      case (Some(value), _)  => Some(Coercion.coerceToFirst(wdlTypes, value))
    }
  }
}

object ValueMap {
  lazy val empty: ValueMap = ValueMap(Map.empty)
}
