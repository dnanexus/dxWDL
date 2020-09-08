package dx.core.languages.wdl

import java.nio.file.Path

import dx.core.ir.{Type, Value}
import dx.core.ir.Type._
import dx.core.ir.TypeSerde.UnknownTypeException
import dx.core.ir.Value._
import dx.core.languages.Language
import dx.core.languages.Language.Language
import spray.json.{JsBoolean, JsObject, JsString, JsValue}
import wdlTools.eval.Coercion
import wdlTools.eval.WdlValues._
import wdlTools.syntax.{Parsers, SourceLocation, SyntaxException, WdlParser, WdlVersion}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.types.WdlTypes._
import wdlTools.types.{
  TypeCheckingRegime,
  TypeException,
  TypeInfer,
  WdlTypes,
  TypedAbstractSyntax => TAT
}
import wdlTools.util.{
  DefaultBindings,
  FileSource,
  FileSourceResolver,
  JsUtils,
  Logger,
  StringFileSource
}

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
  def parseSource(
      path: Path,
      fileResolver: FileSourceResolver = FileSourceResolver.get,
      regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
      logger: Logger = Logger.get
  ): (TAT.Document, DefaultBindings[WdlTypes.T_Struct]) = {
    val sourceCode = fileResolver.fromPath(path)
    val parser = Parsers(followImports = true, fileResolver = fileResolver, logger = logger)
      .getParser(sourceCode)
    parseSource(parser, sourceCode, fileResolver, regime, logger)
  }

  def parseSource(
      sourceCodeStr: String,
      fileResolver: FileSourceResolver = FileSourceResolver.get,
      regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
      logger: Logger = Logger.get
  ): (TAT.Document, DefaultBindings[WdlTypes.T_Struct]) = {
    val sourceCode = StringFileSource(sourceCodeStr)
    val parser = Parsers(followImports = true, fileResolver = fileResolver, logger = logger)
      .getParser(sourceCode)
    parseSource(parser, sourceCode, fileResolver, regime, logger)
  }

  def parseSource(
      parser: WdlParser,
      sourceCode: FileSource,
      fileResolver: FileSourceResolver = FileSourceResolver.get,
      regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
      logger: Logger = Logger.get
  ): (TAT.Document, DefaultBindings[WdlTypes.T_Struct]) = {
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

  // create a wdl-value of a specific type.
  def getDefaultValueOfType(wdlType: WdlTypes.T,
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
      case WdlTypes.T_Optional(t) => getDefaultValueOfType(t)

      // The WdlValues.V_Map type HAS to appear before the array types, because
      // otherwise it is coerced into an array. The map has to
      // contain at least one key-value pair, otherwise you get a type error.
      case WdlTypes.T_Map(keyType, valueType) =>
        val k = getDefaultValueOfType(keyType)
        val v = getDefaultValueOfType(valueType)
        TAT.ExprMap(Map(k -> v), wdlType, loc)

      // an empty array
      case WdlTypes.T_Array(_, false) =>
        TAT.ExprArray(Vector.empty, wdlType, loc)

      // Non empty array
      case WdlTypes.T_Array(t, true) =>
        TAT.ExprArray(Vector(getDefaultValueOfType(t)), wdlType, loc)

      case WdlTypes.T_Pair(lType, rType) =>
        TAT.ExprPair(getDefaultValueOfType(lType), getDefaultValueOfType(rType), wdlType, loc)

      case WdlTypes.T_Struct(_, typeMap) =>
        val members = typeMap.map {
          case (fieldName, t) =>
            val key: TAT.Expr = TAT.ValueString(fieldName, WdlTypes.T_String, loc)
            key -> getDefaultValueOfType(t)
        }
        TAT.ExprObject(members, wdlType, loc)

      case _ => throw new Exception(s"Unhandled type ${wdlType}")
    }
  }

  def serializeType(t: WdlTypes.T): JsValue = {
    t match {
      case T_Boolean         => JsString("Boolean")
      case T_Int             => JsString("Int")
      case T_Float           => JsString("Float")
      case T_String          => JsString("String")
      case T_File            => JsString("File")
      case T_Directory       => JsString("Directory")
      case T_Object          => JsString("Object")
      case T_Struct(name, _) => JsString(name)
      case T_Array(memberType, nonEmpty) =>
        JsObject(
            Map(
                "name" -> JsString("Array"),
                "type" -> serializeType(memberType),
                "nonEmpty" -> JsBoolean(nonEmpty)
            )
        )
      case T_Pair(lType, rType) =>
        JsObject(
            Map(
                "name" -> JsString("Pair"),
                "leftType" -> serializeType(lType),
                "rightType" -> serializeType(rType)
            )
        )
      case T_Map(keyType, valueType) =>
        JsObject(
            Map(
                "name" -> JsString("Map"),
                "keyType" -> serializeType(keyType),
                "valueType" -> serializeType(valueType)
            )
        )
      case T_Optional(inner) =>
        serializeType(inner) match {
          case name: JsString =>
            JsObject(Map("name" -> name, "optional" -> JsBoolean(true)))
          case JsObject(fields) =>
            JsObject(fields + ("optional" -> JsBoolean(true)))
        }
    }
  }

  def simpleFromString(s: String): WdlTypes.T = {
    s match {
      case "Boolean"   => T_Boolean
      case "Int"       => T_Int
      case "Float"     => T_Float
      case "String"    => T_String
      case "File"      => T_File
      case "Directory" => T_Directory
      case "Object"    => T_Object
      case _ if s.endsWith("?") =>
        simpleFromString(s.dropRight(1)) match {
          case T_Optional(_) =>
            throw new Exception(s"nested optional type ${s}")
          case inner =>
            T_Optional(inner)
        }
      case s if s.contains("[") =>
        throw new Exception(s"type ${s} is not primitive")
      case _ =>
        throw UnknownTypeException(s"Unknown type ${s}")
    }
  }

  def deserializeType(jsValue: JsValue, typeAliases: Map[String, WdlTypes.T]): WdlTypes.T = {
    def resolveType(name: String): WdlTypes.T = {
      try {
        simpleFromString(name)
      } catch {
        case _: UnknownTypeException if typeAliases.contains(name) =>
          typeAliases(name)
      }
    }
    def inner(innerValue: JsValue): WdlTypes.T = {
      innerValue match {
        case JsString(name) => resolveType(name)
        case JsObject(fields) =>
          val t = fields("name") match {
            case JsString("Array") =>
              val arrayType = inner(fields("type"))
              val nonEmpty = fields.get("nonEmpty").exists(JsUtils.getBoolean(_))
              T_Array(arrayType, nonEmpty)
            case JsString("Map") =>
              val keyType = inner(fields("keyType"))
              val valueType = inner(fields("valueType"))
              T_Map(keyType, valueType)
            case JsString("Pair") =>
              val lType = inner(fields("leftType"))
              val rType = inner(fields("rightType"))
              T_Pair(lType, rType)
            case JsString(name) => resolveType(name)
          }
          if (fields.get("optional").exists(JsUtils.getBoolean(_))) {
            T_Optional(t)
          } else {
            t
          }
      }
    }
    inner(jsValue)
  }

  def toIRType(wdlType: T): Type = {
    wdlType match {
      case T_Boolean     => TBoolean
      case T_Int         => TInt
      case T_Float       => TFloat
      case T_String      => TString
      case T_File        => TFile
      case T_Directory   => TDirectory
      case T_Object      => THash
      case T_Optional(t) => TOptional(toIRType(t))
      case T_Array(t, nonEmpty) =>
        TArray(toIRType(t), nonEmpty)
      case T_Map(keyType, valueType) =>
        val key = toIRType(keyType)
        val value = toIRType(valueType)
        TMap(key, value)
      case T_Struct(name, members) =>
        TSchema(name, members.map {
          case (key, value) => key -> toIRType(value)
        })
      case _ =>
        throw new Exception(s"Cannot convert WDL type ${wdlType} to IR")
    }
  }

  def toIRTypeMap(wdlTypes: Map[String, T]): Map[String, Type] = {
    wdlTypes.map {
      case (name, t) => name -> toIRType(t)
    }
  }

  def fromIRType(irType: Type, typeAliases: Map[String, T] = Map.empty): T = {
    def inner(innerType: Type): T = {
      innerType match {
        case TBoolean     => T_Boolean
        case TInt         => T_Int
        case TFloat       => T_Float
        case TString      => T_String
        case TFile        => T_File
        case TDirectory   => T_Directory
        case THash        => T_Object
        case TOptional(t) => T_Optional(inner(t))
        case TArray(t, nonEmpty) =>
          T_Array(inner(t), nonEmpty = nonEmpty)
        case TMap(key, value) =>
          val keyType = inner(key)
          val valueType = inner(value)
          T_Map(keyType, valueType)
        case TSchema(name, _) if typeAliases.contains(name) =>
          typeAliases(name)
        case TSchema(name, _) =>
          throw new Exception(s"Unknown type ${name}")
        case _ =>
          throw new Exception(s"Cannot convert IR type ${innerType} to WDL")
      }
    }
    inner(irType)
  }

  def toIRValue(wdlValue: V): Value = {
    wdlValue match {
      case V_Null            => VNull
      case V_Boolean(b)      => VBoolean(value = b)
      case V_Int(i)          => VInt(i)
      case V_Float(f)        => VFloat(f)
      case V_String(s)       => VString(s)
      case V_File(path)      => VFile(path)
      case V_Directory(path) => VDirectory(path)
      case V_Array(array) =>
        VArray(array.map(v => toIRValue(v)))
      case V_Object(members) =>
        VHash(members.map {
          case (key, value) => key -> toIRValue(value)
        })
      case V_Struct(_, members) =>
        VHash(members.map {
          case (key, value) => key -> toIRValue(value)
        })
      case V_Map(m) =>
        VMap(m.map {
          case (key, value) => toIRValue(key) -> toIRValue(value)
        })
      case _ =>
        throw new Exception(s"Invalid WDL value ${wdlValue})")
    }
  }

  def toIRValue(wdlValue: V, wdlType: T): Value = {
    (wdlType, wdlValue) match {
      case (_, V_Null)                      => VNull
      case (T_Boolean, V_Boolean(b))        => VBoolean(value = b)
      case (T_Int, V_Int(i))                => VInt(i)
      case (T_Float, V_Float(f))            => VFloat(f)
      case (T_String, V_String(s))          => VString(s)
      case (T_File, V_String(path))         => VFile(path)
      case (T_File, V_File(path))           => VFile(path)
      case (T_Directory, V_String(path))    => VDirectory(path)
      case (T_Directory, V_Directory(path)) => VDirectory(path)
      case (T_Object, o: V_Object)          => toIRValue(o)
      case (T_Optional(t), V_Optional(v))   => toIRValue(v, t)
      case (T_Optional(t), v)               => toIRValue(v, t)
      case (t, V_Optional(v))               => toIRValue(v, t)
      case (T_Array(_, true), V_Array(array)) if array.isEmpty =>
        throw new Exception(
            s"Empty array with non-empty (+) quantifier"
        )
      case (T_Array(t, _), V_Array(array)) =>
        VArray(array.map(v => toIRValue(v, t)))
      case (T_Map(keyType, valueType), V_Map(m)) =>
        VMap(m.map {
          case (key, value) =>
            toIRValue(key, keyType) -> toIRValue(value, valueType)
        })
      case (T_Struct(name, memberTypes), V_Object(members)) =>
        // ensure 1) members keys are a subset of memberTypes keys, 2) members
        // values are convertable to the corresponding types, and 3) any keys
        // in memberTypes that do not appear in members are optional
        val keys1 = members.keySet
        val keys2 = memberTypes.keySet
        val extra = keys2.diff(keys1)
        if (extra.nonEmpty) {
          throw new Exception(
              s"struct ${name} value has members that do not appear in the struct definition: ${extra}"
          )
        }
        val missingNonOptional = keys1.diff(keys2).map(key => key -> memberTypes(key)).filterNot {
          case (_, T_Optional(_)) => false
          case _                  => true
        }
        if (missingNonOptional.nonEmpty) {
          throw new Exception(
              s"struct ${name} value is missing non-optional members ${missingNonOptional}"
          )
        }
        VHash(members.map {
          case (key, value) => key -> toIRValue(value, memberTypes(key))
        })
      case _ =>
        throw new Exception(s"Invalid (type, value) combination (${wdlType}, ${wdlValue})")
    }
  }

  def fromIRValue(value: Value, name: Option[String]): V = {
    value match {
      case VNull         => V_Null
      case VBoolean(b)   => V_Boolean(b)
      case VInt(i)       => V_Int(i)
      case VFloat(f)     => V_Float(f)
      case VString(s)    => V_String(s)
      case VFile(f)      => V_File(f)
      case VDirectory(d) => V_Directory(d)
      case VArray(array) =>
        V_Array(array.zipWithIndex.map {
          case (v, i) => fromIRValue(v, name.map(n => s"${n}[${i}]"))
        })
      case VHash(fields) =>
        V_Object(fields.map {
          case (key, value) => key -> fromIRValue(value, name.map(n => s"${n}[${key}]"))
        })
      case VMap(fields) =>
        V_Map(fields.map {
          case (key, value) =>
            val elementName = name.map(n => s"${n}[${key}]")
            fromIRValue(key, elementName) -> fromIRValue(value, elementName)
        })
      case _ =>
        throw new Exception(
            s"Cannot convert ${name.getOrElse("IR")} value ${value} to WDL value"
        )
    }
  }

  def fromIRValue(value: Value,
                  wdlType: T,
                  name: String,
                  handler: Option[(Value, T, String) => Option[V]] = None): V = {
    def inner(innerValue: Value, innerType: T, innerName: String): V = {
      val v = handler.flatMap(_(innerValue, innerType, innerName))
      if (v.isDefined) {
        return v.get
      }
      (wdlType, value) match {
        case (T_Optional(_), VNull)          => V_Null
        case (T_Boolean, VBoolean(b))        => V_Boolean(value = b)
        case (T_Int, VInt(i))                => V_Int(i)
        case (T_Float, VFloat(f))            => V_Float(f)
        case (T_String, VString(s))          => V_String(s)
        case (T_File, VString(path))         => V_File(path)
        case (T_File, VFile(path))           => V_File(path)
        case (T_Directory, VString(path))    => V_Directory(path)
        case (T_Directory, VDirectory(path)) => V_Directory(path)
        case (T_Object, o: VHash)            => fromIRValue(o, Some(innerName))
        case (T_Optional(t), v)              => V_Optional(inner(v, t, innerName))
        case (T_Array(_, true), VArray(array)) if array.isEmpty =>
          throw new Exception(
              s"Empty array with non-empty (+) quantifier"
          )
        case (T_Array(t, _), VArray(array)) =>
          V_Array(array.zipWithIndex.map {
            case (v, i) => inner(v, t, s"${innerName}[${i}]")
          })
        case (T_Map(keyType, valueType), VMap(m)) =>
          V_Map(m.map {
            case (key, value) =>
              val elementName = s"${innerName}[${key}]"
              inner(key, keyType, elementName) -> inner(value, valueType, elementName)
          })
        case (T_Struct(structName, memberTypes), VHash(members)) =>
          // ensure 1) members keys are a subset of memberTypes keys, 2) members
          // values are convertable to the corresponding types, and 3) any keys
          // in memberTypes that do not appear in members are optional
          val keys1 = members.keySet
          val keys2 = memberTypes.keySet
          val extra = keys2.diff(keys1)
          if (extra.nonEmpty) {
            throw new Exception(
                s"struct ${structName} value has members that do not appear in the struct definition: ${extra}"
            )
          }
          val missingNonOptional = keys1.diff(keys2).map(key => key -> memberTypes(key)).filterNot {
            case (_, T_Optional(_)) => false
            case _                  => true
          }
          if (missingNonOptional.nonEmpty) {
            throw new Exception(
                s"struct ${structName} value is missing non-optional members ${missingNonOptional}"
            )
          }
          V_Object(members.map {
            case (key, value) => key -> inner(value, memberTypes(key), s"${innerName}[${key}]")
          })
        case _ =>
          throw new Exception(
              s"Cannot convert ${innerName} (${innerType}, ${innerValue}) to WDL value"
          )
      }
    }
    inner(value, wdlType, name)
  }

  private def ensureUniformType(exprs: Iterable[TAT.Expr]): T = {
    exprs.headOption.map(_.wdlType) match {
      case Some(t) if exprs.tail.exists(_.wdlType != t) =>
        throw new Exception(s"${exprs} contains non-homogeneous values")
      case Some(t) => t
      case None    => T_Any
    }
  }

  def irValueToExpr(value: Value): TAT.Expr = {
    val loc = SourceLocation.empty
    value match {
      case VNull            => TAT.ValueNull(T_Any, loc)
      case VBoolean(b)      => TAT.ValueBoolean(b, T_Boolean, loc)
      case VInt(i)          => TAT.ValueInt(i, T_Int, loc)
      case VFloat(f)        => TAT.ValueFloat(f, T_Float, loc)
      case VString(s)       => TAT.ValueString(s, T_String, loc)
      case VFile(path)      => TAT.ValueFile(path, T_File, loc)
      case VDirectory(path) => TAT.ValueDirectory(path, T_Directory, loc)
      case VArray(array) =>
        val a = array.map(irValueToExpr)
        val t = ensureUniformType(a)
        TAT.ExprArray(a, t, loc)
      case VHash(members) =>
        val m: Map[TAT.Expr, TAT.Expr] = members.map {
          case (key, value) => TAT.ValueString(key, T_String, loc) -> irValueToExpr(value)
        }
        TAT.ExprObject(m, T_Object, loc)
      case VMap(members) =>
        val m = members.map {
          case (key, value) => irValueToExpr(key) -> irValueToExpr(value)
        }
        val keyType = ensureUniformType(m.keys)
        val valueType = ensureUniformType(m.values)
        TAT.ExprMap(m, T_Map(keyType, valueType), loc)
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
