package dx.core.languages.wdl

import java.nio.file.Path

import dx.core.ir.{Type, TypeSerde, Value}
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
  TypedAbstractSyntax => TAT,
  Utils => TUtils
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
  def parseSourceFile(
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

  def parseSourceString(
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

  def parseSingleTask(
      sourceCode: String,
      fileResolver: FileSourceResolver = FileSourceResolver.get
  ): (TAT.Task, DefaultBindings[WdlTypes.T_Struct], TAT.Document) = {
    val (doc, typeAliases) = parseSourceString(sourceCode, fileResolver)
    if (doc.workflow.isDefined) {
      throw new Exception("a workflow shouldn't be a member of this document")
    }
    val tasks = doc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    if (tasks.isEmpty) {
      throw new Exception("no tasks in this WDL program")
    }
    if (tasks.size > 1) {
      throw new Exception("More than one task in this WDL program")
    }
    (tasks.values.head, typeAliases, doc)
  }

  def parseWorkflow(
      sourceCode: String,
      fileResolver: FileSourceResolver = FileSourceResolver.get
  ): (TAT.Workflow, Map[String, TAT.Task], DefaultBindings[WdlTypes.T_Struct], TAT.Document) = {
    val (doc, typeAliases) = parseSourceString(sourceCode, fileResolver)
    val workflow = doc.workflow.getOrElse(
        throw new RuntimeException("This document should have a workflow")
    )
    val tasks = doc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    (workflow, tasks, typeAliases, doc)
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
          case other =>
            throw new Exception(s"unhandled inner type ${other}")
        }
      case _ =>
        throw new Exception(s"Unhandled type ${t}")
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
            case JsString(name) =>
              resolveType(name)
            case _ =>
              throw new Exception(s"unhandled type value ${innerValue}")
          }
          if (fields.get("optional").exists(JsUtils.getBoolean(_))) {
            T_Optional(t)
          } else {
            t
          }
        case other =>
          throw new Exception(s"unexpected type value ${other}")
      }
    }
    inner(jsValue)
  }

  // Functions to convert between WDL and IR types and values.
  // WDL has two types that IR does not: Pair and Map. These
  // are represented as objects with specific keys ('left' and
  // 'right' for Pair, 'keys' and 'values' for Map). We define
  // a special TSchema for each of these types.

  /**
    * Name prefix for Pair-type schemas.
    */
  val PairSchemaPrefix = "Pair___"

  /**
    * Pair.left key that can be used in WDL input files.
    */
  val PairLeftKey = "left"

  /**
    * Pair.right key that can be used in WDL input files.
    */
  val PairRightKey = "right"

  def createPairSchema(left: Type, right: Type): TSchema = {
    val name = s"${PairSchemaPrefix}(${TypeSerde.toString(left)}, ${TypeSerde.toString(right)})"
    TSchema(name, Map(PairLeftKey -> left, PairRightKey -> right))
  }

  def isPairSchema(t: TSchema): Boolean = {
    t.name.startsWith(PairSchemaPrefix) && t.members.size == 2 && t.members.keySet == Set(
        PairLeftKey,
        PairRightKey
    )
  }

  def isPairValue(fields: Map[String, _]): Boolean = {
    fields.size == 2 && fields.keySet == Set(PairLeftKey, PairRightKey)
  }

  /**
    * Name prefix for Map-type schemas.
    */
  val MapSchemaPrefix = "Map___"

  /**
    * Map.keys key that can be used in WDL input files.
    */
  val MapKeysKey = "keys"

  /**
    * Map.values key that can be used in WDL  input files.
    */
  val MapValuesKey = "values"

  def createMapSchema(keyType: Type, valueType: Type): TSchema = {
    val name =
      s"${MapSchemaPrefix}(${TypeSerde.toString(keyType)}, ${TypeSerde.toString(valueType)})"
    TSchema(name, Map(MapKeysKey -> TArray(keyType), MapValuesKey -> TArray(valueType)))
  }

  def isMapSchema(t: TSchema): Boolean = {
    t.name.startsWith(MapSchemaPrefix) && t.members.size == 2 && t.members.keySet == Set(
        MapKeysKey,
        MapValuesKey
    )
  }

  def isMapValue(fields: Map[String, _]): Boolean = {
    fields.size == 2 && fields.keySet == Set(MapKeysKey, MapValuesKey)
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
      case T_Struct(name, members) =>
        TSchema(name, members.map {
          case (key, value) => key -> toIRType(value)
        })
      case T_Pair(leftType, rightType) =>
        createPairSchema(toIRType(leftType), toIRType(rightType))
      case T_Map(keyType, valueType) =>
        createMapSchema(toIRType(keyType), toIRType(valueType))
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
        case TSchema(name, _) if typeAliases.contains(name) =>
          typeAliases(name)
        case pairSchema: TSchema if isPairSchema(pairSchema) =>
          T_Pair(inner(pairSchema.members(PairLeftKey)), inner(pairSchema.members(PairRightKey)))
        case mapSchema: TSchema if isMapSchema(mapSchema) =>
          T_Map(inner(mapSchema.members(MapKeysKey)), inner(mapSchema.members(MapValuesKey)))
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
      case V_Pair(left, right) =>
        // encode this as a hash with 'left' and 'right' keys
        VHash(
            Map(
                PairLeftKey -> toIRValue(left),
                PairRightKey -> toIRValue(right)
            )
        )
      case V_Map(members) =>
        // encode this as a hash with 'keys' and 'values' keys
        val (keys, values) = members.map {
          case (k, v) => (toIRValue(k), toIRValue(v))
        }.unzip
        VHash(
            Map(
                MapKeysKey -> VArray(keys.toVector),
                MapValuesKey -> VArray(values.toVector)
            )
        )
      case V_Object(members) =>
        VHash(members.map {
          case (key, value) => key -> toIRValue(value)
        })
      case V_Struct(_, members) =>
        VHash(members.map {
          case (key, value) => key -> toIRValue(value)
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
      case (T_Pair(leftType, rightType), V_Pair(leftValue, rightValue)) =>
        // encode this as a hash with left and right keys
        VHash(
            Map(
                PairLeftKey -> toIRValue(leftValue, leftType),
                PairRightKey -> toIRValue(rightValue, rightType)
            )
        )
      case (T_Map(keyType, valueType), V_Map(members)) =>
        // encode this as a hash with 'keys' and 'values' keys
        val (keys, values) = members.map {
          case (k, v) => (toIRValue(k, keyType), toIRValue(v, valueType))
        }.unzip
        VHash(
            Map(
                MapKeysKey -> VArray(keys.toVector),
                MapValuesKey -> VArray(values.toVector)
            )
        )
      case (T_Struct(name, memberTypes), V_Struct(vName, memberValues)) if name == vName =>
        structToIRValue(name, memberValues, memberTypes)
      case (T_Struct(name, memberTypes), V_Object(memberValues)) =>
        structToIRValue(name, memberValues, memberTypes)
      case _ =>
        throw new Exception(s"Invalid (type, value) combination (${wdlType}, ${wdlValue})")
    }
  }

  private def structToIRValue(name: String,
                              memberValues: Map[String, V],
                              memberTypes: Map[String, T]): VHash = {
    // ensure 1) members keys are a subset of memberTypes keys, 2) members
    // values are convertable to the corresponding types, and 3) any keys
    // in memberTypes that do not appear in members are optional
    val keys1 = memberValues.keySet
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
    VHash(memberValues.map {
      case (key, value) => key -> toIRValue(value, memberTypes(key))
    })
  }

  def toIR(wdl: Map[String, (T, V)]): Map[String, (Type, Value)] = {
    wdl.map {
      case (name, (wdlType, wdlValue)) =>
        val irType = toIRType(wdlType)
        val irValue = toIRValue(wdlValue, wdlType)
        name -> (irType, irValue)
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
      case VHash(fields) if isPairValue(fields) =>
        V_Pair(
            fromIRValue(fields(PairLeftKey), name.map(n => s"${n}.${PairLeftKey}")),
            fromIRValue(fields(PairRightKey), name.map(n => s"${n}.${PairRightKey}"))
        )
      case VHash(fields) if isMapValue(fields) =>
        val keys = fromIRValue(fields(MapKeysKey), name.map(n => s"${n}[${MapKeysKey}]"))
        val values =
          fromIRValue(fields(MapValuesKey), name.map(n => s"${n}[${MapValuesKey}]"))
        (keys, values) match {
          case (V_Array(keyArray), V_Array(valueArray)) =>
            V_Map(keyArray.zip(valueArray).toMap)
          case other =>
            throw new Exception(s"invalid map value ${other}")
        }
      case VHash(fields) =>
        V_Object(fields.map {
          case (key, value) => key -> fromIRValue(value, name.map(n => s"${n}[${key}]"))
        })
      case _ =>
        throw new Exception(
            s"cannot convert ${name.getOrElse("IR")} value ${value} to WDL value"
        )
    }
  }

  def fromIRValue(value: Value, wdlType: T, name: String): V = {
    def inner(innerValue: Value, innerType: T, innerName: String): V = {
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
        case (T_Pair(leftType, rightType), VHash(fields)) if isPairValue(fields) =>
          V_Pair(
              inner(fields(PairLeftKey), leftType, s"${name}.${PairLeftKey}"),
              inner(fields(PairRightKey), rightType, s"${name}.${PairRightKey}")
          )
        case (T_Map(keyType, valueType), VHash(fields)) if isMapValue(fields) =>
          val keys = inner(fields(MapKeysKey), keyType, s"${name}[${MapKeysKey}]")
          val values = inner(fields(MapValuesKey), valueType, s"${name}[${MapValuesKey}]")
          (keys, values) match {
            case (V_Array(keyArray), V_Array(valueArray)) =>
              V_Map(keyArray.zip(valueArray).toMap)
            case other =>
              throw new Exception(s"invalid map value ${other}")
          }
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

  def fromIR(ir: Map[String, (Type, Value)],
             typeAliases: Map[String, T_Struct]): Map[String, (T, V)] = {
    ir.map {
      case (name, (t, v)) =>
        val wdlType = fromIRType(t, typeAliases)
        val wdlValue = fromIRValue(v, wdlType, name)
        name -> (wdlType, wdlValue)
    }
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
      case VHash(fields) if isPairValue(fields) =>
        val left = irValueToExpr(fields(PairLeftKey))
        val right = irValueToExpr(fields(PairRightKey))
        TAT.ExprPair(left, right, T_Pair(left.wdlType, right.wdlType), loc)
      case VHash(fields) if isMapValue(fields) =>
        val keys = irValueToExpr(fields(MapKeysKey))
        val values = irValueToExpr(fields(MapValuesKey))
        (keys, values) match {
          case (TAT.ExprArray(keyArray, keyType, _), TAT.ExprArray(valueArray, valueType, _)) =>
            TAT.ExprMap(keyArray.zip(valueArray).toMap, T_Map(keyType, valueType), loc)
          case other =>
            throw new Exception(s"invalid map value ${other}")
        }
      case VHash(members) =>
        val m: Map[TAT.Expr, TAT.Expr] = members.map {
          case (key, value) => TAT.ValueString(key, T_String, loc) -> irValueToExpr(value)
        }
        TAT.ExprObject(m, T_Object, loc)
      case _ =>
        throw new Exception(s"cannot convert IR value ${value} to WDL")
    }
  }

  /**
    * A trivial expression has no operators, it is either(1) a constant,
    * (2) a single identifier, or (3) an access to a call field.
    * For example, `5`, `['a', 'b', 'c']`, and `true` are trivial.
    * 'x + y'  is not.
    * @param expr expression
    * @return
    */
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

  /**
    * Deep search for all calls in WorkflowElements.
    * @param elements WorkflowElements
    * @return
    */
  def deepFindCalls(elements: Vector[TAT.WorkflowElement]): Vector[TAT.Call] = {
    elements.foldLeft(Vector.empty[TAT.Call]) {
      case (accu, call: TAT.Call) =>
        accu :+ call
      case (accu, ssc: TAT.Scatter) =>
        accu ++ deepFindCalls(ssc.body)
      case (accu, ifStmt: TAT.Conditional) =>
        accu ++ deepFindCalls(ifStmt.body)
      case (accu, _) =>
        accu
    }
  }

  // figure out the identifiers used in an expression.
  //
  // For example:
  //   expression   inputs
  //   x + y        Vector(x, y)
  //   x + y + z    Vector(x, y, z)
  //   1 + 9        Vector.empty
  //   "a" + "b"    Vector.empty
  //   foo.y + 3    Vector(foo.y)   [withMember = false]
  //   foo.y + 3    Vector(foo)     [withMember = true]
  //
  def getExpressionInputs(expr: TAT.Expr,
                          withMember: Boolean = true): Vector[(String, WdlTypes.T, Boolean)] = {
    def inner(expr: TAT.Expr): Vector[(String, WdlTypes.T, Boolean)] = {
      expr match {
        case _: TAT.ValueNull      => Vector.empty
        case _: TAT.ValueNone      => Vector.empty
        case _: TAT.ValueBoolean   => Vector.empty
        case _: TAT.ValueInt       => Vector.empty
        case _: TAT.ValueFloat     => Vector.empty
        case _: TAT.ValueString    => Vector.empty
        case _: TAT.ValueFile      => Vector.empty
        case _: TAT.ValueDirectory => Vector.empty
        case TAT.ExprIdentifier(id, wdlType, _) =>
          Vector((id, wdlType, TUtils.isOptional(wdlType)))
        case TAT.ExprCompoundString(valArr, _, _) =>
          valArr.flatMap(elem => inner(elem))
        case TAT.ExprPair(l, r, _, _) =>
          inner(l) ++ inner(r)
        case TAT.ExprArray(arrVal, _, _) =>
          arrVal.flatMap(elem => inner(elem))
        case TAT.ExprMap(valMap, _, _) =>
          valMap
            .map { case (k, v) => inner(k) ++ inner(v) }
            .toVector
            .flatten
        case TAT.ExprObject(fields, _, _) =>
          fields
            .map { case (_, v) => inner(v) }
            .toVector
            .flatten
        case TAT.ExprPlaceholderCondition(t: TAT.Expr, f: TAT.Expr, value: TAT.Expr, _, _) =>
          inner(t) ++ inner(f) ++ inner(value)
        case TAT.ExprPlaceholderDefault(default: TAT.Expr, value: TAT.Expr, _, _) =>
          inner(default) ++ inner(value)
        case TAT.ExprPlaceholderSep(sep: TAT.Expr, value: TAT.Expr, _, _) =>
          inner(sep) ++ inner(value)
        // Access an array element at [index]
        case TAT.ExprAt(value, index, _, _) =>
          inner(value) ++ inner(index)
        // conditional:
        case TAT.ExprIfThenElse(cond, tBranch, fBranch, _, _) =>
          inner(cond) ++ inner(tBranch) ++ inner(fBranch)
        // Apply a standard library function to arguments.
        //
        // TODO: some arguments may be _optional_ we need to take that
        // into account. We need to look into the function type
        // and figure out which arguments are optional.
        case TAT.ExprApply(_, _, elements, _, _) =>
          elements.flatMap(inner)
        // Access the field of a call/struct/etc. What we do here depends on the
        // value of withMember. When we the expression value is a struct and we
        // are generating a closure, we only need the parent struct, not the member.
        // Otherwise, we need to add the member name.
        // Note: this case was added to fix bug/APPS-104 - there may be other expressions
        // besides structs that need to not have the member name added when withMember = false.
        // It may also be the case that the bug is with construction of the environment rather
        // than here with the closure.
        case TAT.ExprGetName(expr, _, _, _)
            if !withMember && TUtils.unwrapOptional(expr.wdlType).isInstanceOf[WdlTypes.T_Struct] =>
          inner(expr)
        // Access a field of an identifier
        //   Int z = eliminateDuplicate.fields
        case TAT.ExprGetName(TAT.ExprIdentifier(id, _, _), fieldName, wdlType, _) =>
          Vector((s"${id}.${fieldName}", wdlType, false))
        // Access a field of the result of an expression
        case TAT.ExprGetName(expr, fieldName, _, _) =>
          inner(expr) match {
            case Vector((name, wdlType, _)) =>
              Vector((s"${name}.${fieldName}", wdlType, false))
            case _ =>
              throw new Exception(
                  s"Unhandled ExprGetName construction ${TUtils.prettyFormatExpr(expr)}"
              )
          }
        case other =>
          throw new Exception(s"Unhandled expression ${other}")
      }
    }
    inner(expr)
  }

  def getCallInputs(call: TAT.Call): Vector[(String, WdlTypes.T, Boolean)] = {
    // What the callee expects
    call.callee.input.flatMap {
      case (name: String, (_: WdlTypes.T, optional: Boolean)) =>
        // provided by the caller
        (call.inputs.get(name), optional) match {
          case (None, false) =>
            // A required input that will have to be provided at runtime
            Vector.empty
          case (Some(expr), false) =>
            // required input that is provided
            getExpressionInputs(expr)
          case (None, true) =>
            // a missing optional input, doesn't have to be provided
            Vector.empty
          case (Some(expr), true) =>
            // an optional input
            getExpressionInputs(expr).map {
              case (name, wdlType, _) => (name, wdlType, true)
            }
        }
    }.toVector
  }

  /**
    * Get all inputs and outputs for a block of statements.
    * @param elements the block elements
    * @return
    */
  def getInputOutputClosure(
      elements: Vector[TAT.WorkflowElement]
  ): (Map[String, (WdlTypes.T, Boolean)], Map[String, TAT.OutputDefinition]) = {
    // accumulate the inputs, outputs, and local definitions.
    //
    // start with:
    //  an empty list of inputs
    //  empty list of local definitions
    //  empty list of outputs
    elements.foldLeft(
        (Map.empty[String, (WdlTypes.T, Boolean)], Map.empty[String, TAT.OutputDefinition])
    ) {
      case ((inputs, outputs), elem) =>
        elem match {
          case decl: TAT.Declaration =>
            val newInputs = decl.expr match {
              case None => inputs
              case Some(expr) =>
                inputs ++ getExpressionInputs(expr).collect {
                  case (name, wdlType, optional)
                      if !(inputs.contains(name) || outputs.contains(name)) =>
                    name -> (wdlType, optional)
                }.toMap
            }
            val newOutputs =
              decl.expr match {
                case None => outputs
                case Some(expr) =>
                  outputs + (decl.name -> TAT.OutputDefinition(decl.name,
                                                               decl.wdlType,
                                                               expr,
                                                               decl.loc))
              }
            (newInputs, newOutputs)
          case call: TAT.Call =>
            val newInputs = inputs ++ Utils
              .getCallInputs(call)
              .collect {
                case (name, wdlType, optional)
                    if !(inputs.contains(name) || outputs.contains(name)) =>
                  name -> (wdlType, optional)
              }
              .toMap
            val newOutputs = outputs ++ call.callee.output.map {
              case (name, wdlType) =>
                val fqn = s"${call.actualName}.${name}"
                fqn -> TAT.OutputDefinition(fqn,
                                            wdlType,
                                            TAT.ExprIdentifier(fqn, wdlType, call.loc),
                                            call.loc)
            }
            (newInputs, newOutputs)
          case cond: TAT.Conditional =>
            // recurse into body of conditional
            val (subBlockInputs, subBlockOutputs) = getInputOutputClosure(cond.body)
            val exprInputs = getExpressionInputs(cond.expr).collect {
              case (name, wdlType, optional)
                  if !(inputs.contains(name) || subBlockInputs.contains(name) || outputs
                    .contains(name)) =>
                name -> (wdlType, optional)
            }
            val newInputs = inputs ++ subBlockInputs ++ exprInputs
            // make outputs optional
            val newOutputs = outputs ++ subBlockOutputs.values.map {
              case TAT.OutputDefinition(name, wdlType, expr, loc) =>
                name -> TAT.OutputDefinition(name, TUtils.ensureOptional(wdlType), expr, loc)
            }
            (newInputs, newOutputs)
          case scatter: TAT.Scatter =>
            // recurse into body of the scatter
            val (subBlockInputs, subBlockOutputs) = getInputOutputClosure(scatter.body)
            val exprInputs = getExpressionInputs(scatter.expr).collect {
              case (name, wdlType, optional)
                  if !(inputs.contains(name) || subBlockInputs.contains(name) || outputs
                    .contains(name)) =>
                name -> (wdlType, optional)
            }
            val newInputs = inputs ++ subBlockInputs ++ exprInputs
            // make outputs arrays
            val newOutputs = outputs ++ subBlockOutputs.values.map {
              case TAT.OutputDefinition(name, wdlType, expr, loc) =>
                name -> TAT.OutputDefinition(name,
                                             WdlTypes.T_Array(wdlType, nonEmpty = false),
                                             expr,
                                             loc)
            }
            // remove the collection iteration variable
            (newInputs - scatter.identifier, newOutputs - scatter.identifier)
        }
    }
  }

  /**
    * We are building an applet for the output section of a workflow. The outputs have
    * expressions, and we need to figure out which variables they refer to. This will
    * allow the calculations to proceeed inside a stand alone applet.
    * @param outputs output definitions
    * @return
    */
  def getOutputClosure(outputs: Vector[TAT.OutputDefinition]): Map[String, WdlTypes.T] = {
    // create inputs from all the expressions that go into outputs
    outputs
      .flatMap {
        case TAT.OutputDefinition(_, _, expr, _) => Vector(expr)
      }
      .flatMap(e => getExpressionInputs(e, withMember = false))
      .foldLeft(Map.empty[String, WdlTypes.T]) {
        case (accu, (name, wdlType, _)) =>
          accu + (name -> wdlType)
      }
  }

  def prettyFormat(element: TAT.WorkflowElement, indent: String = "    "): String = {
    element match {
      case TAT.Scatter(varName, expr, body, _) =>
        val collection = TUtils.prettyFormatExpr(expr)
        val innerBlock = body
          .map { innerElement =>
            prettyFormat(innerElement, indent + "  ")
          }
          .mkString("\n")
        s"""|${indent}scatter (${varName} in ${collection}) {
            |${innerBlock}
            |${indent}}
            |""".stripMargin

      case TAT.Conditional(expr, body, _) =>
        val innerBlock =
          body
            .map { innerElement =>
              prettyFormat(innerElement, indent + "  ")
            }
            .mkString("\n")
        s"""|${indent}if (${TUtils.prettyFormatExpr(expr)}) {
            |${innerBlock}
            |${indent}}
            |""".stripMargin

      case call: TAT.Call =>
        val inputNames = call.inputs
          .map {
            case (key, expr) =>
              s"${key} = ${TUtils.prettyFormatExpr(expr)}"
          }
          .mkString(",")
        val inputs =
          if (inputNames.isEmpty) ""
          else s"{ input: ${inputNames} }"
        call.alias match {
          case None =>
            s"${indent}call ${call.fullyQualifiedName} ${inputs}"
          case Some(al) =>
            s"${indent}call ${call.fullyQualifiedName} as ${al} ${inputs}"
        }

      case TAT.Declaration(_, wdlType, None, _) =>
        s"${indent} ${TUtils.prettyFormatType(wdlType)}"
      case TAT.Declaration(_, wdlType, Some(expr), _) =>
        s"${indent} ${TUtils.prettyFormatType(wdlType)} = ${TUtils.prettyFormatExpr(expr)}"
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
