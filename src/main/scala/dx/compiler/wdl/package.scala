package dx.compiler.wdl

import java.nio.file.Path

import dx.api.DxApi
import dx.compiler.{ApplicationSource, DocumentSource, WorkflowSource}
import dx.compiler.ir.Parameter.Attribute
import dx.compiler.ir.{Extras, LanguageSupport, LanguageSupportFactory, Parameter, Type, Value}
import dx.compiler.ir.Type._
import dx.compiler.ir.Value._
import dx.core.languages.Language
import dx.core.languages.Language.Language
import dx.core.languages.wdl.WdlVarLinksConverter
import wdlTools.eval.WdlValues._
import wdlTools.syntax.{Parsers, SourceLocation, WdlVersion}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.types.WdlTypes._
import wdlTools.types.{TypeCheckingRegime, TypedAbstractSyntax => TAT}
import wdlTools.util.{Adjuncts, FileSourceResolver, Logger}

case class WdlLanguageSupport(wdlVersion: WdlVersion,
                              extrasPath: Option[Path],
                              regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                              fileResolver: FileSourceResolver = FileSourceResolver.get,
                              dxApi: DxApi = DxApi.get,
                              logger: Logger = Logger.get)
    extends LanguageSupport[V] {
  lazy val language: Language = Language.fromWdlVersion(wdlVersion)

  override lazy val getExtras: Option[Extras[V]] = extrasPath.map { path =>
    WdlExtrasParser().parse(path)
  }

  override def getDxNativeInterface: WdlDxNativeInterface = {
    wdlVersion match {
      case WdlVersion.Draft_2 =>
        Logger.get.warning("Upgrading draft-2 input to verion 1.0")
        WdlDxNativeInterface(WdlVersion.V1,
                             fileResolver = fileResolver,
                             dxApi = dxApi,
                             logger = logger)
      case WdlVersion.V1 =>
        WdlDxNativeInterface(WdlVersion.V1)
      case _ =>
        throw new Exception(s"DxNI not supported for WDL version ${wdlVersion}")
    }
  }

  override def getTranslator: WdlTranslator = {
    WdlTranslator(getExtras, regime, fileResolver, dxApi, logger)
  }
}

case class WdlLanguageSupportFactory(fileResolver: FileSourceResolver = FileSourceResolver.get,
                                     regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                                     dxApi: DxApi = DxApi.get,
                                     logger: Logger = Logger.get)
    extends LanguageSupportFactory[V] {

  override def create(language: Language, extrasPath: Option[Path]): Option[LanguageSupport[V]] = {
    try {
      val wdlVersion = Language.toWdlVersion(language)
      Some(WdlLanguageSupport(wdlVersion, extrasPath, regime, fileResolver, dxApi, logger))
    } catch {
      case _: Throwable => None
    }
  }

  override def create(sourceFile: Path, extrasPath: Option[Path]): Option[LanguageSupport[V]] = {
    try {
      val fileSource = fileResolver.fromPath(sourceFile)
      try {
        val parsers = Parsers(followImports = false, fileResolver)
        val wdlVersion = parsers.getWdlVersion(fileSource)
        Some(WdlLanguageSupport(wdlVersion, extrasPath, regime, fileResolver, dxApi, logger))
      } catch {
        case _: Throwable => None
      }
    }
  }
}

case class WdlBundle(version: WdlVersion,
                     primaryCallable: Option[TAT.Callable],
                     tasks: Map[String, TAT.Task],
                     workflows: Map[String, TAT.Workflow],
                     callableNames: Set[String],
                     sources: Map[String, TAT.Document],
                     adjunctFiles: Map[String, Vector[Adjuncts.AdjunctFile]])

// Compile time representation of a variable. Used also as
// an applet argument.
//
// The fullyQualifiedName could contains dots. However dx does not allow
// dots in applet/workflow arugment names, this requires some kind
// of transform.
//
// The attributes are used to encode DNAx applet input/output
// specification fields, such as {help, suggestions, patterns}.
//
case class WdlParameter(
    name: String,
    dxType: Type,
    optional: Boolean = false,
    defaultValue: Option[Value] = None,
    attrs: Vector[Attribute] = Vector.empty
) extends Parameter {
  // dx does not allow dots in variable names, so we
  // convert them to underscores.
  //
  // TODO: check for collisions that are created this way.
  def dxName: String = {
    val nameNoDots = WdlVarLinksConverter.transformVarName(name)
    assert(!nameNoDots.contains("."))
    nameNoDots
  }
}

// wrappers around WDL-specific document elements, used by Native
// when generating apps/workflows
case class WdlDocumentSource(doc: TAT.Document) extends DocumentSource
case class WdlApplicationSource(task: TAT.Task) extends ApplicationSource
case class WdlWorkflowSource(workflow: TAT.Workflow) extends WorkflowSource

object Utils {
  def wdlToIRType(wdlType: T, optional: Boolean = false): (Type, Boolean) = {
    wdlType match {
      case T_Optional(t) => wdlToIRType(t, optional = true)
      case nonOptType =>
        val t = nonOptType match {
          case T_Boolean   => TBoolean
          case T_Int       => TInt
          case T_Float     => TFloat
          case T_String    => TString
          case T_File      => TFile
          case T_Directory => TDirectory
          case T_Array(t, _) =>
            val innerType = wdlToIRType(t) match {
              case (t, false) => t
              case (_, true) =>
                throw new Exception(s"Nested types cannot be optional")
            }
            TArray(innerType)
          case T_Map(T_String, _) => THash
          case T_Object           => THash
          case _ =>
            throw new Exception(s"Cannot convert WDL type ${wdlType} to IR")
        }
        (t, optional)
    }
  }

  def irToWdlType(irType: Type, optional: Boolean): T = {
    val wdlType = irType match {
      case TBoolean   => T_Boolean
      case TInt       => T_Int
      case TFloat     => T_Float
      case TString    => T_String
      case TFile      => T_File
      case TDirectory => T_Directory
      case TArray(t) =>
        T_Array(irToWdlType(t, optional = false), nonEmpty = false)
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

  def wdlToIRValue(wdlValue: V): Value = {
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
        VArray(array.map(v => wdlToIRValue(v)))
      case V_Object(m) =>
        VHash(m.map {
          case (key, value) => key -> wdlToIRValue(value)
        })
      case V_Map(m) =>
        VHash(m.map {
          case (V_String(key), value) =>
            key -> wdlToIRValue(value)
          case _ =>
            throw new Exception(s"WDL Map value must have String key ${m}")
        })
      case _ =>
        throw new Exception(s"Invalid WDL value ${wdlValue})")
    }
  }

  def wdlToIRValue(wdlValue: V, wdlType: T): Value = {
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
        VArray(array.map(v => wdlToIRValue(v, t)))
      case (T_Object, o: V_Object) => wdlToIRValue(o)
      case (T_Map(T_String, t), V_Map(m)) =>
        VHash(m.map {
          case (V_String(key), value) =>
            key -> wdlToIRValue(value, t)
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
        val a = array.map(irValueToExpr)
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
          case (key, value) => TAT.ValueString(key, T_String, loc) -> irValueToExpr(value)
        }
        TAT.ExprObject(m, T_Object, loc)
      case _ =>
        throw new Exception(s"Cannot convert IR value ${value} to WDL")
    }
  }
}
