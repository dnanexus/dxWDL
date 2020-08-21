package dx.compiler.wdl

import java.nio.file.Path

import dx.api.DxApi
import dx.compiler.{ApplicationSource, DocumentSource, WorkflowSource}
import dx.compiler.ir.Parameter.Attribute
import dx.compiler.ir.{Extras, LanguageSupport, LanguageSupportFactory, Parameter}
import dx.core.ir.Type._
import dx.core.ir.Value._
import dx.core.ir.{Type, Value}
import dx.core.languages.{Language, wdl}
import dx.core.languages.Language.Language
import dx.core.languages.wdl.{Utils, WdlDxLinkSerde}
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
    val nameNoDots = WdlDxLinkSerde.encodeDots(name)
    assert(!nameNoDots.contains("."))
    nameNoDots
  }
}

// wrappers around WDL-specific document elements, used by Native
// when generating apps/workflows
case class WdlDocumentSource(doc: TAT.Document) extends DocumentSource
case class WdlApplicationSource(task: TAT.Task) extends ApplicationSource
case class WdlWorkflowSource(workflow: TAT.Workflow) extends WorkflowSource
