package dx.core.languages.wdl

import java.nio.file.Path

import dx.api.DxApi
import dx.core.languages.wdl.WdlUtils.parseSource
import wdlTools.generators.code.WdlGenerator
import wdlTools.syntax.{Parsers, WdlParser, WdlVersion}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.types.{TypeCheckingRegime, WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{Bindings, FileNode, FileSourceResolver, Logger, StringFileNode}

case class VersionSupport(version: WdlVersion,
                          fileResolver: FileSourceResolver = FileSourceResolver.get,
                          regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                          dxApi: DxApi = DxApi.get,
                          logger: Logger = Logger.get,
                          wdlParser: Option[WdlParser] = None) {
  private lazy val parser = wdlParser.getOrElse(
      Parsers(followImports = true, fileResolver = fileResolver, logger = logger).getParser(version)
  )

  lazy val generatedVersion: WdlVersion = {
    version match {
      case WdlVersion.Draft_2 =>
        logger.warning("Upgrading draft-2 input to verion 1.0")
        WdlVersion.V1
      case _ => version
    }
  }

  lazy val codeGenerator: WdlGenerator = WdlGenerator(Some(generatedVersion))

  def parse(sourceCode: FileNode): (TAT.Document, Bindings[String, WdlTypes.T_Struct]) = {
    WdlUtils.parseAndCheckSource(sourceCode, parser, fileResolver, regime, logger)
  }

  def parse(src: String): (TAT.Document, Bindings[String, WdlTypes.T_Struct]) = {
    parse(StringFileNode(src))
  }

  def parse(path: Path): (TAT.Document, Bindings[String, WdlTypes.T_Struct]) = {
    parse(fileResolver.fromPath(path))
  }

  def validateWdlCode(wdlWfSource: String): Unit = {
    val (tDoc, _) = parse(wdlWfSource)
    // Check that this is the correct language version
    if (tDoc.version.value != version) {
      throw new Exception(
          s"document has wrong version ${tDoc.version.value}, should be ${version}"
      )
    }
  }

  def generateDocument(doc: TAT.Document): String = {
    val sourceString = codeGenerator.generateDocument(doc).mkString("\n")
    Logger.get.ignore(WdlUtils.parseAndCheckSourceString(sourceString))
    sourceString
  }

  def generateElement(element: TAT.Element): String = {
    val sourceString = codeGenerator.generateElement(element).mkString("\n")
    // add the version statement so we can try to parse it
    val standAloneString = s"version ${generatedVersion.name}\n\n${sourceString}"
    // we only do parsing, not type checking here, since the element may
    // not be stand-alone
    val parser = Parsers.default.getParser(generatedVersion)
    Logger.get.ignore(parseSource(StringFileNode(standAloneString), parser))
    standAloneString
  }
}

object VersionSupport {
  def fromSource(
      sourceCode: FileNode,
      fileResolver: FileSourceResolver = FileSourceResolver.get,
      regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
      dxApi: DxApi = DxApi.get,
      logger: Logger = Logger.get
  ): (TAT.Document, Bindings[String, WdlTypes.T_Struct], VersionSupport) = {
    val parser = Parsers(followImports = true, fileResolver = fileResolver, logger = logger)
      .getParser(sourceCode)
    val (doc, typeAliases) =
      WdlUtils.parseAndCheckSource(sourceCode, parser, fileResolver, regime, logger)
    val versionSupport =
      VersionSupport(doc.version.value, fileResolver, regime, dxApi, logger, Some(parser))
    (doc, typeAliases, versionSupport)
  }

  def fromSourceFile(
      sourceFile: Path,
      fileResolver: FileSourceResolver = FileSourceResolver.get,
      regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
      dxApi: DxApi = DxApi.get,
      logger: Logger = Logger.get
  ): (TAT.Document, Bindings[String, WdlTypes.T_Struct], VersionSupport) = {
    fromSource(fileResolver.fromPath(sourceFile), fileResolver, regime, dxApi, logger)
  }

  def fromSourceString(
      sourceCode: String,
      fileResolver: FileSourceResolver = FileSourceResolver.get,
      regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
      dxApi: DxApi = DxApi.get,
      logger: Logger = Logger.get
  ): (TAT.Document, Bindings[String, WdlTypes.T_Struct], VersionSupport) = {
    fromSource(StringFileNode(sourceCode), fileResolver, regime, dxApi, logger)
  }
}
