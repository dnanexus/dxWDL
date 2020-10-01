package dx.core.languages.wdl

import java.nio.file.Path

import dx.api.DxApi
import wdlTools.generators.code.WdlV1Generator
import wdlTools.syntax.{Parsers, WdlParser, WdlVersion}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.types.{TypeCheckingRegime, WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{DefaultBindings, FileNode, FileSourceResolver, Logger, StringFileNode}

case class VersionSupport(version: WdlVersion,
                          fileResolver: FileSourceResolver = FileSourceResolver.get,
                          regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                          dxApi: DxApi = DxApi.get,
                          logger: Logger = Logger.get,
                          wdlParser: Option[WdlParser] = None) {
  private lazy val parser = wdlParser.getOrElse(
      Parsers(followImports = true, fileResolver = fileResolver, logger = logger).getParser(version)
  )

  lazy val codeGenerator: WdlV1Generator = {
    version match {
      case WdlVersion.Draft_2 =>
        logger.warning("Upgrading draft-2 input to verion 1.0")
        WdlV1Generator()
      case WdlVersion.V1 =>
        WdlV1Generator()
      case WdlVersion.V2 =>
        throw new RuntimeException("WDL 2.0 is not yet supported")
    }
  }

  def parse(sourceCode: FileNode): (TAT.Document, DefaultBindings[WdlTypes.T_Struct]) = {
    WdlUtils.parseSource(parser, sourceCode, fileResolver, regime, logger)
  }

  def parse(src: String): (TAT.Document, DefaultBindings[WdlTypes.T_Struct]) = {
    parse(StringFileNode(src))
  }

  def parse(path: Path): (TAT.Document, DefaultBindings[WdlTypes.T_Struct]) = {
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
}

object VersionSupport {
  def fromSource(
      source: Path,
      fileResolver: FileSourceResolver = FileSourceResolver.get,
      regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
      dxApi: DxApi = DxApi.get,
      logger: Logger = Logger.get
  ): (TAT.Document, DefaultBindings[WdlTypes.T_Struct], VersionSupport) = {
    val sourceCode = fileResolver.fromPath(source)
    val parser = Parsers(followImports = true, fileResolver = fileResolver, logger = logger)
      .getParser(sourceCode)
    val (doc, typeAliases) = WdlUtils.parseSource(parser, sourceCode, fileResolver, regime, logger)
    val versionSupport =
      VersionSupport(doc.version.value, fileResolver, regime, dxApi, logger, Some(parser))
    (doc, typeAliases, versionSupport)
  }
}
