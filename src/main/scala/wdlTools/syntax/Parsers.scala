package wdlTools.syntax

import java.net.URL
import java.nio.file.Path

import wdlTools.syntax.AbstractSyntax.Document
import wdlTools.util.{Options, SourceCode, Util}

import scala.collection.mutable

case class Parsers(opts: Options = Options()) {
  private lazy val parsers: Map[WdlVersion, WdlParser] = Map(
      WdlVersion.Draft_2 -> draft_2.ParseAll(opts),
      WdlVersion.V1 -> v1.ParseAll(opts)
  )

  def getParser(url: URL): WdlParser = {
    getParser(SourceCode.loadFrom(url))
  }

  def getParser(sourceCode: SourceCode): WdlParser = {
    WdlVersion.All.foreach { ver =>
      val parser = parsers(ver)
      if (parser.canParse(sourceCode)) {
        return parser
      }
    }
    throw new Exception(s"No parser is able to parse document ${sourceCode.url}")
  }

  def getParser(wdlVersion: WdlVersion): WdlParser = {
    parsers(wdlVersion)
  }

  def parseDocument(path: Path): Document = {
    parseDocument(Util.pathToUrl(path))
  }

  def parseDocument(url: URL): Document = {
    val sourceCode = SourceCode.loadFrom(url)
    val parser = getParser(sourceCode)
    parser.parseDocument(sourceCode)
  }

  def getDocumentWalker[T](
      url: URL,
      results: mutable.Map[URL, T] = mutable.HashMap.empty[URL, T]
  ): DocumentWalker[T] = {
    val sourceCode = SourceCode.loadFrom(url)
    val parser = getParser(sourceCode)
    parser.Walker(sourceCode, results)
  }
}
