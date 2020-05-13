package wdlTools.syntax

import java.net.URL
import java.nio.file.Path

import wdlTools.syntax.AbstractSyntax.Document
import wdlTools.syntax.Antlr4Util.ParseTreeListenerFactory
import wdlTools.util.{Options, SourceCode, Util}

import scala.collection.mutable

case class Parsers(opts: Options = Options(),
                   listenerFactories: Vector[ParseTreeListenerFactory] = Vector.empty) {
  private lazy val parsers: Map[WdlVersion, WdlParser] = Map(
      WdlVersion.Draft_2 -> draft_2.ParseAll(opts, listenerFactories),
      WdlVersion.V1 -> v1.ParseAll(opts, listenerFactories),
      WdlVersion.V2 -> v2.ParseAll(opts, listenerFactories)
  )

  def getParser(url: URL): WdlParser = {
    getParser(SourceCode.loadFrom(url))
  }

  def getParser(sourceCode: SourceCode): WdlParser = {
    parsers.values.collectFirst {
      case parser if parser.canParse(sourceCode) => parser
    } match {
      case Some(parser) => parser
      case _            => throw new Exception(s"No parser is able to parse document ${sourceCode.url}")
    }
  }

  def getParser(wdlVersion: WdlVersion): WdlParser = {
    parsers.get(wdlVersion) match {
      case Some(parser) => parser
      case _            => throw new Exception(s"No parser defined for WdlVersion ${wdlVersion}")
    }
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
