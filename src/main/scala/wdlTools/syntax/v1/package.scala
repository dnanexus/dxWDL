package wdlTools.syntax.v1

import org.antlr.v4.runtime.{CharStream, CommonTokenStream}
import org.openwdl.wdl.parser.v1.{WdlV1Lexer, WdlV1Parser}
import wdlTools.syntax.Antlr4Util.GrammarFactory
import wdlTools.util.Options

case class WdlV1GrammarFactory(opts: Options)
    extends GrammarFactory[WdlV1Lexer, WdlV1Parser](opts) {
  override def createLexer(charStream: CharStream): WdlV1Lexer = {
    new WdlV1Lexer(charStream)
  }

  override def createParser(tokenStream: CommonTokenStream): WdlV1Parser = {
    new WdlV1Parser(tokenStream)
  }
}
