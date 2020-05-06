package wdlTools.syntax.draft_2

import org.antlr.v4.runtime.{CharStream, CommonTokenStream}
import org.openwdl.wdl.parser.draft_2.{WdlDraft2Lexer, WdlDraft2Parser}
import wdlTools.syntax.Antlr4Util.GrammarFactory
import wdlTools.util.Options

case class WdlDraft2GrammarFactory(opts: Options)
    extends GrammarFactory[WdlDraft2Lexer, WdlDraft2Parser](opts) {
  override def createLexer(charStream: CharStream): WdlDraft2Lexer = {
    new WdlDraft2Lexer(charStream)
  }

  override def createParser(tokenStream: CommonTokenStream): WdlDraft2Parser = {
    new WdlDraft2Parser(tokenStream)
  }
}
