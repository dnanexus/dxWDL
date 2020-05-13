package wdlTools.syntax.draft_2

import java.net.URL
import java.nio.ByteBuffer

import org.antlr.v4.runtime.{CodePointBuffer, CodePointCharStream, CommonTokenStream}
import org.openwdl.wdl.parser.draft_2.{WdlDraft2Lexer, WdlDraft2Parser}
import wdlTools.syntax.Antlr4Util.{Grammar, ParseTreeListenerFactory}
import wdlTools.syntax.WdlVersion
import wdlTools.util.{Options, SourceCode}

case class WdlDraft2Grammar(override val lexer: WdlDraft2Lexer,
                            override val parser: WdlDraft2Parser,
                            override val listenerFactories: Vector[ParseTreeListenerFactory],
                            override val docSourceUrl: Option[URL] = None,
                            override val docSource: String,
                            override val opts: Options)
    extends Grammar(WdlVersion.Draft_2,
                    lexer,
                    parser,
                    listenerFactories,
                    docSourceUrl,
                    docSource,
                    opts)

object WdlDraft2Grammar {
  def newInstance(sourceCode: SourceCode,
                  listenerFactories: Vector[ParseTreeListenerFactory],
                  opts: Options): WdlDraft2Grammar = {
    newInstance(sourceCode.toString, listenerFactories, Some(sourceCode.url), opts)
  }

  def newInstance(text: String,
                  listenerFactories: Vector[ParseTreeListenerFactory],
                  docSourceUrl: Option[URL] = None,
                  opts: Options): WdlDraft2Grammar = {
    val codePointBuffer: CodePointBuffer =
      CodePointBuffer.withBytes(ByteBuffer.wrap(text.getBytes()))
    val charStream = CodePointCharStream.fromBuffer(codePointBuffer)
    val lexer = new WdlDraft2Lexer(charStream)
    val parser = new WdlDraft2Parser(new CommonTokenStream(lexer))
    new WdlDraft2Grammar(lexer, parser, listenerFactories, docSourceUrl, text, opts)
  }
}
