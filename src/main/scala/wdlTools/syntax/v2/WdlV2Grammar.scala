package wdlTools.syntax.v2

import java.net.URL
import java.nio.ByteBuffer

import org.antlr.v4.runtime.{CodePointBuffer, CodePointCharStream, CommonTokenStream}
import org.openwdl.wdl.parser.v2.{WdlV2Lexer, WdlV2Parser}
import wdlTools.syntax.Antlr4Util.{Grammar, ParseTreeListenerFactory}
import wdlTools.syntax.WdlVersion
import wdlTools.util.{Options, SourceCode}

case class WdlV2Grammar(override val lexer: WdlV2Lexer,
                        override val parser: WdlV2Parser,
                        override val listenerFactories: Vector[ParseTreeListenerFactory],
                        override val docSourceUrl: Option[URL] = None,
                        override val docSource: String,
                        override val opts: Options)
    extends Grammar(WdlVersion.V2, lexer, parser, listenerFactories, docSourceUrl, docSource, opts)

object WdlV2Grammar {
  def newInstance(sourceCode: SourceCode,
                  listenerFactories: Vector[ParseTreeListenerFactory],
                  opts: Options): WdlV2Grammar = {
    newInstance(sourceCode.toString, listenerFactories, Some(sourceCode.url), opts)
  }

  def newInstance(text: String,
                  listenerFactories: Vector[ParseTreeListenerFactory],
                  docSourceUrl: Option[URL] = None,
                  opts: Options): WdlV2Grammar = {
    val codePointBuffer: CodePointBuffer =
      CodePointBuffer.withBytes(ByteBuffer.wrap(text.getBytes()))
    val charStream = CodePointCharStream.fromBuffer(codePointBuffer)
    val lexer = new WdlV2Lexer(charStream)
    val parser = new WdlV2Parser(new CommonTokenStream(lexer))
    new WdlV2Grammar(lexer, parser, listenerFactories, docSourceUrl, text, opts)
  }
}
