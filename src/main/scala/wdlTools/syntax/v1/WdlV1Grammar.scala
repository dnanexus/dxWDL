package wdlTools.syntax.v1

import java.net.URL
import java.nio.ByteBuffer

import org.antlr.v4.runtime.{CodePointBuffer, CodePointCharStream, CommonTokenStream}
import org.openwdl.wdl.parser.v1.{WdlV1Lexer, WdlV1Parser}
import wdlTools.syntax.Antlr4Util.{Grammar, ParseTreeListenerFactory}
import wdlTools.syntax.WdlVersion
import wdlTools.util.{Options, SourceCode}

case class WdlV1Grammar(override val lexer: WdlV1Lexer,
                        override val parser: WdlV1Parser,
                        override val listenerFactories: Vector[ParseTreeListenerFactory],
                        override val docSourceUrl: Option[URL] = None,
                        override val docSource: String,
                        override val opts: Options)
    extends Grammar(WdlVersion.V1, lexer, parser, listenerFactories, docSourceUrl, docSource, opts)

object WdlV1Grammar {
  def newInstance(sourceCode: SourceCode,
                  listenerFactories: Vector[ParseTreeListenerFactory],
                  opts: Options): WdlV1Grammar = {
    newInstance(sourceCode.toString, listenerFactories, Some(sourceCode.url), opts)
  }

  def newInstance(text: String,
                  listenerFactories: Vector[ParseTreeListenerFactory],
                  docSourceUrl: Option[URL] = None,
                  opts: Options): WdlV1Grammar = {
    val codePointBuffer: CodePointBuffer =
      CodePointBuffer.withBytes(ByteBuffer.wrap(text.getBytes()))
    val charStream = CodePointCharStream.fromBuffer(codePointBuffer)
    val lexer = new WdlV1Lexer(charStream)
    val parser = new WdlV1Parser(new CommonTokenStream(lexer))
    new WdlV1Grammar(lexer, parser, listenerFactories, docSourceUrl, text, opts)
  }
}
