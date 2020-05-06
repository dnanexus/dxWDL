package wdlTools.syntax

import java.net.URL
import java.nio.ByteBuffer

import org.antlr.v4.runtime.tree.TerminalNode
import org.antlr.v4.runtime.{
  BaseErrorListener,
  BufferedTokenStream,
  CharStream,
  CodePointBuffer,
  CodePointCharStream,
  CommonTokenStream,
  Lexer,
  Parser,
  ParserRuleContext,
  RecognitionException,
  Recognizer,
  Token
}
import scala.jdk.CollectionConverters._
import wdlTools.syntax
import wdlTools.util.{Options, SourceCode, Verbosity}

import scala.collection.mutable

object Antlr4Util {
  def getTextSource(startToken: Token, maybeStopToken: Option[Token] = None): TextSource = {
    val stopToken = maybeStopToken.getOrElse(startToken)
    syntax.TextSource(
        line = startToken.getLine,
        col = startToken.getCharPositionInLine,
        endLine = stopToken.getLine,
        endCol = stopToken.getCharPositionInLine + stopToken.getText.length
    )
  }

  def getTextSource(ctx: ParserRuleContext): TextSource = {
    getTextSource(ctx.getStart, Some(ctx.getStop))
  }

  def getTextSource(symbol: TerminalNode): TextSource = {
    getTextSource(symbol.getSymbol, None)
  }

  // Based on Patrick Magee's error handling code (https://github.com/patmagee/wdl4j)
  //
  case class WdlAggregatingErrorListener(docSourceUrl: Option[URL]) extends BaseErrorListener {

    private var errors = Vector.empty[SyntaxError]

    // This is called by the antlr grammar during parsing.
    // We collect these errors in a list, and report collectively
    // when parsing is complete.
    override def syntaxError(recognizer: Recognizer[_, _],
                             offendingSymbol: Any,
                             line: Int,
                             charPositionInLine: Int,
                             msg: String,
                             e: RecognitionException): Unit = {
      val symbolText =
        offendingSymbol match {
          case null => ""
          case tok: Token =>
            tok.getText
          case _ =>
            offendingSymbol.toString
        }
      val err = SyntaxError(docSourceUrl, symbolText, line, charPositionInLine, msg)
      errors = errors :+ err
    }

    def getErrors: Vector[SyntaxError] = errors

    def hasErrors: Boolean = errors.nonEmpty
  }

  case class CommentListener(tokenStream: BufferedTokenStream,
                             channelIndex: Int,
                             docSourceUrl: Option[URL] = None,
                             comments: mutable.Map[Int, Comment] = mutable.HashMap.empty)
      extends AllParseTreeListener {
    def addComments(tokens: Vector[Token]): Unit = {
      tokens.foreach { tok =>
        val source = Antlr4Util.getTextSource(tok, None)
        if (comments.contains(source.line)) {
          // TODO: should this be an error?
        } else {
          comments(source.line) = Comment(tok.getText, source)
        }
      }
    }

    override def exitEveryRule(ctx: ParserRuleContext): Unit = {
      // full-line comments
      if (ctx.getStart != null && ctx.getStart.getTokenIndex >= 0) {
        val beforeComments =
          tokenStream.getHiddenTokensToLeft(ctx.getStart.getTokenIndex, channelIndex)
        if (beforeComments != null) {
          addComments(beforeComments.asScala.toVector)
        }
      }
      // line-end comments
      if (ctx.getStop != null && ctx.getStop.getTokenIndex >= 0) {
        val afterComments =
          tokenStream.getHiddenTokensToRight(ctx.getStop.getTokenIndex, channelIndex)
        if (afterComments != null) {
          addComments(afterComments.asScala.toVector)
        }
      }
    }
  }

  case class Grammar[L <: Lexer, P <: Parser](lexer: L,
                                              parser: P,
                                              errListener: WdlAggregatingErrorListener,
                                              comments: mutable.Map[Int, Comment],
                                              docSourceUrl: Option[URL] = None,
                                              opts: Options) {
    def verify(): Unit = {
      // check if any errors were found
      val errors: Vector[SyntaxError] = errListener.getErrors
      if (errors.nonEmpty) {
        if (opts.verbosity > Verbosity.Quiet) {
          for (err <- errors) {
            System.out.println(err)
          }
        }
        throw new SyntaxException(errors)
      }
    }
  }

  abstract class GrammarFactory[L <: Lexer, P <: Parser](opts: Options,
                                                         commentChannelName: String = "COMMENTS") {
    def createGrammar(sourceCode: SourceCode): Grammar[L, P] = {
      createGrammar(sourceCode.toString, Some(sourceCode.url))
    }

    def createGrammar(inp: String, docSourceUrl: Option[URL] = None): Grammar[L, P] = {
      val codePointBuffer: CodePointBuffer =
        CodePointBuffer.withBytes(ByteBuffer.wrap(inp.getBytes()))
      val lexer: L = createLexer(CodePointCharStream.fromBuffer(codePointBuffer))
      val parser: P = createParser(new CommonTokenStream(lexer))

      // setting up our own error handling
      val errListener = WdlAggregatingErrorListener(docSourceUrl)
      lexer.removeErrorListeners()
      lexer.addErrorListener(errListener)
      parser.removeErrorListeners()
      parser.addErrorListener(errListener)

      if (opts.antlr4Trace) {
        parser.setTrace(true)
      }

      val comments: mutable.Map[Int, Comment] = mutable.HashMap.empty

      parser.addParseListener(
          CommentListener(
              parser.getTokenStream.asInstanceOf[BufferedTokenStream],
              lexer.getChannelNames.indexOf(commentChannelName),
              docSourceUrl,
              comments
          )
      )

      Grammar(lexer, parser, errListener, comments, docSourceUrl, opts)
    }

    def createLexer(charStream: CharStream): L

    def createParser(tokenStream: CommonTokenStream): P
  }
}
