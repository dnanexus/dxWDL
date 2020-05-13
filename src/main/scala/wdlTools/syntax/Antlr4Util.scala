package wdlTools.syntax

import java.net.URL

import org.antlr.v4.runtime.tree.{ParseTreeListener, TerminalNode}
import org.antlr.v4.runtime.{
  BaseErrorListener,
  BufferedTokenStream,
  Lexer,
  Parser,
  ParserRuleContext,
  RecognitionException,
  Recognizer,
  Token
}

import scala.jdk.CollectionConverters._
import wdlTools.syntax
import wdlTools.util.{Options, Verbosity}

import scala.collection.mutable

object Antlr4Util {
  def getTextSource(startToken: Token, maybeStopToken: Option[Token] = None): TextSource = {
    // TODO: for an ending token that containing newlines, the endLine and endCol will be wrong
    val stopToken = maybeStopToken.getOrElse(startToken)
    syntax.TextSource(
        line = startToken.getLine,
        col = startToken.getCharPositionInLine,
        endLine = stopToken.getLine,
        endCol = stopToken.getCharPositionInLine + stopToken.getText.length
    )
  }

  def getTextSource(ctx: ParserRuleContext): TextSource = {
    val stop = ctx.getStop
    getTextSource(ctx.getStart, Option(stop))
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

  private case class CommentListener(tokenStream: BufferedTokenStream,
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

  trait ParseTreeListenerFactory {
    def createParseTreeListeners(grammar: Grammar): Vector[ParseTreeListener]
  }

  private case object CommentListenerFactory extends ParseTreeListenerFactory {
    override def createParseTreeListeners(grammar: Grammar): Vector[ParseTreeListener] = {
      Vector(
          CommentListener(
              grammar.parser.getTokenStream.asInstanceOf[BufferedTokenStream],
              grammar.commentChannel,
              grammar.docSourceUrl,
              grammar.comments
          )
      )
    }
  }

  private val defaultListenerFactories = Vector(CommentListenerFactory)

  class Grammar(
      val version: WdlVersion,
      val lexer: Lexer,
      val parser: Parser,
      val listenerFactories: Vector[ParseTreeListenerFactory],
      val docSourceUrl: Option[URL] = None,
      val docSource: String,
      val opts: Options,
      val comments: mutable.Map[Int, Comment] = mutable.HashMap.empty
  ) {
    val errListener: WdlAggregatingErrorListener = WdlAggregatingErrorListener(docSourceUrl)
    // setting up our own error handling
    lexer.removeErrorListeners()
    lexer.addErrorListener(errListener)
    parser.removeErrorListeners()
    parser.addErrorListener(errListener)

    if (opts.antlr4Trace) {
      parser.setTrace(true)
    }

    def getChannel(name: String): Int = {
      val channel = lexer.getChannelNames.indexOf(name)
      require(channel >= 0)
      channel
    }

    val hiddenChannel: Int = getChannel("HIDDEN")
    val commentChannel: Int = getChannel("COMMENTS")

    (defaultListenerFactories ++ listenerFactories).foreach(
        _.createParseTreeListeners(this).map(parser.addParseListener)
    )

    def getHiddenTokens(ctx: ParserRuleContext,
                        channel: Int = hiddenChannel,
                        before: Boolean = true,
                        within: Boolean = false,
                        after: Boolean = false): Vector[Token] = {

      def getTokenIndex(tok: Token): Option[Int] = {
        if (tok == null || tok.getTokenIndex < 0) {
          None
        } else {
          Some(tok.getTokenIndex)
        }
      }

      val startIdx = if (before || within) getTokenIndex(ctx.getStart) else None
      val stopIdx = if (after || within) getTokenIndex(ctx.getStop) else None

      if (startIdx.isEmpty && stopIdx.isEmpty) {
        Vector.empty
      } else {
        def tokensToSet(tokens: java.util.List[Token]): Set[Token] = {
          if (tokens == null) {
            Set.empty
          } else {
            tokens.asScala.toSet
          }
        }

        val tokenStream = parser.getTokenStream.asInstanceOf[BufferedTokenStream]
        val beforeTokens = if (before && startIdx.isDefined) {
          tokensToSet(tokenStream.getHiddenTokensToLeft(startIdx.get, channel))
        } else {
          Set.empty
        }
        val withinTokens = if (within && startIdx.isDefined && stopIdx.isDefined) {
          (startIdx.get until stopIdx.get)
            .flatMap { idx =>
              tokensToSet(tokenStream.getHiddenTokensToRight(idx, channel))
            }
            .toSet
            .filter(tok => tok.getTokenIndex >= startIdx.get && tok.getTokenIndex <= stopIdx.get)
        } else {
          Set.empty
        }
        val afterTokens = if (after && stopIdx.isDefined) {
          tokensToSet(tokenStream.getHiddenTokensToRight(stopIdx.get, channel))
        } else {
          Set.empty
        }
        (beforeTokens ++ withinTokens ++ afterTokens).toVector.sortWith((left, right) =>
          left.getTokenIndex < right.getTokenIndex
        )
      }
    }

    def beforeParse(): Unit = {
      // call the enter() method on any `AllParseTreeListener`s
      parser.getParseListeners.asScala.foreach {
        case l: AllParseTreeListener => l.enter()
        case _                       => ()
      }
    }

    def afterParse(): Unit = {
      // call the exit() method on any `AllParseTreeListener`s
      parser.getParseListeners.asScala.foreach {
        case l: AllParseTreeListener => l.exit()
        case _                       => ()
      }

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

    def visitDocument[T <: ParserRuleContext, E](
        ctx: T,
        visitor: (T, mutable.Map[Int, Comment]) => E
    ): E = {
      if (ctx == null) {
        throw new Exception("WDL file does not contain a valid document")
      }
      beforeParse()
      val result = visitor(ctx, comments)
      afterParse()
      result
    }

    def visitFragment[T <: ParserRuleContext, E](
        ctx: T,
        visitor: T => E
    ): E = {
      if (ctx == null) {
        throw new Exception("Not a valid fragment")
      }
      beforeParse()
      val result = visitor(ctx)
      afterParse()
      result

    }
  }
}
