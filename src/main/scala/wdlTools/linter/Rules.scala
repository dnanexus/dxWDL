package wdlTools.linter

import java.net.URL

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import wdlTools.linter.Severity.Severity
import wdlTools.syntax
import wdlTools.syntax.AbstractSyntax._
import wdlTools.syntax.{ASTVisitor, AllParseTreeListener, Antlr4Util, TextSource, WdlVersion}
import wdlTools.syntax.Antlr4Util.Grammar
import wdlTools.types

import scala.collection.mutable

// TODO: shamelessly copy rules from
//  * miniwdl: https://github.com/chanzuckerberg/miniwdl/blob/master/WDL/Lint.py
//  * winstanley: https://github.com/broadinstitute/winstanley
object Rules {
  // Parser-level rules
  // These are mostly to check things related to whitespace, which is not
  // accessible from the AST

  // ideally we could provide the id as a class annotation, but dealing with annotations
  // in Scala is currently horrendous - for how it would be done, see
  // https://stackoverflow.com/questions/23046958/accessing-an-annotation-value-in-scala

  type LinterParserRuleApplySig = (
      String,
      Severity,
      mutable.Buffer[LintEvent],
      Grammar
  ) => LinterParserRule

  class LinterParserRule(id: String,
                         severity: Severity,
                         docSourceUrl: Option[URL],
                         events: mutable.Buffer[LintEvent])
      extends AllParseTreeListener {
    protected def addEventFromTokens(tok: Token,
                                     stopToken: Option[Token] = None,
                                     message: Option[String] = None): Unit = {
      addEvent(Antlr4Util.getTextSource(tok, stopToken), message)
    }

    protected def addEvent(textSource: TextSource, message: Option[String] = None): Unit = {
      events.append(LintEvent(id, severity, textSource, docSourceUrl, message))
    }
  }

  abstract class HiddenTokensLinterParserRule(id: String,
                                              severity: Severity,
                                              events: mutable.Buffer[LintEvent],
                                              grammar: Grammar)
      extends LinterParserRule(id, severity, grammar.docSourceUrl, events) {
    private val tokenIndexes: mutable.Set[Int] = mutable.HashSet.empty

    protected def addEvent(tok: Token): Unit = {
      val idx = tok.getTokenIndex
      if (!tokenIndexes.contains(idx)) {
        // properly construct TextSource to deal with newlines
        val text = tok.getText
        val lines = text.linesWithSeparators.toVector
        val textSource = syntax.TextSource(
            line = tok.getLine,
            col = tok.getCharPositionInLine,
            endLine = tok.getLine + math.max(lines.size, 1) - 1,
            endCol = if (lines.size <= 1) {
              tok.getCharPositionInLine + text.length
            } else {
              lines.last.length + 1
            }
        )
        addEvent(textSource)
        tokenIndexes.add(idx)
      }
    }
  }

  abstract class EveryRuleHiddenTokensLinterParserRule(id: String,
                                                       severity: Severity,
                                                       events: mutable.Buffer[LintEvent],
                                                       grammar: Grammar)
      extends HiddenTokensLinterParserRule(id, severity, events, grammar) {
    override def exitEveryRule(ctx: ParserRuleContext): Unit = {
      grammar
        .getHiddenTokens(ctx, within = true)
        .filter(isViolation)
        .foreach(addEvent)
    }

    def isViolation(token: Token): Boolean
  }

  case class WhitespaceTabsRule(id: String,
                                severity: Severity,
                                events: mutable.Buffer[LintEvent],
                                grammar: Grammar)
      extends EveryRuleHiddenTokensLinterParserRule(id, severity, events, grammar) {
    override def isViolation(token: Token): Boolean = {
      token.getText.contains("\t")
    }
  }

  case class OddIndentRule(id: String,
                           severity: Severity,
                           events: mutable.Buffer[LintEvent],
                           grammar: Grammar)
      extends EveryRuleHiddenTokensLinterParserRule(id, severity, events, grammar) {
    private val indentRegex = "\n+([ \t]+)".r

    override def isViolation(token: Token): Boolean = {
      // find any tokens that contain a newline followed by an odd number of spaces
      indentRegex.findAllMatchIn(token.getText).exists { ws =>
        ws.group(1)
          .map {
            case ' '  => 1
            case '\t' => 2
          }
          .sum % 2 == 1
      }
    }
  }

  case class MultipleBlankLineRule(id: String,
                                   severity: Severity,
                                   events: mutable.Buffer[LintEvent],
                                   grammar: Grammar)
      extends EveryRuleHiddenTokensLinterParserRule(id, severity, events, grammar) {
    private val multipleReturns = "(\n\\s*){3,}".r

    override def isViolation(token: Token): Boolean = {
      multipleReturns.findFirstIn(token.getText).isDefined
    }
  }

  case class TopLevelIndentRule(id: String,
                                severity: Severity,
                                events: mutable.Buffer[LintEvent],
                                grammar: Grammar)
      extends HiddenTokensLinterParserRule(id, severity, events, grammar) {
    private val endWhitespaceRegex = "\\s$".r

    def checkIndent(ctx: ParserRuleContext): Unit = {
      grammar
        .getHiddenTokens(ctx)
        .collectFirst {
          case tok
              if tok.getTokenIndex == ctx.getStart.getTokenIndex - 1 &&
                endWhitespaceRegex.findFirstIn(tok.getText).isDefined =>
            tok
        }
        .foreach(addEvent)
    }

    override def enterVersion(ctx: ParserRuleContext): Unit = {
      checkIndent(ctx)
    }

    override def enterImport_doc(ctx: ParserRuleContext): Unit = {
      checkIndent(ctx)
    }

    override def enterTask(ctx: ParserRuleContext): Unit = {
      checkIndent(ctx)
    }

    override def enterWorkflow(ctx: ParserRuleContext): Unit = {
      checkIndent(ctx)
    }
  }

  case class DeprecatedCommandStyleRule(id: String,
                                        severity: Severity,
                                        events: mutable.Buffer[LintEvent],
                                        grammar: Grammar)
      extends HiddenTokensLinterParserRule(id, severity, events, grammar) {

    override def exitTask_command_expr_part(ctx: ParserRuleContext): Unit = {
      if (grammar.version >= WdlVersion.V1) {
        if (!ctx.start.getText.contains("~")) {
          addEventFromTokens(ctx.start, Some(ctx.stop))
        }
      }
    }
  }

  // TODO: load these dynamically from a file
  val parserRules: Map[String, LinterParserRuleApplySig] = Map(
      "P001" -> WhitespaceTabsRule.apply,
      "P002" -> OddIndentRule.apply,
      "P003" -> MultipleBlankLineRule.apply,
      "P004" -> TopLevelIndentRule.apply,
      "P005" -> DeprecatedCommandStyleRule.apply
  )

  // AST-level rules
  // Note that most of these are caught by the type-checker, but it's
  // still good to be able to warn the user

  class LinterAstRule(id: String,
                      severity: Severity,
                      docSourceUrl: Option[URL],
                      events: mutable.Buffer[LintEvent])
      extends ASTVisitor {
    protected def addEvent(element: Element, message: Option[String] = None): Unit = {
      events.append(LintEvent(id, severity, element.text, docSourceUrl, message))
    }
  }

  type LinterAstRuleApplySig = (
      String,
      Severity,
      WdlVersion,
      types.Context,
      types.Stdlib,
      mutable.Buffer[LintEvent],
      Option[URL]
  ) => LinterAstRule

  // rules ported from miniwdl

  /**
    * Conversion from File-typed expression to String declaration/parameter
    */
//  case class StringCoersionRule(id: String,
//                                severity: Severity,
//                                version: WdlVersion,
//                                typesContext: types.Context,
//                                stdlib: types.Stdlib,
//                                events: mutable.Buffer[LintEvent],
//                                docSourceUrl: Option[URL])
//      extends LinterAstRule(id, severity, docSourceUrl, events) {
//
//    // TODO: This can probably be replaced by a call to one of the functions
//    //  in types.Util
//    def isCompoundCoercion[T <: Type](to: Type, from: WT)(implicit tag: ClassTag[T]): Boolean = {
//      (to, from) match {
//        case (TypeArray(toType, _, _), WT_Array(fromType)) =>
//          isCompoundCoercion[T](toType, fromType)
//        case (TypeMap(toKey, toValue, _), WT_Map(fromKey, fromValue)) =>
//          isCompoundCoercion[T](toKey, fromKey) || isCompoundCoercion[T](toValue, fromValue)
//        case (TypePair(toLeft, toRight, _), WT_Pair(fromLeft, fromRight)) =>
//          isCompoundCoercion(toLeft, fromLeft) || isCompoundCoercion(toRight, fromRight)
//        case (_: T, _: T) => false
//        case (_: T, _)    => true
//        case _            => false
//      }
//    }
//    def isCompoundCoercion[T <: WT](to: WT, from: WT)(implicit tag: ClassTag[T]): Boolean = {
//      (to, from) match {
//        case (WT_Array(toType), WT_Array(fromType)) =>
//          isCompoundCoercion[T](toType, fromType)
//        case (WT_Map(toKey, toValue), WT_Map(fromKey, fromValue)) =>
//          isCompoundCoercion[T](toKey, fromKey) || isCompoundCoercion[T](toValue, fromValue)
//        case (WT_Pair(toLeft, toRight), WT_Pair(fromLeft, fromRight)) =>
//          isCompoundCoercion(toLeft, fromLeft) || isCompoundCoercion(toRight, fromRight)
//        case (_: T, _: T) => false
//        case (_: T, _)    => true
//        case _            => false
//      }
//    }
//
//    override def visitExpression(ctx: ASTVisitor.Context[Expr]): Unit = {
//      // File-to-String coercions are normal in tasks, but flagged at the workflow level.
//      val isInWorkflow = ctx.getParentExecutable match {
//        case Some(_: Workflow) => true
//        case _                 => false
//      }
//      if (isInWorkflow) {
//        // if this expression is the rhs of a declaration, check that it is coercible to the lhs type
//        ctx.getParent.element match {
//          case Declaration(name, wdlType, expr, _)
//              if expr.isDefined && isCompoundCoercion[TypeString](
//                  wdlType,
//                  typesContext.declarations(name)
//              ) =>
//            addEvent(ctx.element)
//          case _ => ()
//        }
//        // check compatible arguments for operations that take multiple string arguments
//        // TODO: can't implement this until we have type inference
//        ctx.element match {
//          case ExprAdd(a, b, _) if ctx.findParent[ExprCompoundString].isEmpty =>
//            // if either a or b is a non-literal string type while the other is a file type
//            val wts = Vector(a, b).map(inferType)
//            val areStrings = wts.map(isStringType)
//            if (areStrings.exists(_) && !areStrings.forall(_) && !wts.exits(isStringLiteral)) {
//              addEvent(ctx, Some(""))
//            }
//          case ExprApply(funcName, elements, _) =>
//            // conversion of file-type expression to string-type function parameter
//            val toTypes: Vector[WT] = stdlib.getFunction(funcName) match {
//              case WT_Function1(_, arg1, _)             => Vector(arg1)
//              case WT_Function2(_, arg1, arg2, _)       => Vector(arg1, arg2)
//              case WT_Function3(_, arg1, arg2, arg3, _) => Vector(arg1, arg2, arg3)
//              case _                                    => Vector.empty
//            }
//            val fromTypes: Vector[WT] = elements.map(inferType)
//            toTypes.zip(fromTypes).foreach {
//              case (to, from) if isCompoundCoercion[WT_String](to, from) => addEvent(ctx.element)
//            }
//          case ExprArray(values, _) =>
//            // mixed string and file types in array
//            val areStrings = values.apply(isStringType)
//            if (areStrings.exists(_) && !areStrings.forall(_)) {
//              addEvent(ctx, Some(""))
//            }
//        }
//      }
//    }
//  }

  // rules ported from winstanley
  // rules not ported:
  // * missing command section - a command section is required, so the parser throws a SyntaxException if
  //   it doesn't find one
  // * value/callable lookup - these are caught by the type-checker
  // * no immediate declaration - the parser catches these
  // * wildcard outputs - the parser does not allow these even in draft-2
  // * unexpected/unsupplied inputs - the type-checker catches this

  case class NonPortableTaskRule(id: String,
                                 severity: Severity,
                                 version: WdlVersion,
                                 typesContext: types.Context,
                                 stdlib: types.Stdlib,
                                 events: mutable.Buffer[LintEvent],
                                 docSourceUrl: Option[URL])
      extends LinterAstRule(id, severity, docSourceUrl, events) {
    private val containerKeys = Set("docker", "container")

    override def visitTask(ctx: ASTVisitor.Context[Task]): Unit = {
      if (ctx.element.runtime.isEmpty) {
        addEvent(ctx.element, Some("add a runtime section specifying a container"))
      } else if (!ctx.element.runtime.get.kvs.exists(kv => containerKeys.contains(kv.id))) {
        addEvent(ctx.element, Some("add a container to the runtime section"))
      }
    }
  }

  case class NoTaskInputsRule(id: String,
                              severity: Severity,
                              version: WdlVersion,
                              typesContext: types.Context,
                              stdlib: types.Stdlib,
                              events: mutable.Buffer[LintEvent],
                              docSourceUrl: Option[URL])
      extends LinterAstRule(id, severity, docSourceUrl, events) {
    override def visitTask(ctx: ASTVisitor.Context[Task]): Unit = {
      if (ctx.element.input.isEmpty || ctx.element.input.get.declarations.isEmpty) {
        addEvent(ctx.element)
      }
    }
  }

  case class NoTaskOutputsRule(id: String,
                               severity: Severity,
                               version: WdlVersion,
                               typesContext: types.Context,
                               stdlib: types.Stdlib,
                               events: mutable.Buffer[LintEvent],
                               docSourceUrl: Option[URL])
      extends LinterAstRule(id, severity, docSourceUrl, events) {

    override def visitTask(ctx: ASTVisitor.Context[Task]): Unit = {
      if (ctx.element.output.isEmpty || ctx.element.output.get.declarations.isEmpty) {
        addEvent(ctx.element)
      }
    }
  }

  val astRules: Map[String, LinterAstRuleApplySig] = Map(
      "A001" -> NonPortableTaskRule.apply,
      "A002" -> NoTaskInputsRule.apply,
      "A003" -> NoTaskOutputsRule.apply
  )

  lazy val defaultRules: Map[String, Severity] =
    (parserRules.keys.toVector ++ astRules.keys.toVector).map(_ -> Severity.Default).toMap
}
