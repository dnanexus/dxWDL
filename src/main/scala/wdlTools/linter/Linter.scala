package wdlTools.linter

import java.net.URL

import org.antlr.v4.runtime.tree.ParseTreeListener
import wdlTools.linter.Severity.Severity
import wdlTools.syntax.Antlr4Util.ParseTreeListenerFactory
import wdlTools.syntax.{Antlr4Util, Parsers}
import wdlTools.types.{Stdlib, TypeInfer}
import wdlTools.util.Options

import scala.collection.mutable

case class LinterParserRuleFactory(
    rules: Map[String, Severity],
    events: mutable.Map[URL, mutable.Buffer[LintEvent]]
) extends ParseTreeListenerFactory {
  override def createParseTreeListeners(
      grammar: Antlr4Util.Grammar
  ): Vector[ParseTreeListener] = {
    val docEvents: mutable.Buffer[LintEvent] = mutable.ArrayBuffer.empty
    events(grammar.docSourceUrl.get) = docEvents
    rules.collect {
      case (id, severity) if Rules.parserRules.contains(id) =>
        Rules.parserRules(id)(id, severity, docEvents, grammar)
    }.toVector
  }
}

case class Linter(opts: Options,
                  rules: Map[String, Severity] = Rules.defaultRules,
                  events: mutable.Map[URL, mutable.Buffer[LintEvent]] = mutable.HashMap.empty) {
  def hasEvents: Boolean = events.nonEmpty

  def getOrderedEvents: Map[URL, Vector[LintEvent]] =
    events.map {
      case (url, docEvents) => url -> docEvents.toVector.sortWith(_ < _)
    }.toMap

  def apply(url: URL): Unit = {
    val parsers = Parsers(opts, listenerFactories = Vector(LinterParserRuleFactory(rules, events)))
    parsers.getDocumentWalker[mutable.Buffer[LintEvent]](url, events).walk { (url, doc, _) =>
      val astRules = rules.view.filterKeys(Rules.astRules.contains)
      if (astRules.nonEmpty) {
        if (!events.contains(url)) {
          val docEvents = mutable.ArrayBuffer.empty[LintEvent]
          events(url) = docEvents
        }
        // First run the TypeChecker to infer the types of all expressions
        val stdlib = Stdlib(opts)
        val typeChecker = TypeInfer(stdlib)
        val (_, typesContext) = typeChecker.apply(doc)
        // Now execute the linter rules
        val visitors = astRules.map {
          case (id, severity) =>
            Rules.astRules(id)(
                id,
                severity,
                doc.version.value,
                typesContext,
                stdlib,
                events(url),
                Some(url)
            )
        }.toVector
        val astWalker = LinterASTWalker(opts, visitors)
        astWalker.apply(doc)
      }
    }
  }
}
