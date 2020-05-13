package wdlTools.cli

import java.io.{FileOutputStream, PrintStream}
import java.net.URL
import java.nio.file.{Files, Path}

import spray.json.{JsObject, JsString, JsValue}
import spray.json._
import wdlTools.linter.Severity.Severity
import wdlTools.linter.{LintEvent, Linter, Rules, Severity}

import scala.io.{AnsiColor, Source}
import scala.language.reflectiveCalls

case class Lint(conf: WdlToolsConf) extends Command {
  private val RULES_RESOURCE = "rules/lint_rules.json"

  override def apply(): Unit = {
    val url = conf.lint.url()
    val opts = conf.lint.getOptions
    val rules =
      if (conf.lint.config.isDefined) {
        val (incl, excl) = rulesFromFile(conf.lint.config())
        incl.getOrElse(Rules.defaultRules).removedAll(excl.getOrElse(Set.empty))
      } else if (conf.lint.includeRules.isDefined || conf.lint.excludeRules.isDefined) {
        val incl = conf.lint.includeRules.map(rulesFromOptions)
        val excl = conf.lint.excludeRules.map(_.toSet)
        incl.getOrElse(Rules.defaultRules).removedAll(excl.getOrElse(Set.empty))
      } else {
        Rules.defaultRules
      }
    val linter = Linter(opts, rules)
    linter.apply(url)

    if (linter.hasEvents) {
      // Load rule descriptions
      val rules = readJsonSource(Source.fromResource(RULES_RESOURCE))
      val outputFile = conf.lint.outputFile.toOption
      val toFile = outputFile.isDefined
      val printer: PrintStream = if (toFile) {
        val resolved = outputFile.get
        if (!conf.lint.overwrite() && Files.exists(resolved)) {
          throw new Exception(s"File already exists: ${resolved}")
        }
        val fos = new FileOutputStream(outputFile.get.toFile)
        new PrintStream(fos, true)
      } else {
        System.out
      }
      if (conf.lint.json()) {
        val js = eventsToJson(linter.getOrderedEvents, rules).prettyPrint
        printer.println(js)
      } else {
        printEvents(linter.getOrderedEvents, rules, printer, effects = !toFile)
      }
      if (toFile) {
        printer.close()
      }
    }
  }

  private def readJsonSource(src: Source): Map[String, JsValue] = {
    val jsObj =
      try {
        src.getLines.mkString(System.lineSeparator).parseJson
      } catch {
        case _: NullPointerException =>
          throw new Exception(s"Could not open resource ${RULES_RESOURCE}")
      }
    jsObj match {
      case JsObject(fields) => fields
      case _                => throw new Exception("Invalid lint_rules.json file")
    }
  }

  private def rulesFromFile(path: Path): (Option[Map[String, Severity]], Option[Set[String]]) = {
    val rules = readJsonSource(Source.fromFile(path.toFile))

    def rulesToSet(key: String): Option[Vector[JsValue]] = {
      if (rules.contains(key)) {
        rules(key) match {
          case JsArray(values) => Some(values)
          case _               => throw new Exception(s"Invalid lint config file ${path}")
        }
      } else {
        None
      }
    }

    val include = rulesToSet("include").map { rules =>
      rules.map {
        case JsString(value) => value -> Severity.Default
        case JsObject(fields) =>
          val severity = if (fields.contains("severity")) {
            Severity.withName(getString(fields("severity")))
          } else {
            Severity.Default
          }
          getString(fields("ruleId")) -> severity
        case _ => throw new Exception(s"Invalid lint config file ${path}")
      }.toMap
    }
    val exclude = rulesToSet("exclude").map { rules =>
      rules.map {
        case JsString(value) => value
        case _               => throw new Exception(s"Invalid lint config file ${path}")
      }.toSet
    }
    (include, exclude)
  }

  private def rulesFromOptions(options: Seq[String]): Map[String, Severity] = {
    options.map { opt =>
      val parts = opt.split("=", 1)
      if (parts.length == 2) {
        parts(0) -> Severity.withName(parts(1))
      } else {
        parts(0) -> Severity.Default
      }
    }.toMap
  }

  private def getFields(js: JsValue): Map[String, JsValue] = {
    js match {
      case JsObject(fields) => fields
      case _                => throw new Exception("Invalid linter_rules.json format")
    }
  }

  private def getString(js: JsValue): String = {
    js match {
      case JsString(value) => value
      case other           => throw new Exception(s"Expected JsString, got ${other}")
    }
  }

  private def eventsToJson(events: Map[URL, Vector[LintEvent]],
                           rules: Map[String, JsValue]): JsObject = {
    def eventToJson(err: LintEvent): JsObject = {
      val rule = getFields(rules(err.ruleId))
      JsObject(
          Map(
              "ruleId" -> JsString(err.ruleId),
              "ruleName" -> JsString(getString(rule("name"))),
              "ruleDescription" -> JsString(getString(rule("description"))),
              "severity" -> JsString(err.severity.toString),
              "startLine" -> JsNumber(err.textSource.line),
              "startCol" -> JsNumber(err.textSource.col),
              "endLine" -> JsNumber(err.textSource.endLine),
              "endCol" -> JsNumber(err.textSource.endCol)
          )
      )
    }

    JsObject(Map("sources" -> JsArray(events.map {
      case (url, events) =>
        JsObject(
            Map("source" -> JsString(url.toString), "events" -> JsArray(events.map(eventToJson)))
        )
    }.toVector)))
  }

  private def severityToColor(severity: Severity): String = {
    severity match {
      case Severity.Error   => AnsiColor.RED
      case Severity.Warning => AnsiColor.YELLOW
      case Severity.Ignore  => AnsiColor.REVERSED
    }
  }

  private def printEvents(events: Map[URL, Vector[LintEvent]],
                          rules: Map[String, JsValue],
                          printer: PrintStream,
                          effects: Boolean): Unit = {
    def colorMsg(msg: String, color: String): String = {
      if (effects) {
        s"${color}${msg}${AnsiColor.RESET}"
      } else {
        msg
      }
    }
    events.foreach { item =>
      val (msg, events) = item match {
        case (url, events) => (s"Lint in ${url}", events)
      }
      val border1 = "=" * msg.length
      val border2 = "-" * msg.length
      printer.println(border1)
      printer.println(colorMsg(msg, AnsiColor.BLUE))
      printer.println(border1)
      printer.println(colorMsg("Line:Col | Rule | Description", AnsiColor.BOLD))
      printer.println(border2)
      events.sortWith(_.textSource < _.textSource).foreach { event =>
        val ruleDesc = rules(event.ruleId) match {
          case JsObject(fields) =>
            val desc = getString(fields("description"))
            val eventMsg = if (event.message.isDefined) {
              s"${desc}: ${event.message.get}"
            } else {
              desc
            }
            if (effects) {
              colorMsg(eventMsg, severityToColor(event.severity))
            } else {
              s"${event.severity.toString}: ${eventMsg}"
            }
          case _ => throw new Exception("Invalid linter_rules.json format")
        }
        printer.println(f"${event.textSource}%-9s| ${event.ruleId} | ${ruleDesc}")
      }
    }
  }
}
