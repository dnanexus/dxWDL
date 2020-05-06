package wdlTools.eval

import java.net.URL
import wdlTools.syntax.TextSource

case class Context(bindings: Map[String, WdlValues.WV]) {
  def addBinding(name: String, value: WdlValues.WV): Context = {
    assert(!(bindings contains name))
    this.copy(bindings = bindings + (name -> value))
  }
}

// There is a standard library implementation for each WDL version.
trait StandardLibraryImpl {
  def call(funcName: String, args: Vector[WdlValues.WV], text: TextSource): WdlValues.WV
}

// A runtime error
final class EvalException(message: String) extends Exception(message) {
  def this(msg: String, text: TextSource, docSourceUrl: Option[URL] = None) = {
    this(EvalException.formatMessage(msg, text, docSourceUrl))
  }
}

object EvalException {
  def formatMessage(msg: String, text: TextSource, docSourceUrl: Option[URL]): String = {
    val urlPart = docSourceUrl.map(url => s" in ${url.toString}").getOrElse("")
    s"${msg} at ${text}${urlPart}"
  }
}

final class JsonSerializationException(message: String) extends Exception(message)
