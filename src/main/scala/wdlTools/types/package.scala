package wdlTools.types

import java.net.URL
import wdlTools.syntax.TextSource

// Type error exception
final class TypeException(message: String) extends Exception(message) {
  def this(msg: String, text: TextSource, docSourceUrl: Option[URL] = None) = {
    this(TypeException.formatMessage(msg, text, docSourceUrl))
  }
}

object TypeException {
  def formatMessage(msg: String, text: TextSource, docSourceUrl: Option[URL]): String = {
    val urlPart = docSourceUrl.map(url => s" in ${url.toString}").getOrElse("")
    s"${msg} at ${text}${urlPart}"
  }
}

final class TypeUnificationException(message: String) extends Exception(message)
