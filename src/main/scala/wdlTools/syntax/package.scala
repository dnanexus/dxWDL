package wdlTools.syntax

import java.net.URL

sealed abstract class WdlVersion(val name: String, val order: Int) extends Ordered[WdlVersion] {
  def compare(that: WdlVersion): Int = this.order - that.order
}

object WdlVersion {
  case object Draft_2 extends WdlVersion("draft-2", 0)
  case object V1 extends WdlVersion("1.0", 1)
  case object V2 extends WdlVersion("2.0", 2)

  val All: Vector[WdlVersion] = Vector(V1, Draft_2).sortWith(_ < _)

  def withName(name: String): WdlVersion = {
    val version = All.collectFirst { case v if v.name == name => v }
    if (version.isDefined) {
      version.get
    } else {
      throw new NoSuchElementException(s"No value found for ${name}")
    }
  }
}

/** Source location in a WDL program. We add it to each syntax element
  * so we could do accurate error reporting. All positions are 1-indexed.
  *
  * Note: 'line' and 'col' may be 0 for "implicit" elements. Currently,
  * the only example of this is Version, which in draft-2 documents has
  * an implicit value of WdlVersion.Draft_2, but there is no actual version
  * statement.
  *
  * @param line: starting line number
  * @param col: starting column
  * @param endLine: line (end-inclusive) on which the last token ends
  * @param endCol: column (end-exclusive) at which the last token ends
  */
case class TextSource(line: Int, col: Int, endLine: Int, endCol: Int) extends Ordered[TextSource] {
  lazy val lineRange: Range = line to endLine

  def compare(that: TextSource): Int = {
    line - that.line match {
      case 0 =>
        col - that.col match {
          case 0 =>
            endLine - that.endLine match {
              case 0     => endCol - that.endCol
              case other => other
            }
          case other => other
        }
      case other => other
    }
  }

  override def toString: String = {
    s"${line}:${col}-${endLine}:${endCol}"
  }
}

object TextSource {
  val empty: TextSource = TextSource(0, 0, 0, 0)

  def fromSpan(start: TextSource, stop: TextSource): TextSource = {
    TextSource(
        start.line,
        start.col,
        stop.endLine,
        stop.endCol
    )
  }
}

// A syntax error that occured when parsing a document. It is generated
// by the ANTLR machinery and we transform it into this format.
final case class SyntaxError(docSourceUrl: Option[URL],
                             symbol: String,
                             line: Int,
                             charPositionInLine: Int,
                             msg: String)

// Syntax error exception
final class SyntaxException(message: String) extends Exception(message) {
  def this(msg: String, text: TextSource, docSourceUrl: Option[URL] = None) = {
    this(SyntaxException.formatMessage(msg, text, docSourceUrl))
  }
  def this(errors: Seq[SyntaxError]) = {
    this(SyntaxException.formatMessageFromErrorList(errors))
  }
}

object SyntaxException {
  def formatMessage(msg: String, text: TextSource, docSourceUrl: Option[URL]): String = {
    val urlPart = docSourceUrl.map(url => s" in ${url.toString}").getOrElse("")
    s"${msg} at ${text}${urlPart}"
  }

  def formatMessageFromErrorList(errors: Seq[SyntaxError]): String = {
    // make one big report on all the syntax errors
    val messages = errors.map {
      case SyntaxError(docSourceUrl, symbol, line, position, msg) =>
        val urlPart = docSourceUrl.map(url => s" in ${url.toString}").getOrElse("")
        s"${msg} at ${symbol}${urlPart} line ${line} column ${position}"
    }
    messages.mkString("\n")
  }
}

/**
  * A WDL comment.
  * @param value the comment string, including prefix ('#')
  * @param text the location of the comment in the source file
  */
case class Comment(value: String, text: TextSource) extends Ordered[Comment] {
  override def compare(that: Comment): Int = text.line - that.text.line
}

case class CommentMap(comments: Map[Int, Comment]) {
  def nonEmpty: Boolean = {
    comments.nonEmpty
  }

  lazy val minLine: Int = comments.keys.min

  lazy val maxLine: Int = comments.keys.max

  def filterWithin(range: Seq[Int]): CommentMap = {
    CommentMap(comments.view.filterKeys(range.contains).toMap)
  }

  def toSortedVector: Vector[Comment] = {
    comments.values.toVector.sortWith(_ < _)
  }

  def get(line: Int): Option[Comment] = {
    comments.get(line)
  }

  def apply(line: Int): Comment = {
    comments(line)
  }
}

object CommentMap {
  val empty: CommentMap = CommentMap(Map.empty)
}
