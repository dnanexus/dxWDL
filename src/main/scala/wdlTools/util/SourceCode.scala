package wdlTools.util

import java.net.URL
import java.nio.file.Paths

import scala.io.Source

/**
  * Source code from a URL
  * @param url the source URL
  * @param lines the lines of the file
  */
case class SourceCode(url: URL, lines: Seq[String]) {
  lazy override val toString: String = lines.mkString(System.lineSeparator())
}

object SourceCode {

  // Examples for URLs:
  //   http://google.com/A.txt
  //   https://google.com/A.txt
  //   file://A/B.txt
  //   foo.txt
  //
  // Follow the URL and retrieve the content as a string.
  //
  // Note: we are assuming this is a textual file.
  private def fetchHttpAddress(url: URL): Seq[String] = {
    val src = Source.fromURL(url)
    try {
      src.getLines().toVector
    } finally {
      src.close()
    }
  }

  def loadFrom(url: URL): SourceCode = {
    val lines = url.getProtocol match {
      case "http"  => fetchHttpAddress(url)
      case "https" => fetchHttpAddress(url)
      case "file"  => Util.readLinesFromFile(Paths.get(url.getPath))
      case _       => throw new Exception(s"unknown protocol in URL ${url}")
    }

    SourceCode(url, lines)
  }
}
