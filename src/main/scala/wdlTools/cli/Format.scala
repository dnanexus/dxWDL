package wdlTools.cli

import java.nio.file.Files

import wdlTools.formatter._
import wdlTools.util.Util

import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls

case class Format(conf: WdlToolsConf) extends Command {
  override def apply(): Unit = {
    val url = conf.format.url()
    val opts = conf.format.getOptions
    val outputDir = conf.format.outputDir.toOption
    val overwrite = conf.format.overwrite()
    val formatter = WdlV1Formatter(opts)
    formatter.formatDocuments(url)
    formatter.documents.foreach {
      case (uri, lines) =>
        if (outputDir.isDefined || overwrite) {
          Files.write(Util.getLocalPath(uri, outputDir, overwrite), lines.asJava)
        } else {
          println(lines.mkString(System.lineSeparator()))
        }
    }
  }
}
