package wdlTools.cli

import java.nio.file.Files

import wdlTools.formatter._
import wdlTools.util.Util

import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls

case class Upgrade(conf: WdlToolsConf) extends Command {
  override def apply(): Unit = {
    val url = conf.upgrade.url()
    val opts = conf.upgrade.getOptions
    val outputDir = conf.format.outputDir.toOption
    val overwrite = conf.format.overwrite()
    val upgrader = Upgrader(opts)
    // write out upgraded versions
    val documents =
      upgrader.upgrade(url, conf.upgrade.srcVersion.toOption, conf.upgrade.destVersion())
    documents.foreach {
      case (uri, lines) =>
        if (outputDir.isDefined || overwrite) {
          Files.write(Util.getLocalPath(uri, outputDir, overwrite), lines.asJava)
        } else {
          println(lines.mkString(System.lineSeparator()))
        }
    }
  }
}
