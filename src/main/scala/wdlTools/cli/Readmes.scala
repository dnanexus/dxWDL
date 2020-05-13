package wdlTools.cli

import wdlTools.generators.{ReadmeGenerator, Renderer}
import wdlTools.syntax.Parsers
import wdlTools.util.Util

import scala.language.reflectiveCalls

case class Readmes(conf: WdlToolsConf) extends Command {
  override def apply(): Unit = {
    val url = conf.readmes.url()
    val opts = conf.readmes.getOptions
    val parsers = Parsers(opts)
    val renderer = Renderer()
    val readmes = parsers.getDocumentWalker[String](url).walk { (url, doc, results) =>
      ReadmeGenerator(conf.readmes.developerReadmes(), renderer, results).apply(url, doc)
    }
    Util.writeContentsToFiles(readmes,
                              outputDir = conf.readmes.outputDir.toOption,
                              overwrite = conf.readmes.overwrite())
  }
}
