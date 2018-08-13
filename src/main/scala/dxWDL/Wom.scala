package dxWDL

import java.nio.file.{Files, Paths}

import common.Checked
import common.validation.Validation._
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.languages.util.ImportResolver._
import languages.cwl.CwlV1_0LanguageFactory
import languages.wdl.biscayne.WdlBiscayneLanguageFactory
import languages.wdl.draft2.WdlDraft2LanguageFactory
import languages.wdl.draft3.WdlDraft3LanguageFactory
import wom.executable.WomBundle

import scala.collection.JavaConverters._
import scala.util.Try

object Wom {

    def getBundle(workflowSourcePath: String): WomBundle = {
        val maybeWdl: Try[Path] = DefaultPathBuilder.build(workflowSourcePath)
        val mainFile = maybeWdl.get

        val bundle: Checked[WomBundle] = getBundleAndFactory(mainFile)
        bundle.toOption.get
    }

    private def getBundleAndFactory(mainFile: Path): Checked[WomBundle] = {
        // Resolves for:
        // - Where we run from
        // - Where the file is
        lazy val importResolvers: List[ImportResolver] = List(
            DirectoryResolver(
                DefaultPathBuilder.build(Paths.get(".")),
                allowEscapingDirectory = false),
            DirectoryResolver(
                DefaultPathBuilder.build(Paths.get(mainFile.toAbsolutePath.toFile.getParent)),
                allowEscapingDirectory = true
            ),
            HttpResolver()
        )

        readFile(mainFile.toAbsolutePath.pathAsString) flatMap { mainFileContents =>
            val languageFactory =
                List(
                    new WdlDraft3LanguageFactory(Map.empty),
                    new WdlBiscayneLanguageFactory(Map.empty),
                    new CwlV1_0LanguageFactory(Map.empty))
                    .find(_.looksParsable(mainFileContents))
                    .getOrElse(new WdlDraft2LanguageFactory(Map.empty))

            val bundle = languageFactory.getWomBundle(mainFileContents, "{}", importResolvers, List(languageFactory))
            bundle
        }
    }

    private def readFile(filePath: String): Checked[String] =
        Try(Files.readAllLines(Paths.get(filePath)).asScala.mkString(System.lineSeparator())).toChecked

}
