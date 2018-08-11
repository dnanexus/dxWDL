package dxWDL

import java.nio.file.{Files, Paths}

import com.typesafe.config.ConfigFactory
import common.Checked
import common.validation.Validation._
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.languages.LanguageFactory
import cromwell.languages.util.ImportResolver._
import languages.cwl.CwlV1_0LanguageFactory
import languages.wdl.biscayne.WdlBiscayneLanguageFactory
import languages.wdl.draft2.WdlDraft2LanguageFactory
import languages.wdl.draft3.WdlDraft3LanguageFactory
import wom.executable.WomBundle
import wom.expression.NoIoFunctionSet
import wom.graph._

import scala.collection.JavaConverters._
import scala.util.Try

object Wom {

    def getBundle(workflowSourcePath: String): WomBundle = {
        val maybeWdl: Try[Path] = DefaultPathBuilder.build(workflowSourcePath)
        val mainFile = maybeWdl.get

        val bundle: Checked[WomBundle] = getBundleAndFactory(mainFile).map(_._1)
        bundle.toOption.get
    }

    private def getBundleAndFactory(mainFile: Path): Checked[(WomBundle, LanguageFactory)] = {
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
                    new WdlDraft3LanguageFactory(ConfigFactory.empty()),
                    new WdlBiscayneLanguageFactory(ConfigFactory.empty()),
                    new CwlV1_0LanguageFactory(ConfigFactory.empty()))
                    .find(_.looksParsable(mainFileContents))
                    .getOrElse(new WdlDraft2LanguageFactory(ConfigFactory.empty()))

            val bundle = languageFactory.getWomBundle(mainFileContents, "{}", importResolvers, List(languageFactory))
            // Return the pair with the languageFactory
            bundle map ((_, languageFactory))
        }
    }

    private def readFile(filePath: String): Checked[String] =
        Try(Files.readAllLines(Paths.get(filePath)).asScala.mkString(System.lineSeparator())).toChecked

}
