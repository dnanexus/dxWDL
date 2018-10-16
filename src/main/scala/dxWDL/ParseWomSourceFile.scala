// Parse a WDL/CWL source file, and return an abstract syntax tree.

package dxWDL

import common.Checked
import common.validation.Validation._
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.languages.util.ImportResolver._
import java.nio.file.{Files, Paths}
import languages.cwl.CwlV1_0LanguageFactory
import languages.wdl.draft2.WdlDraft2LanguageFactory
import languages.wdl.draft3.WdlDraft3LanguageFactory
import scala.collection.JavaConverters._
import scala.util.Try
import wom.executable.WomBundle

object ParseWomSourceFile {

    private def readFile(filePath: String): Checked[String] =
        Try(Files.readAllLines(Paths.get(filePath)).asScala.mkString(System.lineSeparator())).toChecked


    private def getBundle(mainFile: Path): Checked[WomBundle] = {
        // Resolves for:
        // - Where we run from
        // - Where the file is
        lazy val importResolvers = List(
            cromwell.languages.util.ImportResolver.directoryResolver(
                DefaultPathBuilder.build(Paths.get(".")), allowEscapingDirectory = true
            ),
            directoryResolver(
                DefaultPathBuilder.build(Paths.get(mainFile.toAbsolutePath.toFile.getParent)),
                allowEscapingDirectory = true
            ),
            httpResolver
        )

        readFile(mainFile.toAbsolutePath.pathAsString) flatMap { mainFileContents =>
            val languageFactory = if (mainFile.name.toLowerCase().endsWith("wdl")) {
                if (mainFileContents.startsWith("version 1.0") || mainFileContents.startsWith("version draft-3")) {
                    new WdlDraft3LanguageFactory(Map.empty)
                } else {
                    new WdlDraft2LanguageFactory(Map.empty)
                }
            } else new CwlV1_0LanguageFactory(Map.empty)

            languageFactory.getWomBundle(mainFileContents, "{}", importResolvers, List(languageFactory))
        }
    }

    def apply(sourceFile: String) : WomBundle = {
        val src : Path = DefaultPathBuilder.build(Paths.get(sourceFile))
        val bundle = getBundle(src)
        bundle match {
            case Right(bn) =>
                bn
            case Left(errors) =>
                throw new Exception(errors.toString)
        }
    }
}
