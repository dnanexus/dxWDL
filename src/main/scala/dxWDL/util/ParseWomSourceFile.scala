package dxWDL.util

import cats.data.Validated.{Invalid, Valid}
import common.Checked
import common.transforms.CheckedAtoB
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.languages.util.ImportResolver._
import java.nio.file.{Files, Paths}
import languages.cwl.CwlV1_0LanguageFactory
import languages.wdl.draft2.WdlDraft2LanguageFactory
import languages.wdl.draft3.WdlDraft3LanguageFactory
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import wom.executable.WomBundle

// Read, parse, and typecheck a WDL/CWL source file. This includes loading all imported files.
object ParseWomSourceFile {

    // A mapping from
    private val allSources = HashMap.empty[String, String]

    // Record all files accessed while traversing imports. We wrap
    // the Cromwell importers, and write down every new file.
    //
    // type ImportResolver = CheckedAtoB[String, WorkflowSource]
    private def fileRecorder(resolver: ImportResolver) : ImportResolver = {
        CheckedAtoB.fromErrorOr { path : String =>
            val fileContent = resolver(path)
            allSources(path) = fileContent.right.toOption.get
            fileContent match {
                case Left(errors) => Invalid(errors)
                case Right(v) => Valid(v)
            }
        }
    }

    private def getBundle(mainFile: Path): (Language.Value, WomBundle) = {
        // Resolves for:
        // - Where we run from
        // - Where the file is
        lazy val importResolvers = List(
            fileRecorder(directoryResolver(
                DefaultPathBuilder.build(Paths.get(".")), allowEscapingDirectory = true
            )),
            fileRecorder(directoryResolver(
                DefaultPathBuilder.build(Paths.get(mainFile.toAbsolutePath.toFile.getParent)),
                allowEscapingDirectory = true
            )),
            fileRecorder(httpResolver)
        )

        val absPath = Paths.get(mainFile.toAbsolutePath.pathAsString)
        val mainFileContents = Files.readAllLines(absPath).asScala.mkString(System.lineSeparator())

        val languageFactory = if (mainFile.name.toLowerCase().endsWith("wdl")) {
            if (mainFileContents.startsWith("version 1.0") || mainFileContents.startsWith("version draft-3")) {
                new WdlDraft3LanguageFactory(Map.empty)
            } else {
                new WdlDraft2LanguageFactory(Map.empty)
            }
        } else new CwlV1_0LanguageFactory(Map.empty)

        val bundleChk: Checked[WomBundle] =
            languageFactory.getWomBundle(mainFileContents, "{}", importResolvers, List(languageFactory))
        val bundle = bundleChk.right.toOption.get

        val lang = (languageFactory.languageName.toLowerCase, languageFactory.languageVersionName) match {
            case ("wdl", "draft-2") => Language.WDLvDraft2
            case ("wdl", "draft-3") => Language.WDLvDraft2
            case ("wdl", "1.0") => Language.WDLv1_0
            case ("cwl", "1.0") => Language.CWLv1_0
            case (l,v) => throw new Exception(s"Unsupported language (${l}) version (${v})")
        }
        (lang, bundle)
    }

    def apply(sourcePath: java.nio.file.Path) : (Language.Value, WomBundle) = {
        val src : Path = DefaultPathBuilder.build(sourcePath)
        getBundle(src)
    }
}
