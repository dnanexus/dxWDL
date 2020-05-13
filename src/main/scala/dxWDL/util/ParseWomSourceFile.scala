package dxWDL.util

import com.typesafe.config.ConfigFactory
import java.nio.file.{Path, Paths}
import scala.collection.mutable.HashMap
import scala.util.matching.Regex

import wdlTools.syntax.{AbstractSyntax => AST, Parsers, WdlVersion}
import wdlTools.util.{
  Util => WdlUtil,
  Verbosity => WdlVerbosity,
  TypeCheckingRegime => WdlTypeCheckingRegime}
import wdlTools.types.{
  Context => WdlTypeContext,
  Stdlib => WdlTypeStdlib,
  TypeInfer,
  TypedAbstractSyntax => TAT,
  WdlTypes}
import dxWDL.base.{Language, Utils}

// Read, parse, and typecheck a WDL source file. This includes loading all imported files.
case class ParseWomSourceFile(verbose: Boolean) {

  private case class BInfo(callables : Map[String, AST.Callable],
                           sources : Map[String, WorkflowSource],
                           adjunctFiles : Map[String, Vector[Adjuncts.AdjunctFile]])

  private def mergeCallables(aCallables : Map[String, AST.Callable],
                             bCallables : Map[String, AST.Callable]) : Map[String, AST.Callable] = {
    var allCallables = Map.empty[String, AST.Callable]
    aCallables.foreach {
      case (key, callable) =>
        bCallables.get(key) match {
          case None =>
            allCallables = allCallables + (key -> callable)

          // The comparision is done with "toString", because otherwise two
          // identical definitions are somehow, through the magic of Scala,
          // unequal.
          case Some(existing) if (existing != callable) =>
            Utils.error(s"""|${key} appears with two different callable definitions
                            |1)
                            |${callable}
                            |
                            |2)
                            |${existing}
                            |""".stripMargin)
            throw new Exception(s"${key} appears twice, with two different definitions")
          case _ => ()
        }
    }
    allCallables
  }

  private def bInfoFromDoc(doc : AST.Document) : BInfo = {
    // Add source and adjuncts for main file
    val pathOrUrl = doc.codeSourceUrl.toString
    val (source, adjunctFiles) =
      if (pathOrUrl.contains("://")) {
        val sources = Map(pathOrUrl -> doc.sourceCode)
        (sources, Map.empty)
      } else {
        val absPath: String = Paths.get(pathOrUrl).toAbsolutePath.toString
        val sources = Map(absPath -> doc.sourceCode)
        val adjunctFiles = Adjuncts.findAdjunctFiles(absPath)
        (sources, adjunctFiles)
      }

    val tasks : Vector[AST.Task] = doc.elements.collect {
      case x : AST.Task => x
    }
    val primaryCallable =
      doc.workflow match {
        case None if tasks.size == 1 => Some(tasks.head)
        case Some(wf) => Some(wf)
        case _ => None
      }
    val allCallables = (tasks ++ primaryCallable.toVector).map{
      callable => callable.name -> callable
    }.toMap

    val bundle = Bundle(primaryCallable, allCallables, Map.empty)
    BInfo(bundle, sources, adjunctsFiles)
  }


  // recurse into the imported packages
  //
  // Check the uniqueness of tasks, Workflows, and Types
  // merge everything into one bundle.
  private def dfsFlatten(bInfo : BInfo) : BInfo = {
    val imports : Vector[ImportDoc] = bInfo.bundle.elements.collect {
      case x : ImportDoc => x
    }

    imports.foldLeft(bInfo) {
      case (accu : BInfo, imp) if imp.doc == None =>
        accuInfo
      case (accu : BInfo, imp) =>
        val impBInfo = bInfoFromDoc(imp.doc)
        val flatImpBInfo = dfsFlatten(impBInfo)
        BInfo(
          callables = mergeCallables(accu.callables, flatImpBInfo.callables),
          sources = accu.sources ++ flatImpBInfo.sources,
          adjunctFiles = accu.adjunctsFiles ++ flatImpBInfo.adjunctFiles)
    }
  }

  private def languageFromVersion(version : WdlVersion): Language.Value = {
    version match {
      case WdlVersion.Draft_2  => Language.WDLvDraft2
      case WdlVersion.V1       => Language.WDLv1_0
      case other               => throw new Exception(s"Unsupported dielect ${other}")
    }
  }

  // Parses the main WDL file and all imports and creates a "bundle" of workflows and tasks.
  // Also returns 1) a mapping of input files to source code; 2) a mapping of input files to
  // "adjuncts" (such as README files); and 3) a vector of sub-bundles (one per import).
  def apply(mainFile: Path, imports: List[Path]): (Language.Value,
                                                   WdlBundle,
                                                   Map[String, WorkflowSource],
                                                   Map[String, Vector[Adjuncts.AdjunctFile]]) = {
    // Resolves for:
    // - Where we run from
    // - Where the file is
    val allSources = HashMap.empty[String, WorkflowSource]
    val adjunctFiles = HashMap.empty[String, Vector[Adjuncts.AdjunctFile]]

    // parse and type check
    val mainAbsPath = mainFile.toAbsolutePath
    val srcDir = Paths.get(mainAbsPath.getParent())
    val opts =
      Options(typeChecking = WdlTypeCheckingRegime.Strict,
              antlr4Trace = false,
              localDirectories = imports.toVector :+ srcDir,
              verbosity = if (verbose) WdlVerbosity.Verbose else WdlVerbosity.Quiet)
    val parsers = Parsers(opts)
    val stdlib = WdlTypeStdlib(opts)
    val typeInfer = TypeInfer(stdlib)
    val mainDoc : AST.Document = parsers.parseDocument(WdlUtil.pathToUrl(mainAbsPath))
    val ctxTypes = checker.apply(mainDoc)

    // recurse into the imported packages
    //
    // Check the uniqueness of tasks, Workflows, and Types
    // merge everything into one bundle.
    val flatInfo : BInfo = dfsFlatten(bInfo)

    (languageFromVersion(mainDoc.version),
     Bundle(primaryCallable, flatInfo.callables, ctxTypes.structs),
     allInfo2.sources,
     allInfo2.ajunctsFiles)
  }

}
