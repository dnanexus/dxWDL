package dxWDL.base

import java.nio.file.{Path, Paths}

import wdlTools.formatter.WdlV1Formatter
import wdlTools.syntax.{Parsers, AbstractSyntax => AST}
import wdlTools.util.{
  SourceCode => WdlSourceCode,
  TypeCheckingRegime => WdlTypeCheckingRegime,
  Util => WdlUtil,
  Verbosity => WdlVerbosity
}
import wdlTools.types.{Context, TypeInfer, TypeOptions, WdlTypes, TypedAbstractSyntax => TAT}

import scala.io.Source

// Read, parse, and typecheck a WDL source file. This includes loading all imported files.
case class ParseWomSourceFile(verbose: Boolean) {

  private case class BInfo(callables: Map[String, TAT.Callable],
                           sources: Map[String, WorkflowSource],
                           adjunctFiles: Map[String, Vector[Adjuncts.AdjunctFile]])

  private def mergeCallables(aCallables: Map[String, TAT.Callable],
                             bCallables: Map[String, TAT.Callable]): Map[String, TAT.Callable] = {
    var allCallables = Map.empty[String, TAT.Callable]
    aCallables.foreach {
      case (key, callable) =>
        bCallables.get(key) match {
          case None =>
            allCallables = allCallables + (key -> callable)

          // The comparision is done with "toString", because otherwise two
          // identical definitions are somehow, through the magic of Scala,
          // unequal.
          case Some(existing) if existing != callable =>
            Utils.error(s"""|$key appears with two different callable definitions
                            |1)
                            |$callable
                            |
                            |2)
                            |$existing
                            |""".stripMargin)
            throw new Exception(s"$key appears twice, with two different definitions")
          case _ => ()
        }
    }
    allCallables
  }

  private def bInfoFromDoc(doc: TAT.Document): BInfo = {
    // Add source and adjuncts for main file
    val pathOrUrl = doc.sourceCode
    val (sources, adjunctFiles) =
      if (pathOrUrl.contains("://")) {
        val sources = Map(pathOrUrl -> doc.sourceCode)
        (sources, Map.empty[String, Vector[Adjuncts.AdjunctFile]])
      } else {
        val absPath: String = Paths.get(pathOrUrl).toAbsolutePath.toString
        val sources = Map(absPath -> doc.sourceCode)
        val adjunctFiles = Adjuncts.findAdjunctFiles(Paths.get(absPath))
        (sources, adjunctFiles)
      }

    val tasks: Vector[TAT.Task] = doc.elements.collect {
      case x: TAT.Task => x
    }
    val allCallables = (tasks ++ doc.workflow.toVector).map { callable =>
      callable.name -> callable
    }.toMap

    BInfo(allCallables, sources, adjunctFiles)
  }

  private def makeOptions(imports: Seq[Path] = Vector.empty): TypeOptions = {
    TypeOptions(
        antlr4Trace = false,
        localDirectories = imports.toVector,
        verbosity = if (verbose) WdlVerbosity.Verbose else WdlVerbosity.Quiet,
        followImports = true,
        typeChecking = WdlTypeCheckingRegime.Strict
    )
  }

  // recurse into the imported packages
  //
  // Check the uniqueness of tasks, Workflows, and Types
  // merge everything into one bundle.
  private def dfsFlatten(tDoc: TAT.Document): BInfo = {
    val imports: Vector[TAT.ImportDoc] = tDoc.elements.collect {
      case x: TAT.ImportDoc => x
    }

    val topLevelBInfo = bInfoFromDoc(tDoc)

    // dive into the imported documents and fold them into the top-level
    // document
    imports.foldLeft(topLevelBInfo) {
      case (accu: BInfo, imp) =>
        val flatImpBInfo = dfsFlatten(imp.doc)
        BInfo(
            callables = mergeCallables(accu.callables, flatImpBInfo.callables),
            sources = accu.sources ++ flatImpBInfo.sources,
            adjunctFiles = accu.adjunctFiles ++ flatImpBInfo.adjunctFiles
        )
    }
  }

  private def parseWdlFromPath(path: Path,
                               importDirs: Vector[Path] = Vector.empty): (TAT.Document, Context) = {
    val srcDir = path.getParent
    val opts = makeOptions(importDirs :+ srcDir)
    val parsers = Parsers(opts)
    val doc = parsers.parseDocument(WdlUtil.pathToUrl(path))
    TypeInfer(opts).apply(doc)
  }

  // Parses the main WDL file and all imports and creates a "bundle" of workflows and tasks.
  // Also returns 1) a mapping of input files to source code; 2) a mapping of input files to
  // "adjuncts" (such as README files); and 3) a vector of sub-bundles (one per import).
  def apply(mainFile: Path, imports: List[Path]): (Language.Value,
                                                   WomBundle,
                                                   Map[String, WorkflowSource],
                                                   Map[String, Vector[Adjuncts.AdjunctFile]]) = {
    // Resolves for:
    // - Where we run from
    // - Where the file is

    // parse and type check
    val mainAbsPath = mainFile.toAbsolutePath
    val (tMainDoc, ctxTypes) = parseWdlFromPath(mainAbsPath, imports.toVector)

    val primaryCallable =
      tMainDoc.workflow match {
        case None =>
          val tasks: Vector[TAT.Task] = tMainDoc.elements.collect {
            case x: TAT.Task => x
          }
          if (tasks.size == 1)
            Some(tasks.head)
          else
            None
        case Some(wf) => Some(wf)
        case _        => None
      }

    // recurse into the imported packages
    //
    // Check the uniqueness of tasks, Workflows, and Types
    // merge everything into one bundle.
    val flatInfo: BInfo = dfsFlatten(tMainDoc)

    (Language.fromWdlVersion(tMainDoc.version.value),
     WomBundle(primaryCallable, flatInfo.callables, ctxTypes.aliases),
     flatInfo.sources,
     flatInfo.adjunctFiles)
  }

  private def parseWdlFromString(
      src: String,
      opts: TypeOptions = makeOptions(Vector.empty)
  ): (AST.Document, TAT.Document, Context) = {
    val lines = Source.fromString(src).getLines.toVector
    val sourceCode = WdlSourceCode(None, lines)
    val parser = Parsers(opts).getParser(sourceCode)
    val doc = parser.parseDocument(sourceCode)
    val (tDoc, ctx) = TypeInfer(opts).apply(doc)
    (doc, tDoc, ctx)
  }

  // throw an exception if this WDL program is invalid
  def validateWdlCode(wdlWfSource: String, language: Option[Language.Value] = None): Unit = {
    val (_, tDoc, _) = parseWdlFromString(wdlWfSource)

    // Check that this is the correct language version
    language match {
      case None => ()
      case Some(requiredLang) =>
        val docLang = Language.fromWdlVersion(tDoc.version.value)
        if (docLang != requiredLang)
          throw new Exception(s"document has wrong version $docLang, should be $requiredLang")
    }
  }

  // Parse a Workflow source file. Return the:
  //  * workflow definition
  //  * directory of tasks
  //  * directory of type aliases
  def parseWdlWorkflow(
      wfSource: String
  ): (TAT.Workflow, Map[String, TAT.Task], Map[String, WdlTypes.T], TAT.Document) = {
    val (_, tDoc, _) = parseWdlFromString(wfSource)
    val tasks = tDoc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    val aliases = tDoc.elements.collect {
      case struct: TAT.StructDefinition =>
        struct.name -> WdlTypes.T_Struct(struct.name, struct.members)
    }.toMap
    val wf = tDoc.workflow match {
      case None    => throw new RuntimeException("Sanity, this document should have a workflow")
      case Some(x) => x
    }
    (wf, tasks, aliases, tDoc)
  }

  def parseWdlTask(taskSource: String): (TAT.Task, Map[String, WdlTypes.T], TAT.Document) = {
    val (_, tDoc, typeCtx) = parseWdlFromString(taskSource)
    if (tDoc.workflow.isDefined)
      throw new Exception("a workflow that shouldn't be a member of this document")
    val tasks = tDoc.elements.collect {
      case task: TAT.Task => task
    }
    if (tasks.isEmpty)
      throw new Exception("no tasks in this WDL program")
    if (tasks.size > 1)
      throw new Exception("More than one task in this WDL program")
    (tasks.head, typeCtx.aliases, tDoc)
  }

  // Extract the only task from a namespace
  def getMainTask(bundle: WomBundle): TAT.Task = {
    bundle.primaryCallable match {
      case None                 => throw new Exception("found no callable")
      case Some(task: TAT.Task) => task
      case Some(wf)             => throw new Exception(s"found a workflow ${wf.name} and not a task")
    }
  }

  // Go through one WDL source file, and return a map from task name
  // to its source code. Return an empty map if there are no tasks
  // in this file.
  def scanForTasks(sourceCode: String): Map[String, String] = {
    val opts = makeOptions()
    val (doc, _, _) = parseWdlFromString(sourceCode, opts)
    val formatter = WdlV1Formatter(opts)
    doc.elements.collect {
      case t: AST.Task => t.name -> formatter.formatElement(t, doc.comments).mkString("\n")
    }.toMap
  }

  // Go through one WDL source file, and return a tuple of (wf name,
  // source code). Return None if there is no workflow in this file.
  def scanForWorkflow(sourceCode: String): Option[(String, String)] = {
    val opts = makeOptions()
    val (doc, _, _) = parseWdlFromString(sourceCode, opts)
    val formatter = WdlV1Formatter(opts)
    doc.workflow.map(wf => (wf.name, formatter.formatElement(wf, doc.comments).mkString("\n")))
  }
}
