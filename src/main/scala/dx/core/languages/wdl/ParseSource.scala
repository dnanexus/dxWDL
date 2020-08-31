package dx.core.languages.wdl

import java.nio.file.Path

import dx.api.DxApi
import dx.core.io.DxFileAccessProtocol
import dx.core.languages.Language
import wdlTools.syntax.{Parsers, SyntaxException}
import wdlTools.types.{
  Context,
  TypeException,
  TypeInfer,
  WdlTypes,
  TypeCheckingRegime => WdlTypeCheckingRegime,
  TypedAbstractSyntax => TAT
}
import wdlTools.util.{Adjuncts, FileSource, FileSourceResolver, LocalFileSource, StringFileSource}

// Read, parse, and typecheck a WDL source file. This includes loading all imported files.
case class ParseSource(dxApi: DxApi) {
  private val logger = dxApi.logger
  private case class BInfo(callables: Map[String, TAT.Callable],
                           sources: Map[String, TAT.Document],
                           adjunctFiles: Map[String, Vector[Adjuncts.AdjunctFile]])

  private def mergeCallables(aCallables: Map[String, TAT.Callable],
                             bCallables: Map[String, TAT.Callable]): Map[String, TAT.Callable] = {
    aCallables.foldLeft(bCallables) {
      case (accu, (name, callable)) =>
        accu.get(name) match {
          case None =>
            accu + (name -> callable)

          // The comparision is done with "toString", because otherwise two
          // identical definitions are somehow, through the magic of Scala,
          // unequal.
          case Some(existing) if existing != callable =>
            logger.error(s"""|${name} appears with two different callable definitions
                             |1)
                             |${callable}
                             |
                             |2)
                             |${existing}
                             |""".stripMargin)
            throw new Exception(s"callable ${name} appears twice, with two different definitions")
          case _ =>
            // The same task/workflow appears twice
            accu
        }
    }
  }

  private def bInfoFromDoc(doc: TAT.Document): BInfo = {
    // Add source and adjuncts for main file
    val (sources, adjunctFiles) = doc.source match {
      case localFs: LocalFileSource =>
        val absPath: Path = localFs.localPath
        val sources = Map(absPath.toString -> doc)
        val adjunctFiles = Adjuncts.findAdjunctFiles(absPath)
        (sources, adjunctFiles)
      case fs =>
        val sources = Map(fs.toString -> doc)
        (sources, Map.empty[String, Vector[Adjuncts.AdjunctFile]])
    }
    val tasks: Vector[TAT.Task] = doc.elements.collect {
      case x: TAT.Task => x
    }
    val allCallables = (tasks ++ doc.workflow.toVector).map { callable =>
      callable.name -> callable
    }.toMap

    BInfo(allCallables, sources, adjunctFiles)
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
    val retval = imports.foldLeft(topLevelBInfo) {
      case (accu: BInfo, imp) =>
        val flatImpBInfo = dfsFlatten(imp.doc)
        BInfo(
            callables = mergeCallables(accu.callables, flatImpBInfo.callables),
            sources = accu.sources ++ flatImpBInfo.sources,
            adjunctFiles = accu.adjunctFiles ++ flatImpBInfo.adjunctFiles
        )
    }
    retval
  }

  private def createFileResolver(importDirs: Vector[Path] = Vector.empty): FileSourceResolver = {
    val dxProtocol = DxFileAccessProtocol(dxApi)
    FileSourceResolver.create(importDirs, Vector(dxProtocol), logger)
  }

  private def parseWdlFromPath(path: Path, importDirs: Vector[Path]): (TAT.Document, Context) = {
    val srcDir = path.getParent
    val fileResolver = createFileResolver(importDirs :+ srcDir)
    val parsers = Parsers(followImports = true, fileResolver = fileResolver, logger = logger)
    val doc = parsers.parseDocument(fileResolver.fromPath(path))
    TypeInfer(regime = WdlTypeCheckingRegime.Strict, fileResolver = fileResolver, logger = logger)
      .apply(doc)
  }

  // Parses the main WDL file and all imports and creates a "bundle" of workflows and tasks.
  // Also returns 1) a mapping of input files to source documents; 2) a mapping of input files to
  // "adjuncts" (such as README files); and 3) a vector of sub-bundles (one per import).
  def apply(mainFile: Path, imports: Vector[Path]): (FileSource,
                                                     Language.Value,
                                                     Bundle,
                                                     Map[String, TAT.Document],
                                                     Map[String, Vector[Adjuncts.AdjunctFile]]) = {
    // Resolves for:
    // - Where we run from
    // - Where the file is

    // parse and type check
    val mainAbsPath = mainFile.toAbsolutePath
    val (tMainDoc, ctxTypes) = parseWdlFromPath(mainAbsPath, imports)

    val primaryCallable =
      tMainDoc.workflow match {
        case None =>
          val tasks: Vector[TAT.Task] = tMainDoc.elements.collect {
            case x: TAT.Task => x
          }
          if (tasks.size == 1) {
            Some(tasks.head)
          } else {
            None
          }
        case Some(wf) => Some(wf)
        case _        => None
      }

    // recurse into the imported packages
    //
    // Check the uniqueness of tasks, Workflows, and Types
    // merge everything into one bundle.
    val flatInfo: BInfo = dfsFlatten(tMainDoc)

    (tMainDoc.source,
     Language.fromWdlVersion(tMainDoc.version.value),
     Bundle(primaryCallable, flatInfo.callables, ctxTypes.aliases),
     flatInfo.sources,
     flatInfo.adjunctFiles)
  }

  // Parse a Workflow source file. Return the:
  //  * workflow definition
  //  * directory of tasks
  //  * directory of type aliases
  def parseWdlWorkflow(
      wfSource: String
  ): (TAT.Workflow, Map[String, TAT.Task], Map[String, WdlTypes.T], TAT.Document) = {
    val (tDoc, _) = parseWdlFromString(wfSource)
    val tasks = tDoc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    val aliases = tDoc.elements.collect {
      case struct: TAT.StructDefinition =>
        struct.name -> WdlTypes.T_Struct(struct.name, struct.members)
    }.toMap
    val wf = tDoc.workflow match {
      case None    => throw new RuntimeException("This document should have a workflow")
      case Some(x) => x
    }
    (wf, tasks, aliases, tDoc)
  }

  def parseWdlTasks(
      wfSource: String
  ): (Map[String, TAT.Task], Map[String, WdlTypes.T], TAT.Document) = {
    val (tDoc, typeCtx) = parseWdlFromString(wfSource)
    val tasks = tDoc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    (tasks, typeCtx.aliases, tDoc)
  }

  def parseWdlTask(taskSource: String): (TAT.Task, Map[String, WdlTypes.T], TAT.Document) = {
    val (tasks, aliases, tDoc) = parseWdlTasks(taskSource)
    if (tDoc.workflow.isDefined)
      throw new Exception("a workflow that shouldn't be a member of this document")
    if (tasks.isEmpty)
      throw new Exception("no tasks in this WDL program")
    if (tasks.size > 1)
      throw new Exception("More than one task in this WDL program")
    (tasks.values.head, aliases, tDoc)
  }

  // Extract the only task from a namespace
  def getMainTask(bundle: Bundle): TAT.Task = {
    bundle.primaryCallable match {
      case None                 => throw new Exception("found no callable")
      case Some(task: TAT.Task) => task
      case Some(wf)             => throw new Exception(s"found a workflow ${wf.name} and not a task")
    }
  }
}
