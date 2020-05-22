package dxWDL.base

import com.typesafe.config.ConfigFactory
import java.nio.file.{Path, Paths}
import scala.collection.mutable.HashMap
import scala.util.matching.Regex

import wdlTools.syntax.{Parsers, WdlVersion}
import wdlTools.util.{
  Options,
  SourceCode => WdlSourceCode,
  Util => WdlUtil,
  Verbosity => WdlVerbosity,
  TypeCheckingRegime => WdlTypeCheckingRegime}
import wdlTools.types.{TypeInfer, TypedAbstractSyntax => TAT, WdlTypes}
import dxWDL.base.{Language, BaseUtils}

// Read, parse, and typecheck a WDL source file. This includes loading all imported files.
case class ParseWomSourceFile(verbose: Boolean) {

  private case class BInfo(callables : Map[String, TAT.Callable],
                           sources : Map[String, WorkflowSource],
                           adjunctFiles : Map[String, Vector[Adjuncts.AdjunctFile]])

  private def mergeCallables(aCallables : Map[String, TAT.Callable],
                             bCallables : Map[String, TAT.Callable]) : Map[String, TAT.Callable] = {
    var allCallables = Map.empty[String, TAT.Callable]
    aCallables.foreach {
      case (key, callable) =>
        bCallables.get(key) match {
          case None =>
            allCallables = allCallables + (key -> callable)

          // The comparision is done with "toString", because otherwise two
          // identical definitions are somehow, through the magic of Scala,
          // unequal.
          case Some(existing) if (existing != callable) =>
            BaseUtils.error(s"""|${key} appears with two different callable definitions
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

  private def bInfoFromDoc(doc : TAT.Document) : BInfo = {
    // Add source and adjuncts for main file
    val pathOrUrl = doc.sourceCode.toString
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

    val tasks : Vector[TAT.Task] = doc.elements.collect {
      case x : TAT.Task => x
    }
    val allCallables = (tasks ++ doc.workflow.toVector).map{
      callable => callable.name -> callable
    }.toMap

    BInfo(allCallables, sources, adjunctFiles)
  }


  // recurse into the imported packages
  //
  // Check the uniqueness of tasks, Workflows, and Types
  // merge everything into one bundle.
  private def dfsFlatten(tDoc : TAT.Document) : BInfo = {
    val imports : Vector[TAT.ImportDoc] = tDoc.elements.collect {
      case x : TAT.ImportDoc => x
    }

    val topLevelBInfo = bInfoFromDoc(tDoc)

    // dive into the imported documents and fold them into the top-level
    // document
    imports.foldLeft(topLevelBInfo) {
      case (accu : BInfo, imp) if imp.doc == None =>
        accu
      case (accu : BInfo, imp) =>
        val flatImpBInfo = dfsFlatten(imp.doc)
        BInfo(
          callables = mergeCallables(accu.callables, flatImpBInfo.callables),
          sources = accu.sources ++ flatImpBInfo.sources,
          adjunctFiles = accu.adjunctFiles ++ flatImpBInfo.adjunctFiles)
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
                                                   WomBundle,
                                                   Map[String, WorkflowSource],
                                                   Map[String, Vector[Adjuncts.AdjunctFile]]) = {
    // Resolves for:
    // - Where we run from
    // - Where the file is
    val allSources = HashMap.empty[String, WorkflowSource]
    val adjunctFiles = HashMap.empty[String, Vector[Adjuncts.AdjunctFile]]

    // parse and type check
    val mainAbsPath = mainFile.toAbsolutePath
    val srcDir = mainAbsPath.getParent()
    val opts =
      Options(typeChecking = WdlTypeCheckingRegime.Strict,
              antlr4Trace = false,
              localDirectories = imports.toVector :+ srcDir,
              verbosity = if (verbose) WdlVerbosity.Verbose else WdlVerbosity.Quiet)
    val parsers = new Parsers(opts)
    val typeInfer = new TypeInfer(opts)
    val mainDoc = parsers.parseDocument(WdlUtil.pathToUrl(mainAbsPath))
    val (tMainDoc, ctxTypes) = typeInfer.apply(mainDoc)

    val primaryCallable =
      tMainDoc.workflow match {
        case None =>
          val tasks : Vector[TAT.Task] = tMainDoc.elements.collect {
            case x : TAT.Task => x
          }
          if (tasks.size == 1)
            Some(tasks.head)
          else
            None
        case Some(wf) => Some(wf)
        case _ => None
      }

    // recurse into the imported packages
    //
    // Check the uniqueness of tasks, Workflows, and Types
    // merge everything into one bundle.
    val flatInfo : BInfo = dfsFlatten(tMainDoc)

    (languageFromVersion(tMainDoc.version.value),
     WomBundle(primaryCallable, flatInfo.callables, ctxTypes.aliases),
     flatInfo.sources,
     flatInfo.adjunctFiles)
  }

  // throw an exception if this WDL program is invalid
  def validateWdlCode(wdlWfSource: String): Unit = {
    val opts =
      Options(typeChecking = WdlTypeCheckingRegime.Strict,
              antlr4Trace = false,
              localDirectories = Vector.empty,
              verbosity = if (verbose) WdlVerbosity.Verbose else WdlVerbosity.Quiet)
    val lines = wdlWfSource.split("\n").toVector
    val sourceCode = WdlSourceCode(None, lines)
    val parser = new Parsers(opts).getParser(sourceCode)
    val doc = parser.parseDocument(sourceCode)
    val typeInfer = new TypeInfer(opts)
    val (_, _) = typeInfer.apply(doc)
  }


  // Parse a Workflow source file. Return the:
  //  * workflow definition
  //  * directory of tasks
  //  * directory of type aliases
  def parseWdlWorkflow(wfSource: String):
      (TAT.Workflow, Map[String, TAT.Task], Map[String, WdlTypes.T], TAT.Document) = {
    val opts =
      Options(typeChecking = WdlTypeCheckingRegime.Strict,
              antlr4Trace = false,
              localDirectories = Vector.empty,
              verbosity = if (verbose) WdlVerbosity.Verbose else WdlVerbosity.Quiet)
    val lines = wfSource.split("\n").toVector
    val sourceCode = WdlSourceCode(None, lines)
    val parser = new Parsers(opts).getParser(sourceCode)
    val doc = parser.parseDocument(sourceCode)
    val (tDoc, _) = new TypeInfer(opts).apply(doc)

    val tasks = tDoc.elements.collect{
      case task : TAT.Task => task.name -> task
    }.toMap
    val aliases = tDoc.elements.collect{
      case struct : TAT.StructDefinition =>
        struct.name -> WdlTypes.T_Struct(struct.name, struct.members)
    }.toMap
    val wf = tDoc.workflow match {
      case None => throw new RuntimeException("Sanity, this document should have a workflow")
      case Some(x) => x
    }
    (wf, tasks, aliases, tDoc)
  }

  def parseWdlTask(wfSource: String): (TAT.Task, Map[String, WdlTypes.T], TAT.Document) = {
    val opts =
      Options(typeChecking = WdlTypeCheckingRegime.Strict,
              antlr4Trace = false,
              localDirectories = Vector.empty,
              verbosity = if (verbose) WdlVerbosity.Verbose else WdlVerbosity.Quiet)
    val lines = wfSource.split("\n").toVector
    val sourceCode = WdlSourceCode(None, lines)
    val parser = new Parsers(opts).getParser(sourceCode)
    val doc = parser.parseDocument(sourceCode)
    val (tDoc, typeCtx) = new TypeInfer(opts).apply(doc)

    if (tDoc.workflow.isDefined)
      throw new Exception("a workflow that shouldn't be a member of this document")
    val tasks = tDoc.elements.collect{
      case task : TAT.Task => task
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
      case None => throw new Exception("found no callable")
      case Some(task : TAT.Task) => task
      case Some(wf) => throw new Exception("found a workflow ${wf.name} and not a task")
    }
  }

  // TODO: This code needs to be replaced

  // Look for the first task in the sequence of lines. If not found, return None.
  // If found, return the remaining lines, the name of the task, and the WDL source lines.
  //
  // Go through the lines, until you find a match for a start-line.
  // Look for expression that looks like this:
  //
  // task NAME {
  //  ...
  // }
  //
  // A complication is that the inner string could include curly bracket symbols
  // as well. There needs to be balanced number of left and right brackets. We ignore
  // this situation, and assume the end task marker is a closed curly bracket at
  // the beginning of the line. Note that this algorithm will make a mistake in this
  // case:
  //
  // task  NAME {
  //   Int a
  // command {
  //    ls -lR
  // }
  // }
  private def findWdlElement(lines: List[String],
                             elemStartLine: Regex,
                             elemEndLine: Regex): Option[(List[String], String, String)] = {
    var remaining: List[String] = lines
    var taskLines: Vector[String] = Vector.empty[String]
    var taskName: Option[String] = None

    while (!remaining.isEmpty) {
      // pop the first line
      val line = remaining.head
      remaining = remaining.tail

      taskName match {
        case None if (elemStartLine.pattern.matcher(line).matches) =>
          // hit the beginning of a task
          taskLines = Vector(line)

          // extract the task name
          val allMatches = elemStartLine.findAllMatchIn(line).toList
          if (allMatches.size != 1)
            throw new Exception(s"""|task definition appears twice in one line
                                    |
                                    |${line}
                                    |""".stripMargin)
          taskName = Some(allMatches(0).group(3))
        case None =>
          // lines before the task
          ()
        case Some(tn) if (elemEndLine.pattern.matcher(line).matches) =>
          // hit the end of the task
          taskLines :+= line
          return Some((remaining, tn, taskLines.mkString("\n")))
        case Some(_) =>
          // in the middle of a task
          taskLines :+= line
      }
    }
    return None
  }

  private def findNextTask(lines: List[String]): Option[(List[String], String, String)] = {
    val taskStartLine: Regex = "^(\\s*)task(\\s+)(\\w+)(\\s*)\\{".r
    val taskEndLine: Regex = "^}(\\s)*$".r
    findWdlElement(lines, taskStartLine, taskEndLine)
  }

  private def findNextWorkflow(lines: List[String]): Option[(List[String], String, String)] = {
    val workflowStartLine: Regex = "^(\\s*)workflow(\\s+)(\\w+)(\\s*)\\{".r
    val workflowEndLine: Regex = "^}(\\s)*$".r
    findWdlElement(lines, workflowStartLine, workflowEndLine)
  }

  // Go through one WDL source file, and return a map from task name
  // to its source code. Return an empty map if there are no tasks
  // in this file.
  def scanForTasks(sourceCode: String): Map[String, String] = {
    var lines = sourceCode.split("\n").toList
    val taskDir = HashMap.empty[String, String]

    while (!lines.isEmpty) {
      val retval = findNextTask(lines)

      // TODO: add a WOM syntax check that this is indeed a task.
      retval match {
        case None => return taskDir.toMap
        case Some((remainingLines, taskName, taskLines)) =>
          taskDir(taskName) = taskLines
          lines = remainingLines
      }
    }
    return taskDir.toMap
  }

  // Go through one WDL source file, and return a map from task name
  // to its source code. Return an empty map if there are no tasks
  // in this file.
  def scanForWorkflow(sourceCode: String): Option[(String, String)] = {
    val lines = sourceCode.split("\n").toList
    findNextWorkflow(lines) match {
      case None =>
        None
      case Some((remainingLines, wfName, wfLines)) =>
        Some((wfName, wfLines))
    }
  }
}
