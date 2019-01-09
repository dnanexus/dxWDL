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
import scala.util.matching.Regex
import wom.callable.{WorkflowDefinition}
import wom.core.WorkflowSource
import wom.executable.WomBundle

// Read, parse, and typecheck a WDL/CWL source file. This includes loading all imported files.
object ParseWomSourceFile {

    // allSources: A mapping from file URL to file source.
    //
    // Record all files accessed while traversing imports. We wrap
    // the Cromwell importers, and write down every new file.
    //
    // type ImportResolver = CheckedAtoB[String, WorkflowSource]
    private def fileRecorder(allSources: HashMap[String, WorkflowSource],
                             resolver: ImportResolver) : ImportResolver = {
        CheckedAtoB.fromErrorOr { path : String =>
            val fileContent = resolver(path)

            // convert an 'EitherOr' to 'Validated'
            fileContent match {
                case Left(errors) =>
                    Invalid(errors)
                case Right(v) =>
                    allSources(path) = v
                    Valid(v)
            }
        }
    }

    private def getBundle(mainFile: Path): (Language.Value, WomBundle, Map[String, WorkflowSource]) = {
        // Resolves for:
        // - Where we run from
        // - Where the file is
        val allSources =  HashMap.empty[String, WorkflowSource]

        lazy val importResolvers = List(
            fileRecorder(allSources, directoryResolver(
                DefaultPathBuilder.build(Paths.get(".")), allowEscapingDirectory = true
            )),
            fileRecorder(allSources, directoryResolver(
                DefaultPathBuilder.build(Paths.get(mainFile.toAbsolutePath.toFile.getParent)),
                allowEscapingDirectory = true
            )),
            fileRecorder(allSources, httpResolver)
        )

        val absPath = Paths.get(mainFile.toAbsolutePath.pathAsString)
        val mainFileContents = Files.readAllLines(absPath).asScala.mkString(System.lineSeparator())
        allSources(mainFile.toString) = mainFileContents

        val languageFactory = if (mainFile.name.toLowerCase().endsWith("wdl")) {
            if (mainFileContents.startsWith("version 1.0") ||
                    mainFileContents.startsWith("version draft-3")) {
                new WdlDraft3LanguageFactory(Map.empty)
            } else {
                new WdlDraft2LanguageFactory(Map.empty)
            }
        } else new CwlV1_0LanguageFactory(Map.empty)

        val bundleChk: Checked[WomBundle] =
            languageFactory.getWomBundle(mainFileContents, "{}", importResolvers, List(languageFactory))
        val bundle = bundleChk match {
            case Left(errors) => throw new Exception(s"""|WOM validation errors:
                                                         | ${errors}
                                                         |""".stripMargin)
            case Right(bundle) => bundle
        }
        val lang = (languageFactory.languageName.toLowerCase,
                    languageFactory.languageVersionName) match {
            case ("wdl", "draft-2") => Language.WDLvDraft2
            case ("wdl", "draft-3") => Language.WDLvDraft2
            case ("wdl", "1.0") => Language.WDLv1_0
            case ("cwl", "1.0") => Language.CWLv1_0
            case (l,v) => throw new Exception(s"Unsupported language (${l}) version (${v})")
        }
        (lang, bundle, allSources.toMap)
    }

    def apply(sourcePath: java.nio.file.Path) : (Language.Value, WomBundle, Map[String, WorkflowSource]) = {
        val src : Path = DefaultPathBuilder.build(sourcePath)
        val (lang, bundle, allSources) = getBundle(src)
        lang match {
            case Language.CWLv1_0 =>
                throw new Exception("CWL is not handled at the moment, only WDL is supported")
            case _ => ()
        }
        (lang, bundle, allSources)
    }


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
                               elemStartLine : Regex,
                               elemEndLine : Regex) : Option[(List[String], String, String)] = {
        var remaining: List[String] = lines
        var taskLines : Vector[String] = Vector.empty[String]
        var taskName : Option[String] = None

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


    private def findNextTask(lines: List[String]) : Option[(List[String], String, String)] = {
        val taskStartLine: Regex = "^(\\s*)task(\\s+)(\\w+)(\\s+)\\{".r
        val taskEndLine: Regex = "^}(\\s)*$".r
        findWdlElement(lines, taskStartLine, taskEndLine)
    }

    private def findNextWorkflow(lines: List[String]) : Option[(List[String], String, String)] = {
        val workflowStartLine: Regex = "^(\\s*)workflow(\\s+)(\\w+)(\\s+)\\{".r
        val workflowEndLine: Regex = "^}(\\s)*$".r
        findWdlElement(lines, workflowStartLine, workflowEndLine)
    }

    // add "version 1.0" or such to the WDL source code
    private def addLanguageHeader(language: Language.Value,
                                  sourceCode: String) : String = {
        val header = language match {
            case Language.WDLvDraft2 => ""
            case Language.WDLv1_0 => "version 1.0"
            case Language.CWLv1_0 => throw new NotImplementedError("CWL")
        }

        //System.out.println(s"language header = ${header}")
        if (header.isEmpty) {
            sourceCode
        } else {
            s"""|${header}
                |
                |${sourceCode}""".stripMargin
        }
    }

    // Go through one WDL source file, and return a map from task name
    // to its source code. Return an empty map if there are no tasks
    // in this file.
    def scanForTasks(language: Language.Value,
                     sourceCode: String) : Map[String, String] = {
        var lines = sourceCode.split("\n").toList
        val taskDir = HashMap.empty[String, String]

        while (!lines.isEmpty) {
            val retval = findNextTask(lines)

            // TODO: add a WOM syntax check that this is indeed a task.
            retval match {
                case None => return taskDir.toMap
                case Some((remainingLines, taskName, taskLines)) =>
                    val taskWithLanguageVersion : String = addLanguageHeader(language, taskLines)
                    taskDir(taskName) = taskWithLanguageVersion
                    lines = remainingLines
            }
        }
        return taskDir.toMap
    }

    // Go through one WDL source file, and return a map from task name
    // to its source code. Return an empty map if there are no tasks
    // in this file.
    def scanForWorkflow(language: Language.Value,
                        sourceCode: String) : Option[(String, String)] = {
        val lines = sourceCode.split("\n").toList
        findNextWorkflow(lines) match {
            case None =>
                None
            case Some((remainingLines, wfName, wfLines)) =>
                Some((wfName, wfLines))
        }
    }

    // throw an exception if the workflow source is not valid WDL 1.0
    def validateWdlWorkflow(wdlWfSource: String,
                            language: Language.Value) : Unit = {
        val languageFactory = language match {
            case Language.WDLv1_0 =>
                new WdlDraft3LanguageFactory(Map.empty)
            case Language.WDLvDraft2 =>
                new WdlDraft2LanguageFactory(Map.empty)
            case other =>
                throw new Exception(s"Unsupported language ${other}")
        }

        val bundleChk: Checked[WomBundle] =
            languageFactory.getWomBundle(wdlWfSource, "{}", List.empty, List.empty)
        bundleChk match {
            case Left(errors) =>
                Utils.error("Found Errors in generated WDL source")
                Utils.error(wdlWfSource + "\n")
                throw new Exception(s"""|WOM validation errors:
                                        | ${errors}
                                        |""".stripMargin)
            case Right(bundle) =>
                // Passed WOM validation
                ()
        }
    }

    def parseWdlWorkflow(wfSource: String) : WorkflowDefinition = {
        val languageFactory =
            if (wfSource.startsWith("version 1.0") ||
                    wfSource.startsWith("version draft-3")) {
                new WdlDraft3LanguageFactory(Map.empty)
            } else {
                new WdlDraft2LanguageFactory(Map.empty)
            }

        val bundleChk: Checked[WomBundle] =
            languageFactory.getWomBundle(wfSource, "{}", List.empty, List(languageFactory))
        val womBundle = bundleChk match {
            case Left(errors) => throw new Exception(s"""|WOM validation errors:
                                                         | ${errors}
                                                         |""".stripMargin)
            case Right(bundle) => bundle
        }
        womBundle.primaryCallable match {
            case Some(wf: WorkflowDefinition) => wf
            case _ => throw new Exception("Could not find the workflow in the source")
        }
    }
}
