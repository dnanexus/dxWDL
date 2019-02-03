package dxWDL.util


import cats.data.Validated.{Invalid, Valid}
import common.Checked
import common.transforms.CheckedAtoB
import com.typesafe.config.ConfigFactory
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.languages.util.ImportResolver._
import java.nio.file.{Files, Paths}
import languages.cwl.CwlV1_0LanguageFactory
import languages.wdl.draft2.WdlDraft2LanguageFactory
import languages.wdl.draft3.WdlDraft3LanguageFactory
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.matching.Regex
import wom.callable.{CallableTaskDefinition, ExecutableTaskDefinition, WorkflowDefinition}
import wom.core.WorkflowSource
import wom.executable.WomBundle

// Read, parse, and typecheck a WDL/CWL source file. This includes loading all imported files.
object ParseWomSourceFile {

    // allSources: A mapping from file URL to file source.
    //
    // Record all files accessed while traversing imports. We wrap
    // the Cromwell importers, and write down every new file.
    //
    case class FileRecorderResolver(
        allSources: HashMap[String, WorkflowSource],
        lower: ImportResolver) extends ImportResolver {

        def name = lower.name

        protected def innerResolver(path: String,
                                    currentResolvers: List[ImportResolver])
                : Checked[ResolvedImportBundle] = ???

        override def resolver: CheckedAtoB[ImportResolutionRequest, ResolvedImportBundle] = {
            CheckedAtoB.fromErrorOr {
                case request : ImportResolutionRequest =>
                    val path = request.toResolve
                    val bundleChk: Checked[ResolvedImportBundle] = lower.resolver(request)
                    bundleChk match {
                        case Left(errors) =>
                            Invalid(errors)
                        case Right(bundle) =>
                            val fileContent = bundle.source
                            // convert an 'EitherOr' to 'Validated'
                            allSources(path) = fileContent
                            Valid(bundle)
                    }
            }
        }
    }

    private def fileRecorder(allSources: HashMap[String, WorkflowSource],
                             resolver: ImportResolver) : ImportResolver =
        new FileRecorderResolver(allSources, resolver)

    private def getBundle(mainFile: Path): (Language.Value,
                                            WomBundle,
                                            Map[String, WorkflowSource],
                                            Vector[WomBundle]) = {
        // Resolves for:
        // - Where we run from
        // - Where the file is
        val allSources =  HashMap.empty[String, WorkflowSource]

        val absPath = Paths.get(mainFile.toAbsolutePath.pathAsString)
        val mainFileContents = Files.readAllLines(absPath).asScala.mkString(System.lineSeparator())

        // We need to get all the sources sources
        val importResolvers: List[ImportResolver] =
            DirectoryResolver.localFilesystemResolvers(Some(mainFile)) :+ HttpResolver(relativeTo = None)
        val importResolversRecorded: List[ImportResolver] =
            importResolvers.map{ impr => fileRecorder(allSources, impr) }

        val languageFactory =
            List(
                new WdlDraft3LanguageFactory(ConfigFactory.empty()),
                new CwlV1_0LanguageFactory(ConfigFactory.empty()))
                .find(_.looksParsable(mainFileContents))
                .getOrElse(new WdlDraft2LanguageFactory(ConfigFactory.empty())
            )
        val bundleChk: Checked[WomBundle] =
            languageFactory.getWomBundle(mainFileContents, "{}", importResolversRecorded, List(languageFactory))

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

        // build wom bundles for all the referenced files
        val subBundles : Vector[WomBundle] = allSources.map{
            case (path, wfSource) =>
                languageFactory.getWomBundle(wfSource,
                                             "{}",
                                             importResolvers,
                                             List(languageFactory)) match {
                    case Left(errors) => throw new Exception(s"""|WOM validation errors:
                                                                 | ${errors}
                                                                 |""".stripMargin)
                    case Right(bundle) => bundle
                }
        }.toVector

        allSources(mainFile.toString) = mainFileContents
        (lang, bundle, allSources.toMap, subBundles)
    }

    def apply(sourcePath: java.nio.file.Path) : (Language.Value,
                                                 WomBundle,
                                                 Map[String, WorkflowSource],
                                                 Vector[WomBundle]) = {
        val src : Path = DefaultPathBuilder.build(sourcePath)
        val (lang, bundle, allSources, subBundles) = getBundle(src)
        lang match {
            case Language.CWLv1_0 =>
                throw new Exception("CWL is not handled at the moment, only WDL is supported")
            case _ => ()
        }
        (lang, bundle, allSources, subBundles)
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

    private def matchPatterAtMostOnce(ptrn: Regex,
                                      line: String) : Option[Regex.Match] = {
        val allMatches = ptrn.findAllMatchIn(line).toList
        allMatches.size match {
            case 0 => None
            case 1 => Some(allMatches(0))
            case _ =>
                throw new Exception(s"""|bad line, pattern appears twice
                                        |
                                        |${line}
                                        |""".stripMargin)
        }
    }

    // Scan the workflow for calls, and return the list of call names.
    //
    // For example, scanning workflow foo
    //
    // workflow foo {
    //   Float x
    //   call A
    //   call A as A2
    // }
    //
    // would return : [A, A2]
    def scanForCalls(wdlWfSource: String) : Map[String, Int] = {
        // The "callLine" regular expression will match occurrences of
        // "callAsLine", so we are careful to find all aliased calls first
        val callLine: Regex = "^(\\s*)call(\\s+)(\\w+)".r
        val callAsLine: Regex = "^(\\s*)call(\\s+)(\\w+)(\\s+)as(\\s+)(\\w+)".r
        val wfLines = wdlWfSource.split("\n").toList

        val calls =  HashMap.empty[String, Int]
        var linesWithCalls = Set.empty[Int]
        for (lineNr <- 0 until wfLines.length) {
            val line = wfLines(lineNr)

            // is this an aliased call?
            //   call A as Av1 { input: ... }
            //   calls A as Av1
            matchPatterAtMostOnce(callAsLine, line) match {
                case Some(m) =>
                    val callName = m.group(6)
                    calls(callName) = lineNr
                    linesWithCalls += lineNr
                case None =>
                    calls
            }
        }

        for (lineNr <- 0 until wfLines.length) {
            val line = wfLines(lineNr)

            if (!(linesWithCalls contains lineNr)) {
                // is this a simple call?
                //   call A { input: ... }
                //   call A
                matchPatterAtMostOnce(callLine, line) match {
                    case Some(m) =>
                        val callName = m.group(3)
                        calls.get(callName) match {
                            case None =>
                                calls(callName) = lineNr
                                linesWithCalls += lineNr
                            case Some(_) =>
                                // already matched to an aliased call.
                                ()
                        }
                    case None => ()
                }
            }
        }

        // make sure there are no duplicate lines
        val sourceLines = calls.values.toVector
        assert(sourceLines.size == sourceLines.toSet.size)
        calls.toMap
    }

    // throw an exception if the workflow source is not valid WDL 1.0
    def validateWdlWorkflow(wdlWfSource: String,
                            language: Language.Value) : Unit = {
        val languageFactory = language match {
            case Language.WDLv1_0 =>
                new WdlDraft3LanguageFactory(ConfigFactory.empty())
            case Language.WDLvDraft2 =>
                new WdlDraft2LanguageFactory(ConfigFactory.empty())
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
                new WdlDraft3LanguageFactory(ConfigFactory.empty())
            } else {
                new WdlDraft2LanguageFactory(ConfigFactory.empty())
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

    // Extract the only task from a namespace
    def getMainTask(bundle: WomBundle) : CallableTaskDefinition = {
        // check if the primary is nonempty
        val task: Option[CallableTaskDefinition] = bundle.primaryCallable match  {
            case Some(task : CallableTaskDefinition) => Some(task)
            case Some(exec : ExecutableTaskDefinition) => Some(exec.callableTaskDefinition)
            case _ => None
        }
        task match {
            case Some(x) => x
            case None =>
                // primary is empty, check the allCallables map
                if (bundle.allCallables.size != 1)
                    throw new Exception("WDL file must contains exactly one task")
                val (_, task) = bundle.allCallables.head
                task match {
                    case task : CallableTaskDefinition => task
                    case exec : ExecutableTaskDefinition => exec.callableTaskDefinition
                    case _ => throw new Exception("Cannot find task inside WDL file")
                }
        }
    }

    def parseWdlTask(wfSource: String) : CallableTaskDefinition = {
        val languageFactory =
            if (wfSource.startsWith("version 1.0") ||
                    wfSource.startsWith("version draft-3")) {
                new WdlDraft3LanguageFactory(ConfigFactory.empty())
            } else {
                new WdlDraft2LanguageFactory(ConfigFactory.empty())
            }

        val bundleChk: Checked[WomBundle] =
            languageFactory.getWomBundle(wfSource, "{}", List.empty, List(languageFactory))
        val womBundle = bundleChk match {
            case Left(errors) => throw new Exception(s"""|WOM validation errors:
                                                         | ${errors}
                                                         |""".stripMargin)
            case Right(bundle) => bundle
        }
        getMainTask(womBundle)
    }
}
