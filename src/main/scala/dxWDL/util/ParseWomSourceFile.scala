package dxWDL.util


import cats.data.Validated.{Invalid, Valid}
import common.Checked
import common.transforms.CheckedAtoB
import common.validation.ErrorOr._
import com.typesafe.config.ConfigFactory
import cromwell.core.path.{DefaultPathBuilder}
import cromwell.languages.LanguageFactory
import cromwell.languages.util.ImportResolver._
import java.nio.file.{Files, Path, Paths}
import languages.cwl.CwlV1_0LanguageFactory
import languages.wdl.draft2.WdlDraft2LanguageFactory
import languages.wdl.draft3.WdlDraft3LanguageFactory
import languages.wdl.biscayne.WdlBiscayneLanguageFactory
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.matching.Regex

import wom.SourceFileLocation
import wom.callable.{CallableTaskDefinition, ExecutableTaskDefinition, WorkflowDefinition}
import wom.core.WorkflowSource
import wom.executable.WomBundle
import wom.graph.{CallNode, Graph, GraphNode}
import wom.types._

import dxWDL.base.{Language, Utils}

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

        override def cleanupIfNecessary(): ErrorOr[Unit] = ???
    }

    private def fileRecorder(allSources: HashMap[String, WorkflowSource],
                             resolver: ImportResolver) : ImportResolver =
        new FileRecorderResolver(allSources, resolver)

    // Figure out which language variant is used in the source code. WDL/CWL, and
    // which version.
    private def getLanguageFactory(wfSource : String) : LanguageFactory = {
        val langPossibilties = List(
            new WdlDraft3LanguageFactory(ConfigFactory.empty()),
            new WdlBiscayneLanguageFactory(ConfigFactory.empty()),
            new CwlV1_0LanguageFactory(ConfigFactory.empty()))

        langPossibilties
            .find(_.looksParsable(wfSource))
            .getOrElse(new WdlDraft2LanguageFactory(ConfigFactory.empty()))
    }

    private def getBundle(mainFile: Path,
                          imports: List[Path]): (Language.Value,
                                                 WomBundle,
                                                 Map[String, WorkflowSource],
                                                 Vector[WomBundle]) = {
        // Resolves for:
        // - Where we run from
        // - Where the file is
        val allSources =  HashMap.empty[String, WorkflowSource]

        val absPath = Paths.get(mainFile.toAbsolutePath.toString)
        val mainFileContents = Files.readAllLines(absPath).asScala.mkString(System.lineSeparator())

        // We need to get all the WDL sources, so we could analyze them
        val mainFileResolvers =
            DirectoryResolver.localFilesystemResolvers(Some(DefaultPathBuilder.build(mainFile)))

        // look for source files in each of the import directories
        val fileImportResolvers : List[ImportResolver] = imports.map{ p =>
            val p2 : cromwell.core.path.Path = DefaultPathBuilder.build(p.toAbsolutePath)
            DirectoryResolver(p2, None, None, false)
        }
        val importResolvers: List[ImportResolver] =
            mainFileResolvers ++ fileImportResolvers :+ HttpResolver(relativeTo = None)

        // Allow http addresses when importing
        val importResolversRecorded: List[ImportResolver] =
            importResolvers.map{ impr => fileRecorder(allSources, impr) }

        val languageFactory = getLanguageFactory(mainFileContents)
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
            case ("wdl", "draft-3") => Language.WDLv1_0
            case ("wdl", "1.0") => Language.WDLv1_0
            case ("wdl", "development") => Language.WDLv2_0
            case ("wdl", "Biscayne") => Language.WDLv2_0
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

    def apply(sourcePath: Path,
              imports: List[Path]) : (Language.Value,
                                      WomBundle,
                                      Map[String, WorkflowSource],
                                      Vector[WomBundle]) = {
        val (lang, bundle, allSources, subBundles) = getBundle(sourcePath, imports)
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
        val taskStartLine: Regex = "^(\\s*)task(\\s+)(\\w+)(\\s*)\\{".r
        val taskEndLine: Regex = "^}(\\s)*$".r
        findWdlElement(lines, taskStartLine, taskEndLine)
    }

    private def findNextWorkflow(lines: List[String]) : Option[(List[String], String, String)] = {
        val workflowStartLine: Regex = "^(\\s*)workflow(\\s+)(\\w+)(\\s*)\\{".r
        val workflowEndLine: Regex = "^}(\\s)*$".r
        findWdlElement(lines, workflowStartLine, workflowEndLine)
    }

    // Go through one WDL source file, and return a map from task name
    // to its source code. Return an empty map if there are no tasks
    // in this file.
    def scanForTasks(sourceCode: String) : Map[String, String] = {
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
    def scanForWorkflow(sourceCode: String) : Option[(String, String)] = {
        val lines = sourceCode.split("\n").toList
        findNextWorkflow(lines) match {
            case None =>
                None
            case Some((remainingLines, wfName, wfLines)) =>
                Some((wfName, wfLines))
        }
    }

    // Scan the workflow for calls, and return a mapping from call name
    // to source line number.
    //
    // For example, scanning workflow foo
    //
    // workflow foo {
    //   Float x
    //   call A
    //   call A as A2
    // }
    //
    // would return : Map(A -> 3, A2 -> 4)
    //
    // When a workflow imports other namespaces, the syntax for calls
    // is NAMESPACE.CALL_NAME. For example:
    //
    // import "library.wdl" as lib
    // workflow foo {
    //   call lib.Multiply as mul { ... }
    // }
    //
    private def allNodesDoNotDescendIntoSubWorkflows(graph: Graph): Set[GraphNode] = {
        graph.nodes ++
        graph.scatters.flatMap(x => allNodesDoNotDescendIntoSubWorkflows(x.innerGraph)) ++
        graph.conditionals.flatMap(x => allNodesDoNotDescendIntoSubWorkflows(x.innerGraph)) ++
        graph.workflowCalls
    }

    def scanForCalls(graph: Graph,
                     wfSource: String) : Vector[String] = {
        val nodes = allNodesDoNotDescendIntoSubWorkflows(graph)
        val callToSrcLine : Map[String, Int]  = nodes.collect{
            case cNode : CallNode =>
                val callUnqualifiedName = Utils.getUnqualifiedName(cNode.localName)
                val lineNum = cNode.sourceLocation match {
                    case None => throw new Exception("No source line for call ${cNode}")
                    case Some(SourceFileLocation(x)) => x
                }
                callUnqualifiedName -> lineNum
        }.toMap

        // sort from low to high according to the source lines.
        val callsLoToHi : Vector[(String, Int)] = callToSrcLine.toVector.sortBy(_._2)
        callsLoToHi.map{ case (x,_) => x }
    }

    // throw an exception if the workflow source is not valid WDL 1.0
    def validateWdlCode(wdlWfSource: String,
                        language: Language.Value) : Unit = {
        val languageFactory = language match {
            case Language.WDLv2_0 =>
                new WdlBiscayneLanguageFactory(ConfigFactory.empty())
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

    // Parse a Workflow source file. Return the:
    //  * workflow definition
    //  * directory of tasks
    //  * directory of type aliases
    def parseWdlWorkflow(wfSource: String) : (WorkflowDefinition,
                                              Map[String, CallableTaskDefinition],
                                              Map[String, WomType])= {
        val languageFactory = getLanguageFactory(wfSource)
        val bundleChk: Checked[WomBundle] =
            languageFactory.getWomBundle(wfSource, "{}", List.empty, List(languageFactory))
        val womBundle = bundleChk match {
            case Left(errors) => throw new Exception(s"""|WOM validation errors:
                                                         | ${errors}
                                                         |""".stripMargin)
            case Right(bundle) => bundle
        }
        val wf = womBundle.primaryCallable match {
            case Some(wf: WorkflowDefinition) => wf
            case _ => throw new Exception("Could not find the workflow in the source")
        }
        val taskDir = womBundle.allCallables.flatMap{
            case (name, callable) =>
                callable match {
                    case task : CallableTaskDefinition => Some(name -> task)
                    case exec : ExecutableTaskDefinition => Some(name -> exec.callableTaskDefinition)
                    case _ => None
                }
        }.toMap
        (wf, taskDir, womBundle.typeAliases)
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

    def parseWdlTask(wfSource: String) : (CallableTaskDefinition, Map[String, WomType]) = {
        val languageFactory = getLanguageFactory(wfSource)
        val bundleChk: Checked[WomBundle] =
            languageFactory.getWomBundle(wfSource, "{}", List.empty, List(languageFactory))
        val womBundle = bundleChk match {
            case Left(errors) => throw new Exception(s"""|WOM validation errors:
                                                         | ${errors}
                                                         |""".stripMargin)
            case Right(bundle) => bundle
        }
        (getMainTask(womBundle), womBundle.typeAliases)
    }
}
