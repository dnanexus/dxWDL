package dxWDL.compiler

// An in-memory representation of a WDL namespace, with direct and
// indirectly referenced namespaces.
//
import dxWDL.{Utils, Verbose, WdlPrettyPrinter}
import java.nio.file.Path
import scala.util.{Failure, Success}
import scala.collection.mutable.HashMap
import wdl.draft2.model._
import wom.values.WomValue

// Stripped down WDL module, without the surrounding namespace
case class WdlModule(workflow: Option[WdlWorkflow],
                     tasks: Vector[WdlTask],
                     wdlCode: String)

case class Context(allSourceFiles: HashMap[String, WdlModule],
                   toplevelWdlSourceFile: Path,
                   verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "NamespaceOps"

    // Extract the last part of the name from a filename path.
    //  "/A/B/C/foo.txt"   -> "foo.txt"
    private def getFilename(path: String) : String = {
        val i = path.lastIndexOf('/')
        if (i == -1)
            path
        else
            path.substring(i + 1)
    }
    def makeResolver: Draft2ImportResolver = {
        // Make an immutable copy of the source files, to avoid buggy
        // behavior.
        val allSources = allSourceFiles.foldLeft(Map.empty[String,String]) {
            case (accu, (k, WdlModule(_,_,wdlCode))) => accu + (k -> wdlCode)
        }.toMap
        // Index the source files by name, instead of their full path. This allows finding
        // source WDL files regardless of which import directory they come from.
        val allSourcesByName = allSources.map{ case (k,v) => getFilename(k) -> v}.toMap
        filename => allSourcesByName.get(getFilename(filename)) match {
            case None =>
                val knownFiles = allSources.keys
                throw new Exception(s"Unable to find ${filename}, known files are: ${knownFiles}")
            case Some(content) => content
        }
    }

    private def declarationIsInput(decl: Declaration) : Boolean = {
        decl.expression match {
            case None => true
            case Some(expr) if Utils.isExpressionConst(expr) => true
            case _ => false
        }
    }

    private def genTaskHeader(task: WdlTask) : String = {
        val taskHdr = WdlRewrite.taskGenEmpty(task.unqualifiedName)
        val inputs = task.declarations
            .filter{ decl => declarationIsInput(decl) }
            .map{ decl =>  WdlRewrite.declaration(decl.womType,
                                                  decl.unqualifiedName,
                                                  None) }
        val outputs = task.outputs.map{ to =>
            WdlRewrite.taskOutput(to.unqualifiedName, to.womType, taskHdr)
        }.toVector
        taskHdr.children = inputs ++ outputs

        try {
            WdlPrettyPrinter(false, None).apply(taskHdr, 0).mkString("\n")
        } catch {
            case e: Throwable =>
                val orgTaskLines = WdlPrettyPrinter(false, None).apply(task, 0).mkString("\n")
                Utils.trace(verbose2, s"""|Could not create a header for task:
                                          |${orgTaskLines}""".stripMargin)
                orgTaskLines
        }
    }

    private def genWorkflowHeader(wf: WdlWorkflow) : String = {
        val wfEmpty = WdlRewrite.workflowGenEmpty(wf.unqualifiedName)
        val inputs = wf.declarations
        val outputs = wf.outputs.map { wot =>
            val defaultVal:WomValue = WdlRewrite.genDefaultValueOfType(wot.womType)
            WorkflowOutput(wot.unqualifiedName,
                           wot.womType,
                           WdlExpression.fromString(defaultVal.toWomString),
                           WdlRewrite.INVALID_AST,
                           Some(wfEmpty))

        }
        val wfHdr = WdlRewrite.workflow(wfEmpty, inputs ++ outputs)

        try {
            WdlPrettyPrinter(false, None).apply(wfHdr, 0).mkString("\n")
        } catch {
            case e: Throwable =>
                val orgWfLines = WdlPrettyPrinter(false, None).apply(wf, 0).mkString("\n")
                Utils.trace(verbose2, s"""|Could not create a header for workflow:
                                          |${orgWfLines}""".stripMargin)
                orgWfLines
        }
    }

    private def genHeader(workflow: Option[WdlWorkflow],
                          tasks: Vector[WdlTask]) : String = {
        val taskHeaders: String = tasks.map{ task =>
            genTaskHeader(task)
        }.toVector.mkString("\n\n")
        workflow match {
            case None =>
                taskHeaders
            case Some(wf) =>
                val wfHdr = genWorkflowHeader(wf)
                taskHeaders + "\n\n" + wfHdr
        }

    }

    def addWdlSourceFile(name: String,
                         ns: WdlNamespace,
                         source: String,
                         header: Boolean) : Unit = {
        val workflow = ns match {
            case _:WdlNamespaceWithoutWorkflow => None
            case nswf:WdlNamespaceWithWorkflow => Some(nswf.workflow)
        }
        allSourceFiles(name) = WdlModule(workflow, ns.tasks.toVector, source)
    }

    def addWdlSourceFile(name: String,
                         workflow: WdlWorkflow,
                         tasks: Vector[WdlTask],
                         source: String,
                         header: Boolean) : Unit = {
        allSourceFiles(name) = WdlModule(Some(workflow), tasks, source)
    }

    def clear() : Unit = {
        allSourceFiles.clear
    }

    // prune files that have nothing we want to use
    def filterUnusedFiles(taskWfNames: Set[String]) : Unit = {
        val accessed:Map[String, Boolean] = allSourceFiles.map{
            case (filename, WdlModule(workflow, tasks, _)) =>
                val taskNames = tasks.map(_.unqualifiedName).toSet
                val definedTasksAndWorkflows: Set[String] = workflow match {
                    case None => taskNames
                    case Some(wf) => taskNames + wf.unqualifiedName
                }
                val retval = (taskWfNames intersect definedTasksAndWorkflows).size > 0
                filename -> retval
        }.toMap
        accessed.foreach{
            case (filename, false) => allSourceFiles.remove(filename)
            case (_,_) => ()
        }
    }


    // Take a context, and convert it into headers.
    // This improves the behavior of loadFromSource, because
    // we remove transitive imports, and reduce the import sizes.
    def makeHeaders : Context = {
        val hm = HashMap.empty[String, WdlModule]
        allSourceFiles.map{ case (name, WdlModule(workflow, tasks,_)) =>
            val hdr = genHeader(workflow, tasks)
            hm(name) = WdlModule(workflow, tasks, hdr)
        }
        this.copy(allSourceFiles = hm)
    }
}

object Context {
    def make(allWdlSources: Map[String, String],
             toplevelWdlSourceFile: Path,
             verbose: Verbose) : Context = {
        def resolver: Draft2ImportResolver = {
            filename => allWdlSources.get(filename) match {
                case None => throw new Exception(s"Unable to find ${filename}")
                case Some(content) => content
            }
        }

        val hm = HashMap.empty[String, WdlModule]
        allWdlSources.foreach{ case (name, wdlSourceCode) =>
            WdlNamespace.loadUsingSource(wdlSourceCode,
                                         None, Some(List(resolver))) match {
                case Failure(_) =>
                    // the WDL source does not parse, drop it
                    ()
                case Success(ns) =>
                    val workflow = ns match {
                        case _:WdlNamespaceWithoutWorkflow => None
                        case nswf:WdlNamespaceWithWorkflow => Some(nswf.workflow)
                    }
                    hm(name) = WdlModule(workflow, ns.tasks.toVector, wdlSourceCode)
            }
        }
        new Context(hm, toplevelWdlSourceFile, verbose)
    }
}
