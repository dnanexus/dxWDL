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
import wom.types.WomType

case class Context(allSourceFiles: HashMap[String, String],
                   toplevelWdlSourceFile: Path,
                   verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "NamespaceOps"

    def makeResolver: ImportResolver = {
        // Make an immutable copy of the source files, to avoid buggy
        // behavior.
        val allSources = allSourceFiles.foldLeft(Map.empty[String,String]) {
            case (accu, (k,v)) => accu + (k -> v)
        }.toMap
        filename => allSources.get(filename) match {
            case None => throw new Exception(s"Unable to find ${filename}")
            case Some(content) => content
        }
    }

    def addWdlSourceFile(name: String, sourceCode: String) : Unit = {
        allSourceFiles(name) = sourceCode
    }

    def setWdlSourceFiles(filename:String, wdlSourceCode:String) : Unit = {
        allSourceFiles.clear
        allSourceFiles(filename) = wdlSourceCode
    }

    // prune files that have nothing we want to use
    def filterUnusedFiles(taskWfNames: Set[String]) : Unit = {
        val resolver = makeResolver
        val accessed:Map[String, Boolean] = allSourceFiles.map{
            case (filename, wdlSourceCode) =>
                WdlNamespace.loadUsingSource(wdlSourceCode,
                                             None, Some(List(resolver))) match {
                    case Failure(_) =>
                        // the WDL source does not parse, drop it
                        filename -> false
                    case Success(ns) =>
                        val taskNames = ns.tasks.map(_.unqualifiedName).toSet
                        val definedTasksAndWorkflows: Set[String] = ns match {
                            case nswf:WdlNamespaceWithWorkflow =>
                                taskNames + nswf.workflow.unqualifiedName
                            case _ => taskNames
                        }
                        val retval = (taskWfNames intersect definedTasksAndWorkflows).size > 0
                        filename -> retval
                }
        }.toMap
        accessed.foreach{
            case (filename, false) => allSourceFiles.remove(filename)
            case (_,_) => ()
        }
    }

    private def declarationIsInput(decl: Declaration) : Boolean = {
        decl.expression match {
            case None => true
            case Some(expr) if Utils.isExpressionConst(expr) => true
            case _ => false
        }
    }

    def genDefaultValue(womType: WomType) : WdlExpression = {
        val defaultVal:WomValue = WdlRewrite.genDefaultValueOfType(womType)
        WdlExpression.fromString(defaultVal.toWomString)
    }

    def genTaskHeader(task: WdlTask) : String = {
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

    def genWorkflowHeader(wf: WdlWorkflow) : String = {
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

    def genHeader(ns: WdlNamespace) : String = {
        val taskHeaders: String = ns.tasks.map{ task =>
            genTaskHeader(task)
        }.toVector.mkString("\n\n")
        ns match {
            case _:WdlNamespaceWithoutWorkflow =>
                taskHeaders
            case nswf:WdlNamespaceWithWorkflow =>
                val wfHdr = genWorkflowHeader(nswf.workflow)
                taskHeaders + "\n\n" + wfHdr
        }

    }

    // Take a context, and convert it into headers alone
    def makeHeaders : Context = {
        val resolver = makeResolver
        val hm = HashMap.empty[String, String]
        allSourceFiles.map{ case (name, wdlSourceCode) =>
            val ns = WdlNamespace.loadUsingSource(
                wdlSourceCode, None, Some(List(resolver))
            ).get
            hm(name) = genHeader(ns)
        }
        this.copy(allSourceFiles = hm)
    }
}

object Context {
    def make(allWdlSources: Map[String, String],
             toplevelWdlSourceFile: Path,
             verbose: Verbose) : Context = {
        val hm = HashMap.empty[String, String]
        allWdlSources.foreach{ case (name, src) => hm(name) = src}
        new Context(hm, toplevelWdlSourceFile, verbose)
    }
}
