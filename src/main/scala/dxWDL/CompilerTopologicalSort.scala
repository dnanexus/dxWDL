/**
  *  Preprocessing pass, simplify the original WDL.
  *
  *  Instead of handling expressions in calls directly,
  *  we lift the expressions, generate auxiliary variables, and
  *  call the task with values or variables (no expressions).
  *
  *  A difficulty we face here, is avoiding using internal
  *  representations used by wdl4s. For example, we want to reorganize
  *  scatter blocks, however, we cannot create valid new wdl4s scatter
  *  blocks. Instead, we pretty print a new workflow and then load it.
  */
package dxWDL

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Path, Paths}
import scala.collection.mutable.Queue
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.{Call, Declaration, Scatter, Scope,
    Task, TaskCall, TaskOutput,
    WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow,
    Workflow, WorkflowCall, WdlSource}
import wdl4s.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions

object CompilerTopologicalSort {
    // TODO: move common procs to a common place

    // Add a suffix to a filename, before the regular suffix. For example:
    //  xxx.wdl -> xxx.sorted.wdl
    def addFilenameSuffix(src: Path, secondSuffix: String) : String = {
        val fName = src.toFile().getName()
        val index = fName.lastIndexOf('.')
        if (index == -1) {
            fName + secondSuffix
        }
        else {
            val prefix = fName.substring(0, index)
            val suffix = fName.substring(index)
            prefix + secondSuffix + suffix
        }
    }

    case class State(cef: CompilerErrorFormatter,
                     terminalMap: Map[Terminal, WdlSource],
                     verbose: Boolean)


    // Generic topological sorting procedure
    // Scala code from https://gist.github.com/ThiporKong/4399695
    def tsort[A](edges: Traversable[(A, A)]): Iterable[A] = {
        def tsort(toPreds: Map[A, Set[A]], done: Iterable[A]): Iterable[A] = {
            val (noPreds, hasPreds) = toPreds.partition { _._2.isEmpty }
            if (noPreds.isEmpty) {
                // Every DAG contains a vertex with no incoming edges, otherwise
                // there exists at least one directed cycle in the DAG.
                if (hasPreds.isEmpty) done else sys.error("ERROR: worfklow contains at least one directed cycle.")
            } else {
                val found = noPreds.map { _._1 }
                tsort(hasPreds.mapValues { _ -- found }, done ++ found)
            }
        }

        val toPred = edges.foldLeft(Map[A, Set[A]]()) { (acc, e) =>
            acc + (e._1 -> acc.getOrElse(e._1, Set())) + (e._2 -> (acc.getOrElse(e._2, Set()) + e._1))
        }
        tsort(toPred, Seq())
    }

    // Generate a list of edges corresponding to dependencies for this call
    // e.g. suppose call x has dependencies a,b,c.  This procedure will generate:
    // [(a,x), (b,x), (c,x)]
    def processCall(call: Call, dummy: Scope, cState: State) : Vector[(Scope, Scope)] = {
        if (dummy != call) {
            call.upstream.map { parent =>
               (parent.asInstanceOf[Scope], call.asInstanceOf[Scope])
            }.toVector
        } else {
            Vector()
        }
    }

    def processScatter(scatter: Scatter, dummy: Scope, cState: State) : Vector[(Scope, Scope)] = {
        if (dummy != scatter) {
            Vector((dummy, scatter))
        } else {
            Vector()
        }
    }

    def processDeclaration(decl: Declaration, dummy: Scope, cState: State) : Vector[(Scope, Scope)] = {
        if (dummy != decl) {
            Vector((dummy, decl))
        } else {
            Vector()
        }
    }

    // Generic topological sorting
    def sortWorkflow(wf: Workflow, taskLines: Vector[String], cState:State) : Vector[String] = {
        // Create workflow graph sequence of edges for topological sorting
        // Nodes in the graph (Scopes) can be a call, a scatter, or a declaration
        val edges : Seq[(Scope, Scope)] = wf.children.map {
            // TODO: handle scatter and declaration
            case call: Call => processCall(call, wf.children.last, cState)
            case scatter: Scatter => processScatter(scatter, wf.children.last, cState)
            case decl : Declaration => processDeclaration(decl, wf.children.last, cState)
            case x =>
                throw new Exception(cState.cef.notCurrentlySupported(x.ast,
                                                                     "workflow element"))
            // NOTE FOR NOW:
            // Treat any 'unhandled' node as dependent on the last task
            // Note this does not mean that they are actually dependent
            // on the task, but just guarantees that they will be placed
            // at the end of the sorted list of 'handled' nodes. This comment
            // should be removed after all nodes are handled properly
        }.flatten

        // Topologically sort graph or return error that workflow contains a cycle
        val sortedNodes = tsort(edges)

        // pretty print the workflow. The output
        // must be readable by the standard WDL compiler.
        val elemsPp : Vector[String] = sortedNodes.map {
            case call: Call => WdlPrettyPrinter.apply(call, 1)
            case decl: Declaration => WdlPrettyPrinter.apply(decl, 1)
            case ssc: Scatter => WdlPrettyPrinter.apply(ssc, 1)
            case x =>
                throw new Exception(cState.cef.notCurrentlySupported(x.ast,
                                                                     "workflow element"))
        }.flatten.toVector
        val wfLines = WdlPrettyPrinter.buildBlock(s"workflow ${wf.unqualifiedName}", elemsPp, 0)
        taskLines ++ wfLines
    }

    def apply(wdlSourceFile : Path,
              verbose: Boolean) : Path = {
        Utils.trace(verbose, "Topological sort pass")

        val ns = WdlNamespace.loadUsingPath(wdlSourceFile, None, None).get
        val tm = ns.terminalMap
        val cef = new CompilerErrorFormatter(tm)
        val cState = State(cef, tm, verbose)

        // Create a new file to hold the result.
        //
        // Assuming the source file is xxx.wdl, the new name will
        // be xxx.sorted.wdl.
        val trgName: String = addFilenameSuffix(wdlSourceFile, ".sorted")
        val sortedWdl = Utils.appCompileDirPath.resolve(trgName).toFile
        val fos = new FileWriter(sortedWdl)
        val pw = new PrintWriter(fos)

        // Process the original WDL file,
        // Do not modify the tasks
        val taskLines: Vector[String] = ns.tasks.map{ task =>
            WdlPrettyPrinter.apply(task, 0) :+ "\n"
        }.flatten.toVector
        val rewrittenNs = ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                sortWorkflow(nswf.workflow, taskLines, cState)
            case _ => taskLines
        }

        // write the output to xxx.sorted.wdl
        pw.println(rewrittenNs.mkString("\n"))

        pw.flush()
        pw.close()
        Utils.trace(verbose, s"Wrote sorted WDL to ${sortedWdl.toString}")
        sortedWdl.toPath
    }
}
