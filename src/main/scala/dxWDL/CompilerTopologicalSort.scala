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
    Workflow, WorkflowCall, WdlSource, GraphNode}
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

    // Internal procedure to sort children nodes at a particular level in the AST
    def tsortASTnodes(nodes: Seq[Scope], cState:State, recursionDepth: Int) : Seq[Scope] = {
        val indent = " "*recursionDepth*4

        val recursedNodes = nodes.map {
             case scatter : Scatter => {
                 //val newScatter = Scatter(scatter.index, scatter.item, scatter.collection, scatter.ast)
                 Utils.trace(cState.verbose, indent+"Recursively performing toological sort on scatter: " + scatter.fullyQualifiedName)
                 //newScatter.children = tsortASTnodes(scatter.children, cState, recursionDepth + 1)
                 val newScatter = WdlRewrite.scatter(scatter,
                    tsortASTnodes(scatter.children, cState, recursionDepth + 1))
                 Utils.trace(cState.verbose, indent+"End recursion")
                 newScatter.asInstanceOf[GraphNode]
             }
             case anyOtherNode => anyOtherNode.asInstanceOf[GraphNode]
         }

        // Create workflow graph sequence of edges for topological sorting
        // Nodes in the graph (Scopes) can be a call, a scatter, or a declaration

        // Create a mapping from a child of a scatter node to the root level scatter
        // node it belongs to. This allows for a root level scatter node to be placed
        // after its last dependency.

        val scatterRoot : Map[GraphNode, Scatter] = recursedNodes.map {
            case scat: Scatter => scat.descendants.map { d => (d.asInstanceOf[GraphNode], scat) }
            case _ => Set[(GraphNode, Scatter)]()
        }.flatten.toMap

        // Generate a list of edges corresponding to dependencies for this call
        // e.g. suppose call x has dependencies a,b,c.  This procedure will generate:
        // [(a,x), (b,x), (c,x)]
        val edges : Set[(Scope, Scope)] = recursedNodes.map { gnode =>
            val nodeParents : Vector[(Scope, Scope)] = (gnode.upstream).map { parent =>
               val parentActual : Scope =
                   // If any node's parent is a descendant of a root
                   // level scatter, use the scatter as the parent
                   if (scatterRoot.contains(parent)) {
                       scatterRoot(parent).asInstanceOf[Scope]
                   }
                   // Otherwise just use the actual parent
                   else {
                       parent.asInstanceOf[Scope]
                   }
               (parentActual, gnode.asInstanceOf[Scope])
            }.toVector

            val descendantParents : Vector[(Scope, Scope)] = gnode match {
                case scatter: Scatter => {
                    // If a scatter's descendants have parents outside the scatter context,
                    // map the scatter itself to them.
                    val nodeDescendants = scatter.descendants.map { d => d.asInstanceOf[GraphNode] }
                    val upstreamParents = nodeDescendants.map { d => d.upstream }.flatten.toSet
                    ((upstreamParents -- nodeDescendants) - gnode).map{ parent =>
                        // If the parent outside the scatter is also a member of a scatter, use it instead.
                        if ( scatterRoot.contains(parent) && gnode != scatterRoot(parent)) {
                            (scatterRoot(parent), gnode)
                        } else {
                            (parent, gnode)
                        }
                    }.toVector
                }
                case anyOtherNode => Vector[(Scope, Scope)]()
            }

            nodeParents ++ descendantParents
        }.flatten.toSet

        // Print out edges of DAG for debugging purposes
        edges.foreach {
            case (v,w) => Utils.trace(cState.verbose, indent + v.asInstanceOf[GraphNode].fullyQualifiedName + " -> " + w.asInstanceOf[GraphNode].fullyQualifiedName)
        }

        // Topologically sort graph or return error that workflow contains a cycle
        val sortedNodes = tsort(edges).toSeq
        Utils.trace(cState.verbose,
            indent + "Sorted nodes: " + sortedNodes.map { node => node.asInstanceOf[GraphNode].fullyQualifiedName }.mkString(", "))


        sortedNodes.filter(x=>nodes.contains(x))
    }

    // WDL-specific wrapper for topological sorting
    def sortWorkflow(nswf: WdlNamespaceWithWorkflow, cState: State, wdlSourceFile: Path) : Path = {
        val wf = nswf.workflow
        val sortedNodes = tsortASTnodes(wf.children, cState, 0)

        val newWorkflow = WdlRewrite.workflow(wf, sortedNodes)
        val newNamespace = WdlRewrite.namespace(newWorkflow, nswf.tasks)
        val newSource = WdlPrettyPrinter(true).apply(newNamespace, 1).mkString("\n")
        // val ns = WdlNamespace.loadUsingSource(newSource, None, None).get
        // write the output to xxx.sorted.wdl
        // Placehodler: always print file
        val trgName: String = addFilenameSuffix(wdlSourceFile, ".sorted")
        val sortedWdl = Utils.appCompileDirPath.resolve(trgName).toFile
        val fos = new FileWriter(sortedWdl)
        val pw = new PrintWriter(fos)
        pw.println(newSource)
        pw.flush()
        pw.close()
        Utils.trace(cState.verbose, s"Wrote sorted WDL to ${sortedWdl.toString}")
        sortedWdl.toPath
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
        // Process the original WDL file,
        // Do not modify the tasks
        val newPath = ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                sortWorkflow(nswf, cState, wdlSourceFile)
            case anyOtherType => wdlSourceFile
        }
        newPath
    }
}
