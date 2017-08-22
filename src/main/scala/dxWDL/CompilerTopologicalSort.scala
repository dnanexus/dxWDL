package dxWDL

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Path, Paths}
import scala.collection.mutable.Queue
import Utils.{TopoMode, Verbose}
import wdl4s.wdl.AstTools
import wdl4s.wdl.AstTools.EnhancedAstNode
import wdl4s.wdl.{Call, Declaration, Scatter, Scope,
    Task, TaskCall, TaskOutput,
    WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow,
    Workflow, WorkflowCall, WdlSource, WdlGraphNode}
import wdl4s.wdl.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.wdl.types._
import wdl4s.wdl.values._
import wdl4s.wdl.WdlExpression.AstForExpressions

case class CompilerTopologicalSort(cef: CompilerErrorFormatter,
                                   mode: TopoMode.Value,
                                   verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "toposort"

    // Generic topological sorting procedure
    // Scala code only slightly modified from https://gist.github.com/ThiporKong/4399695
    private def tsort[A](edges: Traversable[(A, A)]): Iterable[A] = {
        def tsort(toPreds: Map[A, Set[A]], done: Iterable[A]): Iterable[A] = {
            // Partition the set of nodes into those that have a predecessor and those that do not
            // (Actually this will partition the map into two)
            val (noPreds, hasPreds) = toPreds.partition { keyval =>
                val predSet = keyval._2
                predSet.isEmpty
            }
            // If there are no nodes left with no predecessors, either we are done recursing or there is a cycle
            if (noPreds.isEmpty) {
                if (hasPreds.isEmpty) {
                    // All nodes have been processed, we are done
                    done
                } else {
                    // There are no nodes with no predecessors, but we still have nodes to process.
                    // Since every DAG contains a vertex with no incoming edges,
                    // there exists at least one directed cycle in the DAG.
                    sys.error("ERROR: workflow contains at least one directed cycle.")
                }
            } else {
                // List of nodes with no predecessors
                val noPredNodes = noPreds.map { keyval => keyval._1 }
                // Build a new list of predecessors that do not contain the nodes above
                val newToPreds = hasPreds.mapValues { predSet => predSet -- noPredNodes }
                tsort(newToPreds, done ++ noPredNodes)
            }
        }
        // Build the initial map of predecessors
        val toPred = edges.foldLeft(Map[A, Set[A]]()) { (acc, e) =>
            acc + (e._1 -> acc.getOrElse(e._1, Set())) + (e._2 -> (acc.getOrElse(e._2, Set()) + e._1))
        }
        tsort(toPred, Seq())
    }

    // Internal procedure to sort children nodes at a particular level in the AST
    def tsortASTnodes(nodes: Seq[Scope], recursionDepth: Int) : Seq[Scope] = {
        val indent = " "*recursionDepth*4

        val recursedNodes = nodes.map {
             case scatter : Scatter => {
                 Utils.trace(verbose2,
                             indent+"Recursively performing toological sort on scatter: " +
                                 scatter.fullyQualifiedName)
                 val newScatter = WdlRewrite.scatter(scatter,
                                                     tsortASTnodes(scatter.children, recursionDepth + 1))
                 Utils.trace(verbose2, indent+"End recursion")
                 newScatter.asInstanceOf[WdlGraphNode]
             }
             case x => x.asInstanceOf[WdlGraphNode]
         }

        // Create workflow graph sequence of edges for topological sorting
        // Nodes in the graph (Scopes) can be a call, a scatter, or a declaration

        // Create a mapping from a child of a scatter node to the root level scatter
        // node it belongs to. This allows for a root level scatter node to be placed
        // after its last dependency.
        val scatterParent : Map[WdlGraphNode, Scatter] = recursedNodes.map {
            case scat: Scatter => scat.descendants.map { d => (d.asInstanceOf[WdlGraphNode], scat) }
            case _ => Set[(WdlGraphNode, Scatter)]()
        }.flatten.toMap

        // Generate a list of edges corresponding to dependencies for this call
        // e.g. suppose call x has dependencies a,b,c.  This procedure will generate:
        // [(a,x), (b,x), (c,x)]
        val edges : Set[(Scope, Scope)] = recursedNodes.map { gnode =>
            val nodeDependencies : Vector[(Scope, Scope)] = (gnode.upstream).map { dependency =>
               val dependencyActual : Scope =
                   if (scatterParent.contains(dependency)) {
                       // If any node's parent is a descendant of a root
                       // level scatter, use the scatter as the parent
                       scatterParent(dependency).asInstanceOf[Scope]
                   } else {
                       // Otherwise just use the actual parent
                       dependency.asInstanceOf[Scope]
                   }
               (dependencyActual, gnode.asInstanceOf[Scope])
            }.toVector

            // Please see Case 1 under 'Default sorting procedure' in doc/TopoSort.md for more info
            val descendantParents : Vector[(Scope, Scope)] = gnode match {
                case scatter: Scatter => {
                    // If a scatter's descendants have parents outside the scatter context,
                    // map the scatter itself to them.
                    val nodeDescendants = scatter.descendants.map { d => d.asInstanceOf[WdlGraphNode] }
                    val upstreamParents = nodeDescendants.map { d => d.upstream }.flatten.toSet
                    ((upstreamParents -- nodeDescendants) - gnode).map{ parent =>
                        // If the parent outside the scatter is also a member of a scatter, use it instead.
                        if ( scatterParent.contains(parent) && gnode != scatterParent(parent)) {
                            (scatterParent(parent), gnode)
                        } else {
                            (parent, gnode)
                        }
                    }.toVector
                }
                case _ => Vector[(Scope, Scope)]()
            }

            nodeDependencies ++ descendantParents
        }.flatten.toSet

        // Print out edges of DAG for debugging purposes
        edges.foreach {
            case (v,w) => Utils.trace(
                verbose2,
                indent + v.asInstanceOf[WdlGraphNode].fullyQualifiedName + " -> " + w.asInstanceOf[WdlGraphNode].fullyQualifiedName)
        }

        // Topologically sort graph or return error that workflow contains a cycle
        val sortedNodes = tsort(edges).toSeq.filter(x=>nodes.contains(x))
        Utils.trace(verbose2,
                    indent + "Sorted nodes: " +
                        sortedNodes.map {
                            node => node.asInstanceOf[WdlGraphNode].fullyQualifiedName }.mkString(", ")
        )
        sortedNodes
    }

    // Alternative method of sorting that does not collapse scatters
    def tsortASTNodesAlternative(allNodesSorted: Seq[WdlGraphNode],
                                 nodes: Seq[WdlGraphNode],
                                 recursionDepth: Int) : Seq[WdlGraphNode] = {
        val indent = " "*recursionDepth*4
        val recursedNodes : Seq[WdlGraphNode] = nodes.map {
             case scatter : Scatter => {
                 Utils.trace(verbose2, indent+"Recursively sorting scatter: " + scatter.fullyQualifiedName)
                 val newScatter = WdlRewrite.scatter(
                     scatter,
                     tsortASTNodesAlternative(
                         allNodesSorted,
                         scatter.children.map{ x => x.asInstanceOf[WdlGraphNode] }, recursionDepth + 1))
                 Utils.trace(verbose2, indent+"End recursion")
                 newScatter.asInstanceOf[WdlGraphNode]
             }
             case x => x.asInstanceOf[WdlGraphNode]
        }
        // TODO: bad big O here.  Better to filter allNodesSorted, however it needs to contain the new scatters
        val filteredNodes = recursedNodes.sortWith { (a,b) => allNodesSorted.indexOf(a) < allNodesSorted.indexOf(b) }
        Utils.trace(verbose2,
            indent + "Sorted nodes: " + filteredNodes.map { node => node.asInstanceOf[WdlGraphNode].fullyQualifiedName }.mkString(", "))

        filteredNodes
    }

    def sortWorkflowAlternative(nswf: WdlNamespaceWithWorkflow): Seq[Scope] = {
        val wf = nswf.workflow
        val edges = wf.descendants.map { scope =>
            scope.asInstanceOf[WdlGraphNode].upstream.map { dependant =>
                (dependant.asInstanceOf[WdlGraphNode], scope.asInstanceOf[WdlGraphNode])
            }
        }.flatten.toSet
        val allNodesSorted = tsort(edges).toSeq.map { x => x.asInstanceOf[WdlGraphNode] }
        Utils.trace(verbose2,
            "Globally sorted nodes: " + allNodesSorted.map { node => node.asInstanceOf[WdlGraphNode].fullyQualifiedName }.mkString(", "))
        tsortASTNodesAlternative(allNodesSorted, wf.children.map { x => x.asInstanceOf[WdlGraphNode] }, 0)
    }

    // WDL-specific wrapper for topological sorting
    def sortWorkflow(nswf: WdlNamespaceWithWorkflow) : WdlNamespaceWithWorkflow = {
        val wf = nswf.workflow
        mode match {
            case TopoMode.Check =>
                // Try sorting, and report on any circular dependencies.
                // Do not modify the workflow.
                Utils.trace(verbose.on, "Checking the workflow is sorted")
                val sortedNodes = tsortASTnodes(wf.children, 0)
                nswf
            case TopoMode.Sort =>
                // Default sort mode
                Utils.trace(verbose.on, "Performing default 'scatter collapsed' topological sort")
                val sortedNodes = tsortASTnodes(wf.children, 0)
                val sortedWf = WdlRewrite.workflow(wf, sortedNodes)
                WdlRewrite.namespace(sortedWf, nswf.tasks)
            case TopoMode.SortRelaxed =>
                Utils.trace(verbose.on, "Performing alternative 'relaxed' topological sort")
                val sortedNodes = sortWorkflowAlternative(nswf)
                val sortedWf = WdlRewrite.workflow(wf, sortedNodes)
                WdlRewrite.namespace(sortedWf, nswf.tasks)
        }
    }

    def apply(ns: WdlNamespace) : WdlNamespace = {
        Utils.trace(verbose.on, "Topological sort pass")
        ns match {
            case nswf : WdlNamespaceWithWorkflow => sortWorkflow(nswf)
            case ns => ns
        }
    }
}

object CompilerTopologicalSort {
    def apply(ns: WdlNamespace,
              mode: TopoMode.Value,
              verbose: Verbose) : WdlNamespace = {
        val cef = new CompilerErrorFormatter(ns.terminalMap)
        val c = CompilerTopologicalSort(cef, mode, verbose)
        c.apply(ns)
    }
}
