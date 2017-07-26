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

    // Mode of operation
    object Mode extends Enumeration {
        val Check, Sort, SortRelaxed = Value
    }

    case class State(cef: CompilerErrorFormatter,
                     terminalMap: Map[Terminal, WdlSource],
                     verbose: Boolean)

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
    def tsortASTnodes(nodes: Seq[Scope], cState:State, recursionDepth: Int) : Seq[Scope] = {
        val indent = " "*recursionDepth*4

        val recursedNodes = nodes.map {
             case scatter : Scatter => {
                 //Utils.trace(cState.verbose, indent+"Recursively performing toological sort on scatter: " + scatter.fullyQualifiedName)
                 val newScatter = WdlRewrite.scatter(scatter,
                    tsortASTnodes(scatter.children, cState, recursionDepth + 1))
                 //Utils.trace(cState.verbose, indent+"End recursion")
                 newScatter.asInstanceOf[GraphNode]
             }
             case x => x.asInstanceOf[GraphNode]
         }

        // Create workflow graph sequence of edges for topological sorting
        // Nodes in the graph (Scopes) can be a call, a scatter, or a declaration

        // Create a mapping from a child of a scatter node to the root level scatter
        // node it belongs to. This allows for a root level scatter node to be placed
        // after its last dependency.
        val scatterParent : Map[GraphNode, Scatter] = recursedNodes.map {
            case scat: Scatter => scat.descendants.map { d => (d.asInstanceOf[GraphNode], scat) }
            case _ => Set[(GraphNode, Scatter)]()
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
                    val nodeDescendants = scatter.descendants.map { d => d.asInstanceOf[GraphNode] }
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
/*        edges.foreach {
            case (v,w) => Utils.trace(cState.verbose, indent + v.asInstanceOf[GraphNode].fullyQualifiedName + " -> " + w.asInstanceOf[GraphNode].fullyQualifiedName)
        }*/

        // Topologically sort graph or return error that workflow contains a cycle
        val sortedNodes = tsort(edges).toSeq.filter(x=>nodes.contains(x))


        Utils.trace(cState.verbose,
            indent + "Sorted nodes: " + sortedNodes.map { node => node.asInstanceOf[GraphNode].fullyQualifiedName }.mkString(", "))

        sortedNodes
    }

    // Alternative method of sorting that does not collapse scatters
    def tsortASTNodesAlternative(allNodesSorted: Seq[GraphNode], nodes: Seq[GraphNode], cState: State, recursionDepth: Int) : Seq[GraphNode] = {
        val indent = " "*recursionDepth*4
        val recursedNodes : Seq[GraphNode] = nodes.map {
             case scatter : Scatter => {
                 //Utils.trace(cState.verbose, indent+"Recursively sorting scatter: " + scatter.fullyQualifiedName)
                 val newScatter = WdlRewrite.scatter(scatter,
                    tsortASTNodesAlternative(allNodesSorted, scatter.children.map{ x => x.asInstanceOf[GraphNode] }, cState, recursionDepth + 1))
                 //Utils.trace(cState.verbose, indent+"End recursion")
                 newScatter.asInstanceOf[GraphNode]
             }
             case x => x.asInstanceOf[GraphNode]
        }
        // TODO: bad big O here.  Better to filter allNodesSorted, however it needs to contain the new scatters
        val filteredNodes = recursedNodes.sortWith { (a,b) => allNodesSorted.indexOf(a) < allNodesSorted.indexOf(b) }
/*        Utils.trace(cState.verbose,
            indent + "Sorted nodes: " + filteredNodes.map { node => node.asInstanceOf[GraphNode].fullyQualifiedName }.mkString(", "))*/

        filteredNodes
    }

    def sortWorkflowAlternative(nswf: WdlNamespaceWithWorkflow, cState: State): Seq[Scope] = {
        val wf = nswf.workflow
        val edges = wf.descendants.map { scope =>
            scope.asInstanceOf[GraphNode].upstream.map { dependant =>
                (dependant.asInstanceOf[GraphNode], scope.asInstanceOf[GraphNode])
            }
        }.flatten.toSet
        val allNodesSorted = tsort(edges).toSeq.map { x => x.asInstanceOf[GraphNode] }
        /*Utils.trace(cState.verbose,
            "Globally sorted nodes: " + allNodesSorted.map { node => node.asInstanceOf[GraphNode].fullyQualifiedName }.mkString(", "))*/
        tsortASTNodesAlternative(allNodesSorted, wf.children.map { x => x.asInstanceOf[GraphNode] }, cState, 0)
    }

    // WDL-specific wrapper for topological sorting
    def sortWorkflow(nswf: WdlNamespaceWithWorkflow,
                     mode: Mode.Value,
                     cState: State) : WdlNamespaceWithWorkflow = {
        val wf = nswf.workflow
        mode match {
            case Mode.Check =>
                // Try sorting, and report on any circular dependencies.
                // Do not modify the workflow.
                Utils.trace(cState.verbose, "Checking the workflow is sorted")
                val sortedNodes = tsortASTnodes(wf.children, cState, 0)
                nswf
            case Mode.Sort =>
                // Default sort mode
                Utils.trace(cState.verbose, "Performing default 'scatter collapsed' topological sort")
                val sortedNodes = tsortASTnodes(wf.children, cState, 0)
                val sortedWf = WdlRewrite.workflow(wf, sortedNodes)
                WdlRewrite.namespace(sortedWf, nswf.tasks)
            case Mode.SortRelaxed =>
                Utils.trace(cState.verbose, "Performing alternative 'relaxed' topological sort")
                val sortedNodes = sortWorkflowAlternative(nswf, cState)
                val sortedWf = WdlRewrite.workflow(wf, sortedNodes)
                WdlRewrite.namespace(sortedWf, nswf.tasks)
        }
    }

    def apply(ns: WdlNamespace,
              mode: Mode.Value,
              verbose: Boolean) : WdlNamespace = {
        Utils.trace(verbose, "Topological sort pass")

        val tm = ns.terminalMap
        val cef = new CompilerErrorFormatter(tm)
        val cState = State(cef, tm, verbose)

        ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                sortWorkflow(nswf, mode, cState)
            case ns => ns
        }
    }
}
