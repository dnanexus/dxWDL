package dxWDL

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, OneInstancePerTest}
import scala.sys.process._
import spray.json._
import spray.json.DefaultJsonProtocol
//import spray.json.JsString
import wdl4s.{AstTools, Call, Task, WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow, Workflow}
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.types._
import wdl4s.values._
import wdl4s.{Call, Declaration, Scatter, Scope,
    Task, TaskCall, TaskOutput,
    WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow,
    Workflow, WorkflowCall, WdlSource, GraphNode}


class CompilerTopologicalSortTest extends FlatSpec with BeforeAndAfterEach {
    val simpleWdl = """|task add {
                       |    Int x
                       |    Int y
                       |    command { }
                       |    output { Int r = x + y }
                       |}
                       |
                       |workflow w {
                       |    call add as C { input: x = A.r, y = 0 }
                       |    call add as B { input: x = 3, y = 0 }
                       |    call add as D { input: x = B.r, y = C.r }
                       |    call add as A { input: x = 0, y = 0 }
                       |}""".stripMargin.trim

    val nestedCycle = """|task add {
                         |    Int x
                         |    Int y
                         |    command { }
                         |    output { Int r = x + y }
                         |}
                         |
                         |workflow W {
                         |    Array[Int] xs
                         |    scatter (x in xs) {
                         |        call add as A { input: x = C.r, y = 0 }
                         |        call add as B { input: x = 3, y = 0 }
                         |    }
                         |    scatter (x in xs) {
                         |        call add as C { input: x = 1, y = 2 }
                         |        call add as D { input: x = B.r, y = 0 }
                         |    }
                         |}""".stripMargin.trim

     def getNames(nodes: Seq[Scope]) = {
         nodes.map { node => node.asInstanceOf[GraphNode].fullyQualifiedName }
     }

     def sortWorkflowHelper(wdl: String, relaxed: Boolean) : (Seq[String], Seq[Scope]) =  {
        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        val wf = ns.workflow

        val tm = ns.terminalMap
        val cef = new CompilerErrorFormatter(tm)
        val verbose = true
        val cState = CompilerTopologicalSort.State(cef, tm, verbose)
        val sortedNodes =
            if (relaxed) {
                CompilerTopologicalSort.tsortASTnodes(wf.children, cState, 0)
            } else {
                CompilerTopologicalSort.sortWorkflowAlternative(ns, cState)
            }
        val sortedNames = getNames(sortedNodes)
        (sortedNames, sortedNodes)
     }


    "Compiler" should "correctly sort a single-level workflow using 'scatter collapse' method" in {
        val (sortedNames, sortedNodes) = sortWorkflowHelper(simpleWdl, false)
        assert( sortedNames == Seq("w.A", "w.B", "w.C", "w.D") || sortedNames == Seq("w.B", "w.A", "w.C", "w.D" ))
    }

    it should "correctly sort a single-level workflow using 'relaxed' method" in {
        val (sortedNames, sortedNodes) = sortWorkflowHelper(simpleWdl, true)
        assert( sortedNames == Seq("w.A", "w.B", "w.C", "w.D") || sortedNames == Seq("w.B", "w.A", "w.C", "w.D" ))
    }

    // TODO: throw specific typed exception and assert for their existence

    it should "error out on a simple cycle" in {
        val simpleCycle = """|task add {
                             |    Int x
                             |    Int y
                             |    command { }
                             |    output { Int r = x + y }
                             |}
                             |
                             |workflow w {
                             |    call add as C { input: x = A.r, y = 0 }
                             |    call add as B { input: x = 3, y = 0 }
                             |    call add as A { input: x = C.r, y = 0 }
                             |}""".stripMargin.trim

        try {
           sortWorkflowHelper(simpleCycle, true)
           throw new Exception("Compiler did not throw an exception on a simple cycle")
        } catch {
            case e: Exception => true
        }
    }

    it should "error out on a nested cycle using 'scatter collapse' method" in {
        try {
           sortWorkflowHelper(nestedCycle, true)
           throw new Exception("Compiler did not throw an exception on a nested cycle")
        } catch {
            case e: Exception => true
        }
    }
    it should "allow for a nested cycle using the 'relaxed' method" in {
        sortWorkflowHelper(nestedCycle, false)
    }
    it should "correctly sort a basic multi-level workflow" in {
        val multiLevelWdl = """|task add {
                               |    Int x
                               |    Int y
                               |    command { }
                               |    output { Int r = x + y }
                               |}
                               |
                               |workflow W {
                               |    Array[Int] xs
                               |    scatter (y in B.r) {
                               |        call add as D { input: x = C.r, y = 0 }
                               |        call add as C { input: x = 1, y = 2 }
                               |    }
                               |    scatter (x in xs) {
                               |        call add as B { input: x = A.r, y = 0 }
                               |        call add as A { input: x = 1, y = 0 }
                               |    }
                               |}""".stripMargin.trim

        val (sortedNames, sortedNodes) = sortWorkflowHelper(multiLevelWdl, false)
        assert(sortedNames == Seq("W.xs", "W.$scatter_1", "W.$scatter_0"))
        assert(getNames(sortedNodes(1).children) == Seq("W.A", "W.B"))
        assert(getNames(sortedNodes(2).children) == Seq("W.C", "W.D"))
    }

}
