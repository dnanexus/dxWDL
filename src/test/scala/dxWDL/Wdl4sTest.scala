package dxWDL

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import scala.sys.process._
import spray.json._
import spray.json.DefaultJsonProtocol
import wdl4s.{AstTools, Declaration, GraphNode, Call, Scatter, Scope, Task, WdlExpression,
    WdlNamespace, WdlNamespaceWithWorkflow, Workflow, WorkflowOutput}
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._


class Wdl4sTest extends FlatSpec with BeforeAndAfterEach {

    override def beforeEach() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.deleteRecursive(metaDir.toFile)
    }


    // Look for a call inside a namespace.
    private def getCallFromNamespace(ns : WdlNamespaceWithWorkflow, callName : String ) : Call = {
        val wf: Workflow = ns.workflow
        wf.findCallByName(callName) match {
            case None => throw new AppInternalException(s"Call ${callName} not found in WDL file")
            case Some(call) => call
        }
    }


    // Fails with wdl4s v0.6, because
    //   '5 * (1 + 1)'  is converted into '5 * 1 + 1'
    //
    it should "serialize expressions without loss" in {
        val s = "5 * (1 + 1)"
        val expr : WdlExpression = WdlExpression.fromString(s)
        val s2 = expr.toWdlString
        assert(s == s2)

    }

    it should "generate expression variable ID" in {
        val expr = WdlExpression.fromString("xxx")
        //System.out.println(s"expr=${expr.toWdlString}")
        assert(expr.toWdlString == "xxx")
    }

    it should "retrieve source code for task" in {
        val wdl = """|task a {
                     |  String prefix
                     |  Array[Int] ints
                     |  command {
                     |    python script.py ${write_lines(ints)} > ${prefix + ".out"}
                     |  }
                     |}
                     |workflow wf {
                     |  call a
                     |}""".stripMargin.trim


        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        ns.findTask("a") foreach { task =>
            assert(task.name == "a")
        }

        /* Traverse the tree to find all Task definitions */
        AstTools.findAsts(ns.ast, "Task") foreach {ast =>
            assert("a" == ast.getAttribute("name").sourceString)
        }

        // doesn't work, toWdlString is not implemented for values
        //System.out.println(s"ns=${ns.toWdlString}")
    }


    it should "Calculate call expression type" in {
        val wdl = """|
                     |task Add {
                     |    Int a
                     |    Int b
                     |
                     |    command {
                     |        echo $((a + b))
                     |    }
                     |    output {
                     |        Int result = read_int(stdout())
                     |    }
                     |}
                     |
                     |workflow w {
                     |    Pair[Int, Int] p = (5, 8)
                     |    call Add {
                     |        input: a=p.left, b=p.right
                     |    }
                     |    call Add as Add2 {
                     |        input: a=Add.result, b=p.right
                     |    }
                     |    call Add as Add3 {
                     |        input: a=Add2.result, b=p.right
                     |    }
                     |    output {
                     |        Add.result
                     |        Add2.result
                     |        Add3.result
                     |    }
                     |}""".stripMargin.trim

        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get

        // These should be call outputs
        List("Add", "Add3").foreach{ elem =>
            val wdlType = WdlNamespace.lookupType(ns.workflow)(elem)
            assert(wdlType.isInstanceOf[WdlCallOutputsObjectType])
        }

        // These should *not* be call outputs
        List("p").foreach{ elem =>
            val wdlType = WdlNamespace.lookupType(ns.workflow)(elem)
            assert(!wdlType.isInstanceOf[WdlCallOutputsObjectType])
        }

        // Accesses to the pair [p] should depend only on the top level
        // variable(s)
        val addCall:Call = ns.workflow.findCallByName("Add").get
        addCall.inputMappings.foreach{ case (key,expr) =>
            //System.err.println(s"key=${key} expr=${expr.toWdlString}")
            //assert(expr.prerequisiteCallNames.isEmpty)
            /*expr.prerequisiteCallNames.map{ fqn =>
                val wdlType = WdlNamespace.lookupType(ns.workflow)(fqn)
                System.err.println(s"fqn=${fqn} wdlType=${wdlType}")
            }*/
            val variables = AstTools.findVariableReferences(expr.ast).map{ case t:Terminal =>
                WdlExpression.toString(t)
            }
            //System.err.println(s"dep vars(${expr.toWdlString}) = ${variables}")
            //assert(expr.toWdlString == variables)
            assert(variables == List("p"))
        }
    }

    // figure out if a variable escapes
    def isLocal(decl: Declaration,
                wfOutputs: Seq[WorkflowOutput]) : Boolean = {
        // find all dependent nodes
        val dNodes:Set[GraphNode] = decl.downstream
        val declParent:Scope = decl.parent.get

        // figure out if these downstream nodes are in the same scope.
        val dnScopes:Set[GraphNode] = dNodes.filter{ node =>
            node.parent.get.fullyQualifiedName != declParent.fullyQualifiedName
            //&&
            //node.fullyQualifiedName != decl.fullyQualifiedName
        }
        val dnScopeFQN = dnScopes.map{ x => x.fullyQualifiedName }

        // Also check if the declaration appears in the workflow output
        val unusedInWfOutput = wfOutputs.forall(x => !(x.upstream contains decl))

        val allOutputs = wfOutputs.map(_.fullyQualifiedName)

        // the declaration is used only locally
        val retval = dnScopes.isEmpty && unusedInWfOutput
/*        System.err.println(s"""|decl ${decl.fullyQualifiedName}
                               |declParent ${declParent.fullyQualifiedName}
                               |downstream scopes=${dnScopeFQN}
                               |retval=${retval}
                               |allOutputs=${allOutputs}
                               |unusedInWfOutput=${unusedInWfOutput}
                               |
                               |""".stripMargin.trim)*/
        retval
    }

    it should "tell if a declaration is local to a block" in {
        val wdl = """|
                     |task Add {
                     |    Int a
                     |    Int b
                     |
                     |    command {
                     |        echo $((a + b))
                     |    }
                     |    output {
                     |        Int result = read_int(stdout())
                     |    }
                     |}
                     |
                     |workflow w {
                     |    Array[Int] integers = [1, 5, 10, 21]
                     |    scatter (i in integers) {
                     |        Int x = i
                     |        Int y = i
                     |        Array[Int] z = [x,x]
                     |    }
                     |    call Add  {
                     |        input: a=x[0], b=x[1]
                     |    }
                     |    output {
                     |        Add.result
                     |        z
                     |    }
                     |}""".stripMargin.trim

        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        val wf:Workflow = ns match {
            case nswf : WdlNamespaceWithWorkflow => nswf.workflow
            case _ => throw new Exception("sanity")
        }

        val (topDecls, rest) = Utils.splitBlockDeclarations(wf.children.toList)
        val scatter = rest.head match {
            case ssc:Scatter => ssc
            case _ => throw new Exception("sanity")
        }
        val wfOutputs:Seq[WorkflowOutput] = wf.outputs

        val decls: List[String] =
            scatter.declarations
                .filter { decl => !isLocal(decl, wfOutputs) }
                .map { decl => decl.unqualifiedName }
                .toList
        assert(decls == List("x", "z"))
    }
}
