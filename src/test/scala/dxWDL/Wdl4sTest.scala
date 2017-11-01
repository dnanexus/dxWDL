package dxWDL

import org.scalatest.{FlatSpec, Matchers}
import wdl4s.wdl._
import wdl4s.wdl.AstTools.EnhancedAstNode
import wdl4s.wdl.types._

class Wdl4sTest extends FlatSpec with Matchers {

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
        val wdl = s"""|task a {
                      |  String prefix
                      |  Array[Int] ints
                      |  command {
                      |    python script.py $${write_lines(ints)} > $${prefix + ".out"}
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
        val addCall:WdlCall = ns.workflow.findCallByName("Add").get
        addCall.inputMappings.foreach{ case (key,expr) =>
            //System.err.println(s"key=${key} expr=${expr.toWdlString}")
            //assert(expr.prerequisiteCallNames.isEmpty)
            /*expr.prerequisiteCallNames.map{ fqn =>
                val wdlType = WdlNamespace.lookupType(ns.workflow)(fqn)
                System.err.println(s"fqn=${fqn} wdlType=${wdlType}")
            }*/
            val variables = AstTools.findVariableReferences(expr.ast).map{
                varRef => varRef.terminal.getSourceString
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
        val dNodes:Set[WdlGraphNode] = decl.downstream
        val declParent:Scope = decl.parent.get

        // figure out if these downstream nodes are in the same scope.
        val dnScopes:Set[WdlGraphNode] = dNodes.filter{ node =>
            node.parent.get.fullyQualifiedName != declParent.fullyQualifiedName
            //&&
            //node.fullyQualifiedName != decl.fullyQualifiedName
        }
        val _ = dnScopes.map{ x => x.fullyQualifiedName }

        // Also check if the declaration appears in the workflow output
        val unusedInWfOutput = wfOutputs.forall(x => !(x.upstream contains decl))

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
        val wf:WdlWorkflow = ns match {
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

    it should "Handle null values" in {
        val wdlCode =
            s"""|task NullArray {
                |  Array[String] arr
                |  command {
                |  }
                |  output {
                |     Int result = 3
                |  }
                |}
                |
                |workflow w {
                |  call NullArray { input: arr=null }
                |  output {
                |    NullArray.result
                |  }
                |}""".stripMargin.trim

        val ns = WdlNamespaceWithWorkflow.load(wdlCode, Seq.empty).get
        //val expr = WdlExpression.fromString("null")
        //System.out.println(s"expr=${expr.toWdlString}")
        //System.out.println(s"type=${expr.wdlType}")

        val call:WdlCall = ns.workflow.findCallByName("NullArray").get
        call.inputMappings.foreach{ case (key,expr) =>
            //System.err.println(s"key=${key} expr=${expr.toWdlString}")
            //assert(expr.prerequisiteCallNames.isEmpty)
            /*expr.prerequisiteCallNames.map{ fqn =>
                val wdlType = WdlNamespace.lookupType(ns.workflow)(fqn)
                System.err.println(s"fqn=${fqn} wdlType=${wdlType}")
             }*/
        }
    }

    it should "recognize an empty command section" in {
        val wdlCode =
            s"""|task emptyCommand {
                |  Array[String] arr
                |  command {
                |  }
                |  output {
                |     Int result = 3
                |  }
                |}
                |
                |workflow w {}
                |""".stripMargin.trim

        val ns = WdlNamespaceWithWorkflow.load(wdlCode, Seq.empty).get
        val task = ns.findTask("emptyCommand").get

        task.commandTemplateString should equal("")
    }
}
