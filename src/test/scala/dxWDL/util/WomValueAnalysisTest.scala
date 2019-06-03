package dxWDL.util

import org.scalatest.{FlatSpec, Matchers}
import wom.callable.WorkflowDefinition
import wom.expression.WomExpression
import wom.graph.expression.ExpressionNode
import wom.types._
import wom.values._

class WomValueAnalysisTest extends FlatSpec with Matchers {

    def parseExpressions(wdlCode: String) : Vector[ExpressionNode] = {
        val (wf : WorkflowDefinition, _, _) = ParseWomSourceFile.parseWdlWorkflow(wdlCode)
        val eNodes : Vector[ExpressionNode] = wf.innerGraph.nodes.collect{
            case x : ExpressionNode => x
        }.toVector
        eNodes
    }

    it should "evalConst" in {
        val allExpectedResults = Map(
            "flag" -> Some(WomBoolean(true)),
            "i" -> Some(WomInteger(8)),
            "x" -> Some(WomFloat(2.718)),
            "s" -> Some(WomString("hello world")),
            "ar1" -> Some(WomArray(WomArrayType(WomStringType),
                                   Vector(WomString("A"), WomString("B"), WomString("C")))),
            "m1" -> Some(WomMap(WomMapType(WomStringType, WomIntegerType),
                                Map(WomString("X") -> WomInteger(1),
                                    WomString("Y") -> WomInteger(10)))),
            "p" -> Some(WomPair(WomInteger(1), WomInteger(12))),
            "file2" -> None,
            "k" ->None,
            "readme" -> None
        )


        // The workflow is a convenient packaging around WOM
        // expressions. When we parse the workflow, the expressions lose
        // their relative ordering, and become a graph. That's why we need
        // to keep track of a map from identifier name to expected result.
        val wdlCode =
            """|version 1.0
               |
               |workflow foo {
               |
               |    # constants
               |    Boolean flag = true
               |    Int i = 3 + 5
               |    Float x = 2.718
               |    String s = "hello" + " world"
               |    Array[String] ar1 = ["A", "B", "C"]
               |    Map[String, Int] m1 = {"X": 1, "Y": 10}
               |    Pair[Int, Int] p = (1, 12)
               |    File? file2 = "/tmp/xxx.txt"
               |
               |    # evaluations
               |    Int k = i + 5
               |    File readme = "/tmp/readme.md"
               |}
               |""".stripMargin

        val expressions = parseExpressions(wdlCode)
        for (node <- expressions) {
            val id : String = node.identifier.localName.value
            val expected : Option[WomValue] = allExpectedResults(id)
            val expr : WomExpression = node.womExpression
            val womType : WomType = node.womType

            val retval = WomValueAnalysis.ifConstEval(womType, expr)
            retval shouldBe(expected)

            // Check that evalConst works
            expected match {
                case None =>
                    assertThrows[Exception] {
                        WomValueAnalysis.evalConst(womType, expr)
                    }
                    WomValueAnalysis.isExpressionConst(womType, expr) shouldBe(false)
                case Some(x) =>
                    WomValueAnalysis.evalConst(womType, expr) shouldBe(x)
                    WomValueAnalysis.isExpressionConst(womType, expr) shouldBe(true)
            }
        }
    }

    it should "not be able to access unsupported file protocols" in {
        val wdlCode =
            """|version 1.0
               |
               |workflow foo {
               |    File readme = "gs://this_file_is_on_google_cloud"
               |}
               |""".stripMargin

        val expressions = parseExpressions(wdlCode)
        val node = expressions.head
        assertThrows[Exception] {
            WomValueAnalysis.ifConstEval(node.womType, node.womExpression)
        }
    }

    it should "handle links to dx files" in {
        val wdlCode =
            """|version 1.0
               |
               |workflow foo {
               |    File fruit_list = "dx://dxWDL_playground:/test_data/fruit_list.txt"
               |    File a_txt = "dx://dxWDL_playground:/A.txt"
               |    File proj_file_id = "dx://project-xxxx:file-yyyy"
               |    File proj_file_name = "dx://project-xxxx:A.txt"
               |}
               |""".stripMargin

        val expressions = parseExpressions(wdlCode)
        val nodes = expressions.toList
        for (node <- nodes)
            WomValueAnalysis.ifConstEval(node.womType, node.womExpression)
    }
}
