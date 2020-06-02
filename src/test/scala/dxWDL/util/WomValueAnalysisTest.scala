package dxWDL.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.eval.WdlValues
import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}

import dxWDL.base.ParseWomSourceFile

class WomValueAnalysisTest extends AnyFlatSpec with Matchers {

  def parseExpressions(wdlCode: String): Vector[TAT.Declaration] = {
    val (wf: TAT.Workflow, _, _, _) = ParseWomSourceFile(false).parseWdlWorkflow(wdlCode)
    wf.body.collect {
      case d: TAT.Declaration => d
    }.toVector
  }

  it should "evalConst" in {
    val allExpectedResults = Map(
        "flag" -> Some(WdlValues.V_Boolean(true)),
        "i" -> Some(WdlValues.V_Int(8)),
        "x" -> Some(WdlValues.V_Float(2.718)),
        "s" -> Some(WdlValues.V_String("hello world")),
        "ar1" -> Some(
            WdlValues.V_Array(
                Vector(WdlValues.V_String("A"), WdlValues.V_String("B"), WdlValues.V_String("C"))
            )
        ),
        "m1" -> Some(
            WdlValues.V_Map(
                Map(WdlValues.V_String("X") -> WdlValues.V_Int(1),
                    WdlValues.V_String("Y") -> WdlValues.V_Int(10))
            )
        ),
        "p" -> Some(WdlValues.V_Pair(WdlValues.V_Int(1), WdlValues.V_Int(12))),
        "j" -> None,
        "k" -> None,
        "readme" -> None,
        "s2" -> None,
        "file2" -> None
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
         |    Int i = 8
         |    Float x = 2.718
         |    String s = "hello world"
         |    Array[String] ar1 = ["A", "B", "C"]
         |    Map[String, Int] m1 = {"X": 1, "Y": 10}
         |    Pair[Int, Int] p = (1, 12)
         |
         |    # evaluations
         |    Int j = 3 + 5
         |    Int k = i + 5
         |    String s2 = "hello" + " world"
         |
         |    # reading a file from local disk
         |    File readme = "/tmp/readme.md"
         |    File? file2 = "/tmp/xxx.txt"
         |}
         |""".stripMargin

    val declarations = parseExpressions(wdlCode)
    for (decl <- declarations) {
      val id: String = decl.name
      val expected: Option[WdlValues.V] = allExpectedResults(id)
      val expr: TAT.Expr = decl.expr.get
      val womType: WdlTypes.T = decl.wdlType

      val retval = WomValueAnalysis.ifConstEval(womType, expr)
      retval shouldBe (expected)

      // Check that evalConst works
      expected match {
        case None =>
          assertThrows[Exception] {
            WomValueAnalysis.evalConst(womType, expr)
          }
          WomValueAnalysis.isExpressionConst(womType, expr) shouldBe (false)
        case Some(x) =>
          WomValueAnalysis.evalConst(womType, expr) shouldBe (x)
          WomValueAnalysis.isExpressionConst(womType, expr) shouldBe (true)
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

    val declarations = parseExpressions(wdlCode)
    val node = declarations.head
    assertThrows[Exception] {
      WomValueAnalysis.ifConstEval(node.wdlType, node.expr.get)
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

    val declarations = parseExpressions(wdlCode)
    for (node <- declarations)
      WomValueAnalysis.ifConstEval(node.wdlType, node.expr.get)
  }
}
