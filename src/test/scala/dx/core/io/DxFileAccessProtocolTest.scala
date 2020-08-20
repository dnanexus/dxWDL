package dx.core.io

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DxFileAccessProtocolTest extends AnyFlatSpec with Matchers {

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
      WdlValueAnalysis.ifConstEval(node.wdlType, node.expr.get)
  }
}
