package dx.core.io

import dx.Assumptions.isLoggedIn
import dx.Tags.ApiTest
import dx.core.languages.wdl.Utils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.eval.{Eval, EvalPaths}
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.util.FileSourceResolver

class DxFileAccessProtocolTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)

  it should "handle links to dx files" taggedAs ApiTest in {
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

    val (doc, _) = Utils.parseSourceString(wdlCode)
    val declarations: Vector[TAT.PrivateVariable] = doc.elements.collect {
      case decl: TAT.PrivateVariable => decl
    }
    val fileResolver = FileSourceResolver.create(userProtocols = Vector(DxFileAccessProtocol()))
    val evaluator = Eval(EvalPaths.empty, Some(WdlVersion.V1), fileResolver)
    declarations.foreach {
      case TAT.PrivateVariable(_, wdlType, Some(expr), _) =>
        // applies the default validation, which tries to resolve files and
        // throws an exception on failure
        evaluator.applyConstAndCoerce(expr, wdlType)
      case other =>
        throw new Exception(s"expected declaration with expression ${other}")
    }
  }
}
