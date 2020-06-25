package dx.api

import dx.core.util.SysUtils
import dx.util.Logger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

class DxPathTest extends AnyFlatSpec with Matchers {
  val DX_API: DxApi = DxApi(Logger.Quiet)
  val TEST_PROJECT = "dxWDL_playground"

  lazy val dxTestProject: DxProject =
    try {
      DX_API.resolveProject(TEST_PROJECT)
    } catch {
      case _: Exception =>
        throw new Exception(
            s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                |the platform on staging.""".stripMargin
        )
    }

  // describe a file on the platform using the dx-toolkit. This is a baseline for comparison
  private def describeDxFilePath(path: String): String = {
    val (stdout, _) = SysUtils.execCommand(s"dx describe ${path} --json")
    val id = stdout.parseJson.asJsObject.fields.get("id") match {
      case Some(JsString(x)) => x.replaceAll("\"", "")
      case other             => throw new Exception(s"Unexpected result ${other}")
    }
    id
  }

  it should "handle files in a root directory" in {
    val path = s"${TEST_PROJECT}:/Readme.md"
    val expectedId = describeDxFilePath(path)
    val dxFile: DxFile = DX_API.resolveDxUrlFile(s"dx://${path}")
    dxFile.getId shouldBe expectedId
  }

  it should "handle files in a subdirectory directory" in {
    val path = s"${TEST_PROJECT}:/test_data/fileA"
    val expectedId = describeDxFilePath(path)
    val dxFile: DxFile = DX_API.resolveDxUrlFile(s"dx://${path}")
    dxFile.getId shouldBe expectedId
  }

  it should "handle files with a colon" in {
    val expectedId = describeDxFilePath(s"${TEST_PROJECT}:/x*.txt")
    val dxFile: DxFile = DX_API.resolveDxUrlFile(s"dx://${TEST_PROJECT}:/x:x.txt")
    dxFile.getId shouldBe expectedId
  }
}
