package dx.api

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.util.{Logger, Util}

class DxPathTest extends AnyFlatSpec with Matchers {
  private val dxApi: DxApi = DxApi(Logger.Quiet)
  private val testProject = "dxWDL_playground"

  lazy val dxTestProject: DxProject =
    try {
      dxApi.resolveProject(testProject)
    } catch {
      case _: Exception =>
        throw new Exception(
            s"""|Could not find project ${testProject}, you probably need to be logged into
                |the platform on staging.""".stripMargin
        )
    }

  // describe a file on the platform using the dx-toolkit. This is a baseline for comparison
  private def describeDxFilePath(path: String): String = {
    val (stdout, _) = Util.execCommand(s"dx describe ${path} --json")
    val id = stdout.parseJson.asJsObject.fields.get("id") match {
      case Some(JsString(x)) => x.replaceAll("\"", "")
      case other             => throw new Exception(s"Unexpected result ${other}")
    }
    id
  }

  it should "handle files in a root directory" in {
    val path = s"${testProject}:/Readme.md"
    val expectedId = describeDxFilePath(path)
    val dxFile: DxFile = dxApi.resolveDxUrlFile(s"dx://${path}")
    dxFile.getId shouldBe expectedId
  }

  it should "handle files in a subdirectory directory" in {
    val path = s"${testProject}:/test_data/fileA"
    val expectedId = describeDxFilePath(path)
    val dxFile: DxFile = dxApi.resolveDxUrlFile(s"dx://${path}")
    dxFile.getId shouldBe expectedId
  }

  it should "handle files with a colon" in {
    val expectedId = describeDxFilePath(s"${testProject}:/x*.txt")
    val dxFile: DxFile = dxApi.resolveDxUrlFile(s"dx://${testProject}:/x:x.txt")
    dxFile.getId shouldBe expectedId
  }
}
