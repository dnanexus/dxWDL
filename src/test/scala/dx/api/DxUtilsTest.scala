package dx.api

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.util.Logger

class DxUtilsTest extends AnyFlatSpec with Matchers {
  val dxApi: DxApi = DxApi(Logger.Quiet)
  val TEST_PROJECT = "dxWDL_playground"

  lazy val dxTestProject: DxProject =
    try {
      dxApi.resolveProject(TEST_PROJECT)
    } catch {
      case _: Exception =>
        throw new Exception(
            s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                |the platform on staging.""".stripMargin
        )
    }

  it should "download files as strings" in {
    val results = dxApi.resolveBulk(List(s"dx://${TEST_PROJECT}:/test_data/fileA"), dxTestProject)
    results.size shouldBe 1
    val dxobj = results.values.head
    val dxFile: DxFile = dxobj.asInstanceOf[DxFile]

    val value = dxApi.downloadString(dxFile)
    value shouldBe "The fibonacci series includes 0,1,1,2,3,5\n"
  }
}
