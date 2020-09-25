package dx.api

import dx.Assumptions.isLoggedIn
import dx.Tags.ApiTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.util.Logger

class DxApiTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)
  private val dxApi: DxApi = DxApi(Logger.Quiet)
  private val testProject = "dxWDL_playground"
  private val testRecord = "record-Fgk7V7j0f9JfkYK55P7k3jGY"
  private val testFile = "file-FJ1qyg80ffP9v6gVPxKz9pQ7"

  private lazy val dxTestProject: DxProject = {
    try {
      dxApi.resolveProject(testProject)
    } catch {
      case _: Exception =>
        throw new Exception(
            s"""|Could not find project ${testProject}, you probably need to be logged into
                |the platform on staging.""".stripMargin
        )
    }
  }

  ignore should "describe a record with details" taggedAs ApiTest in {
    val record = dxApi.record(testRecord, Some(dxTestProject))
    dxApi.logger.ignore(record.describe(Set(Field.Details)))
  }

  ignore should "describe a file with details" taggedAs ApiTest in {
    val record = dxApi.file(testFile, Some(dxTestProject))
    dxApi.logger.ignore(record.describe())
  }

  it should "download files as strings" in {
    val results =
      dxApi.resolveDataObjectBulk(Vector(s"dx://${testProject}:/test_data/fileA"), dxTestProject)
    results.size shouldBe 1
    val dxobj = results.values.head
    val dxFile: DxFile = dxobj.asInstanceOf[DxFile]

    val value = dxApi.downloadString(dxFile)
    value shouldBe "The fibonacci series includes 0,1,1,2,3,5\n"
  }
}
