package dx.api

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.util.Logger

class DxTest extends AnyFlatSpec with Matchers {
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

  ignore should "describe a record with details" in {
    val record = dxApi.record("record-Fgk7V7j0f9JfkYK55P7k3jGY", Some(dxTestProject))
    val desc = record.describe(Set(Field.Details))
    System.out.println(desc)
  }

  ignore should "describe a file with details" in {
    val record = dxApi.file("file-FJ1qyg80ffP9v6gVPxKz9pQ7", Some(dxTestProject))
    val desc = record.describe()
    System.out.println(desc)
  }
}
