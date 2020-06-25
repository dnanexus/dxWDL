package dx.api

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.util.Logger

class DxTest extends AnyFlatSpec with Matchers {
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

  ignore should "describe a record with details" in {
    val record = DX_API.record("record-Fgk7V7j0f9JfkYK55P7k3jGY", Some(dxTestProject))
    val desc = record.describe(Set(Field.Details))
    System.out.println(desc)
  }

  ignore should "describe a file with details" in {
    val record = DX_API.file("file-FJ1qyg80ffP9v6gVPxKz9pQ7", Some(dxTestProject))
    val desc = record.describe()
    System.out.println(desc)
  }
}
