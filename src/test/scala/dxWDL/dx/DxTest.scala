package dxWDL.dx

import org.scalatest.{FlatSpec, Matchers}

class DxTest extends FlatSpec with Matchers {

  val TEST_PROJECT = "dxWDL_playground"
  lazy val dxTestProject: DxProject =
    try {
      DxPath.resolveProject(TEST_PROJECT)
    } catch {
      case e: Exception =>
        throw new Exception(
          s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                                        |the platform on staging.""".stripMargin
        )
    }

  ignore should "describe a record with details" in {
    val record = DxRecord.getInstance("record-Fgk7V7j0f9JfkYK55P7k3jGY", dxTestProject)
    val desc = record.describe(Set(Field.Details))
    System.out.println(desc)
  }

  ignore should "describe a file with details" in {
    val record = DxFile.getInstance("file-FJ1qyg80ffP9v6gVPxKz9pQ7", dxTestProject)
    val desc = record.describe()
    System.out.println(desc)
  }
}
