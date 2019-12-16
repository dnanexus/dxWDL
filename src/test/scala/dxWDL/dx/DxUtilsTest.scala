package dxWDL.dx

import com.dnanexus.{DXFile, DXProject}
import org.scalatest.{FlatSpec, Matchers}

class DxUtilsTest extends FlatSpec with Matchers {

    val TEST_PROJECT = "dxWDL_playground"
    lazy val dxTestProject : DXProject =
        try {
            DxPath.resolveProject(TEST_PROJECT)
        } catch {
            case e : Exception =>
                throw new Exception(s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                                        |the platform on staging.""".stripMargin)
        }

    it should "download files as strings" in {
        val results = DxPath.resolveBulk(
            List(s"dx://${TEST_PROJECT}:/test_data/fileA"), dxTestProject)
        results.size shouldBe(1)
        val dxobj = results.values.head
        val dxFile : DXFile = dxobj.asInstanceOf[DXFile]

        val value = DxUtils.downloadString(dxFile, false)
        value shouldBe("The fibonacci series includes 0,1,1,2,3,5\n")
    }

}
