package dxWDL.dx

import com.dnanexus.{DXFile, DXProject}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

import dxWDL.base.Utils

class DxPathTest extends FlatSpec with Matchers {

    val TEST_PROJECT = "dxWDL_playground"
    lazy val dxTestProject : DXProject =
        try {
            DxPath.lookupProject(TEST_PROJECT)
        } catch {
            case e : Exception =>
                throw new Exception(s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                                        |the platform on staging.""".stripMargin)
        }

    // describe a file on the platform using the dx-toolkit. This is a baseline for comparison
    private def describeDxFilePath(path : String) : String = {
        val (stdout, stderr) = Utils.execCommand(s"dx describe ${path} --json")
        val id = stdout.parseJson.asJsObject.fields.get("id") match {
            case Some(JsString(x)) => x.replaceAll("\"", "")
            case other => throw new Exception(s"Unexpected result ${other}")
        }
        id
    }

    it should "handle files in a root directory" in {
        val path = s"${TEST_PROJECT}:/wgEncodeUwRepliSeqBg02esG1bAlnRep1.bam.bai"
        val expectedId = describeDxFilePath(path)
        val dxFile : DXFile = DxPath.lookupDxURLFile(s"dx://${path}")
        dxFile.getId shouldBe(expectedId)
    }

    it should "handle files in a subdirectory directory" in {
        val path = s"${TEST_PROJECT}:/test_data/fileA"
        val expectedId = describeDxFilePath(path)
        val dxFile : DXFile = DxPath.lookupDxURLFile(s"dx://${path}")
        dxFile.getId shouldBe(expectedId)
    }

    it should "handle files with a colon" in {
        val path = s"${TEST_PROJECT}:/x:x.txt"
        val expectedId = describeDxFilePath(path)
        val dxFile : DXFile = DxPath.lookupDxURLFile(s"dx://${path}")
        dxFile.getId shouldBe(expectedId)
    }
}
