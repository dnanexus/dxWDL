package dxWDL.dx

import com.dnanexus.DXProject
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class CreateManifestTest extends FlatSpec with Matchers {

    val TEST_PROJECT = "dxWDL_playground"
    lazy val dxTestProject : DXProject =
        try {
            DxPath.lookupProject(TEST_PROJECT)
        } catch {
            case e : Exception =>
                throw new Exception(s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                                        |the platform on staging.""".stripMargin)
        }

    it should "create manifests for dxda" in {
        // create a manifest
        val manifest : JsValue = CreateManifest.apply(
            List(
                s"dx://${TEST_PROJECT}:/test_data/fileA",
                s"dx://${TEST_PROJECT}:/test_data/fileB",
                s"dx://${TEST_PROJECT}:/test_data/fileC"),
            dxTestProject)

        val fileA = JsObject("id" -> JsString("file-FGqFGBQ0ffPPkYP19gBvFkZy"),
                             "name" -> JsString("fileA"),
                             "folder" -> JsString("/test_data"),
                             "parts" -> JsObject(
                                 "1" -> JsObject(
                                     "size" -> JsNumber(42),
                                     "md5" -> JsString("71565d7f4dc0760457eb252a31d45964")
                                 )))
        val fileB = JsObject("id" -> JsString("file-FGqFJ8Q0ffPGVz3zGy4FK02P"),
                             "name" -> JsString("fileB"),
                             "folder" -> JsString("/test_data"),
                             "parts" -> JsObject(
                                 "1" -> JsObject(
                                     "size" -> JsNumber(8632),
                                     "md5" -> JsString("903fb36e76d77c1ac95e90dbde9508df")
                                 )))
        val fileC = JsObject("id" -> JsString("file-FGzzpkQ0ffPJX74548Vp6670"),
                             "name" -> JsString("fileC"),
                             "folder" -> JsString("/test_data"),
                             "parts" -> JsObject(
                                 "1" -> JsObject(
                                     "size" -> JsNumber(42),
                                     "md5" -> JsString("57a8ec78ad9b08b2add5b6656017e34c")
                                 )))

        manifest shouldBe(
            JsObject(
                dxTestProject.getId -> JsArray(Vector(fileA, fileB, fileC))))
    }
}
