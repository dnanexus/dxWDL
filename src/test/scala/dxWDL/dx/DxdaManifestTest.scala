package dxWDL.dx

import com.dnanexus.{DXDataObject, DXFile, DXProject}
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class DxdaManifestTest extends FlatSpec with Matchers {

    val TEST_PROJECT = "dxWDL_playground"
    lazy val dxTestProject : DXProject =
        try {
            DxBulkResolve.lookupProject(TEST_PROJECT)
        } catch {
            case e : Exception =>
                throw new Exception(s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                                        |the platform on staging.""".stripMargin)
        }

    it should "create manifests for dxda" in {

        val fileDir : Map[String, Path] = Map(
            s"dx://${TEST_PROJECT}:/test_data/fileA" -> Paths.get("inputs/A"),
            s"dx://${TEST_PROJECT}:/test_data/fileB" -> Paths.get("inputs/B"),
            s"dx://${TEST_PROJECT}:/test_data/fileC" -> Paths.get("inputs/C")
        )

        // resolve the paths
        val resolvedObjects : Map[String, DXDataObject] = DxBulkResolve.apply(fileDir.keys.toVector,
                                                                              dxTestProject)
        val filesInManifest : Map[DXFile, Path] = resolvedObjects.map{
            case (dxPath, dataObj) =>
                val dxFile = dataObj.asInstanceOf[DXFile]
                val local : Path = fileDir(dxPath)
                dxFile -> local
        }.toMap

        // create a manifest
        val manifest : DxdaManifest = DxdaManifest.apply(filesInManifest)

        val fileA = JsObject("id" -> JsString("file-FGqFGBQ0ffPPkYP19gBvFkZy"),
                             "name" -> JsString("A"),
                             "folder" -> JsString("inputs"),
                             "parts" -> JsObject(
                                 "1" -> JsObject(
                                     "size" -> JsNumber(42),
                                     "md5" -> JsString("71565d7f4dc0760457eb252a31d45964")
                                 )))
        val fileB = JsObject("id" -> JsString("file-FGqFJ8Q0ffPGVz3zGy4FK02P"),
                             "name" -> JsString("B"),
                             "folder" -> JsString("inputs"),
                             "parts" -> JsObject(
                                 "1" -> JsObject(
                                     "size" -> JsNumber(8632),
                                     "md5" -> JsString("903fb36e76d77c1ac95e90dbde9508df")
                                 )))
        val fileC = JsObject("id" -> JsString("file-FGzzpkQ0ffPJX74548Vp6670"),
                             "name" -> JsString("C"),
                             "folder" -> JsString("inputs"),
                             "parts" -> JsObject(
                                 "1" -> JsObject(
                                     "size" -> JsNumber(42),
                                     "md5" -> JsString("57a8ec78ad9b08b2add5b6656017e34c")
                                 )))

        manifest shouldBe(
            DxdaManifest(
                JsObject(
                    dxTestProject.getId -> JsArray(Vector(fileA, fileB, fileC)))
            )
        )
    }
}
