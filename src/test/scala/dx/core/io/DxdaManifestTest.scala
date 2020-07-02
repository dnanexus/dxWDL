package dx.core.io

import java.nio.file.{Path, Paths}

import dx.api.{DxApi, DxDataObject, DxFile, DxProject}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.util.Logger

class DxdaManifestTest extends AnyFlatSpec with Matchers {
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

  // describe a platform file, including its parts, with the dx-toolkit. We can then compare
  // it to what we get from our DxdaManifest code.
  //
  it should "create manifests for dxda" in {
    val fileDir: Map[String, Path] = Map(
        s"dx://${TEST_PROJECT}:/test_data/fileA" -> Paths.get("inputs/A"),
        s"dx://${TEST_PROJECT}:/test_data/fileB" -> Paths.get("inputs/B"),
        s"dx://${TEST_PROJECT}:/test_data/fileC" -> Paths.get("inputs/C")
    )

    // resolve the paths
    val resolvedObjects: Map[String, DxDataObject] =
      dxApi.resolveBulk(fileDir.keys.toVector, dxTestProject)
    val filesInManifest: Map[String, (DxFile, Path)] = resolvedObjects.map {
      case (dxPath, dataObj) =>
        val dxFile = dataObj.asInstanceOf[DxFile]
        val local: Path = fileDir(dxPath)
        dxFile.id -> (dxFile, local)
    }

    // create a manifest
    val manifest: DxdaManifest = DxdaManifestBuilder(dxApi).apply(filesInManifest)

    // compare to data obtained with dx-toolkit
    val expected: Vector[JsValue] = resolvedObjects
      .map {
        case (dxPath, dataObj) =>
          val dxFile = dataObj.asInstanceOf[DxFile]
          val local: Path = fileDir(dxPath)

          // add the target folder and name
          val fields = Map(
              "id" -> JsString(dxFile.getId),
              "name" -> JsString(local.toFile.getName),
              "folder" -> JsString(local.toFile.getParent)
          )
          JsObject(fields)
      }
      .toVector
      .reverse

    val manifestValue = manifest.value.asJsObject.fields
    manifestValue.size shouldBe 1
    manifestValue.get(dxTestProject.getId) match {
      case Some(JsArray(array)) => array should contain theSameElementsAs expected
      case _                    => throw new Exception("expected array")
    }
  }

  it should "detect and provide legible error for archived files" in {
    val ARCHIVED_PROJ = "ArchivedStuff"
    val dxArchivedProj: DxProject = dxApi.resolveProject(ARCHIVED_PROJ)

    val fileDir: Map[String, Path] = Map(
        s"dx://${ARCHIVED_PROJ}:/Catch22.txt" -> Paths.get("inputs/A"),
        s"dx://${ARCHIVED_PROJ}:/LICENSE" -> Paths.get("inputs/B"),
        s"dx://${ARCHIVED_PROJ}:/README" -> Paths.get("inputs/C")
    )

    // resolve the paths
    val resolvedObjects: Map[String, DxDataObject] =
      dxApi.resolveBulk(fileDir.keys.toVector, dxArchivedProj)
    val filesInManifest: Map[String, (DxFile, Path)] = resolvedObjects.map {
      case (dxPath, dataObj) =>
        val dxFile = dataObj.asInstanceOf[DxFile]
        val local: Path = fileDir(dxPath)
        dxFile.id -> (dxFile, local)
    }

    // Creating a manifest should fail, because some of the files are archived
    assertThrows[Exception] {
      DxdaManifestBuilder(dxApi).apply(filesInManifest)
    }
  }
}
