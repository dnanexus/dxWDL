package dx.core.io

import java.nio.file.{Path, Paths}

import dx.Assumptions.isLoggedIn
import dx.Tags.ApiTest
import dx.api.{DxApi, DxFile, DxProject}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.util.Logger

class DxdaManifestTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)
  private val dxApi: DxApi = DxApi(Logger.Quiet)
  private val TestProject = "dxWDL_playground"
  private val ArchivedProject = "ArchivedStuff"

  private lazy val dxTestProject: DxProject =
    try {
      dxApi.resolveProject(TestProject)
    } catch {
      case _: Exception =>
        throw new Exception(
            s"""|Could not find project ${TestProject}, you probably need to be logged into
                |the platform on staging.""".stripMargin
        )
    }

  private lazy val dxArchivedProj: DxProject =
    try {
      dxApi.resolveProject(ArchivedProject)
    } catch {
      case _: Exception =>
        throw new Exception(
            s"""|Could not find project ${TestProject}, you probably need to be logged into
                |the platform on staging.""".stripMargin
        )
    }

  // describe a platform file, including its parts, with the dx-toolkit. We can then compare
  // it to what we get from our DxdaManifest code.
  //
  it should "create manifests for dxda" taggedAs ApiTest in {
    val fileDir: Map[String, Path] = Map(
        s"dx://${TestProject}:/test_data/fileA" -> Paths.get("inputs/A"),
        s"dx://${TestProject}:/test_data/fileB" -> Paths.get("inputs/B"),
        s"dx://${TestProject}:/test_data/fileC" -> Paths.get("inputs/C")
    )

    // resolve the paths
    val (idToUri, resolvedFiles) =
      dxApi
        .resolveDataObjectBulk(fileDir.keys.toVector, dxTestProject)
        .map {
          case (dxUri, dxFile: DxFile) => ((dxFile.id, dxUri), dxFile)
          case other                   => throw new Exception(s"expected file, not ${other}")
        }
        .unzip
    val idToUriMap = idToUri.toMap
    // describe the files
    val describedFiles: Map[String, DxFile] = dxApi
      .describeFilesBulk(resolvedFiles.toVector)
      .map { dxFile =>
        val uri = idToUriMap(dxFile.id)
        uri -> dxFile
      }
      .toMap

    val filesInManifest: Map[DxFile, Path] = describedFiles.map {
      case (dxUri, dxFile: DxFile) => dxFile -> fileDir(dxUri)
    }

    // create a manifest
    val manifest: Option[DxdaManifest] = DxdaManifestBuilder(dxApi).apply(filesInManifest)

    // compare to data obtained with dx-toolkit
    val expected: Vector[JsValue] = describedFiles
      .map {
        case (dxUri, dxFile) =>
          val local: Path = fileDir(dxUri)

          // add the target folder and name
          val fields = Map(
              "id" -> JsString(dxFile.id),
              "name" -> JsString(local.toFile.getName),
              "folder" -> JsString(local.toFile.getParent)
          )
          JsObject(fields)
      }
      .toVector
      .reverse

    manifest should matchPattern {
      case Some(DxdaManifest(JsObject(fields))) if fields.size == 1 =>
    }
    manifest.get.value.fields.get(dxTestProject.id) match {
      case Some(JsArray(array)) => array should contain theSameElementsAs expected
      case _                    => throw new Exception("expected array")
    }
  }

  it should "detect and provide legible error for archived files" taggedAs ApiTest in {
    val fileDir: Map[String, Path] = Map(
        s"dx://${ArchivedProject}:/Catch22.txt" -> Paths.get("inputs/A"),
        s"dx://${ArchivedProject}:/LICENSE" -> Paths.get("inputs/B"),
        s"dx://${ArchivedProject}:/README" -> Paths.get("inputs/C")
    )

    // resolve the paths
    val (uris, resolvedFiles) =
      dxApi
        .resolveDataObjectBulk(fileDir.keys.toVector, dxArchivedProj)
        .map {
          case (dxUri, dxFile: DxFile) => (dxUri, dxFile)
          case other                   => throw new Exception(s"expected file, not ${other}")
        }
        .unzip

    // describe the files
    val describedFiles = dxApi.describeFilesBulk(resolvedFiles.toVector)
    val filesInManifest: Map[DxFile, Path] = uris
      .zip(describedFiles)
      .map {
        case (dxUri, dxFile: DxFile) => dxFile -> fileDir(dxUri)
      }
      .toMap

    // Creating a manifest should fail, because some of the files are archived
    assertThrows[Exception] {
      DxdaManifestBuilder(dxApi).apply(filesInManifest)
    }
  }
}
