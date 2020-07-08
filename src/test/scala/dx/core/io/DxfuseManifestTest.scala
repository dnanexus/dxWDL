package dx.core.io

import java.nio.file.{Path, Paths}

import dx.api.{DxApi, DxDataObject, DxFile, DxProject}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.util.Logger

class DxfuseManifestTest extends AnyFlatSpec with Matchers {
  private val DX_API: DxApi = DxApi(Logger.Quiet)
  private lazy val homeDir = Paths.get(System.getProperty("user.home"))
  private lazy val dxPathConfig =
    DxPathConfig.apply(homeDir, streamAllFiles = false, DX_API.logger)

  it should "detect and provide legible error for archived files" in {
    val ARCHIVED_PROJ = "ArchivedStuff"
    val dxArchivedProj: DxProject = DX_API.resolveProject(ARCHIVED_PROJ)

    val fileDir: Map[String, Path] = Map(
        s"dx://${ARCHIVED_PROJ}:/Catch22.txt" -> Paths.get("inputs/A"),
        s"dx://${ARCHIVED_PROJ}:/LICENSE" -> Paths.get("inputs/B"),
        s"dx://${ARCHIVED_PROJ}:/README" -> Paths.get("inputs/C")
    )

    // resolve the paths
    val resolvedObjects: Map[String, DxDataObject] =
      DX_API.resolveBulk(fileDir.keys.toVector, dxArchivedProj)
    val filesInManifest: Map[DxFile, Path] = resolvedObjects.map {
      case (dxPath, dataObj) =>
        val dxFile = dataObj.asInstanceOf[DxFile]
        val local: Path = fileDir(dxPath)
        dxFile -> local
    }

    // Creating a manifest should fail, because some of the files are archived
    assertThrows[Exception] {
      DxfuseManifestBuilder(DX_API).apply(filesInManifest, Map.empty, dxPathConfig)
    }
  }
}
