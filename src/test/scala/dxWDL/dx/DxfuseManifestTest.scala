package dxWDL.dx

import java.nio.file.{Path, Paths}

import dx.core.io.DxfuseManifest
import dx.api.{DxFile, DxPath, DxProject}
import dx.core.languages.wdl.DxFileAccessProtocol
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import dxWDL.util._

class DxfuseManifestTest extends AnyFlatSpec with Matchers {
  private val runtimeDebugLevel = 0
  private lazy val homeDir = Paths.get(System.getProperty("user.home"))
  private lazy val dxPathConfig =
    DxPathConfig.apply(homeDir, streamAllFiles = false, verbose = runtimeDebugLevel >= 1)
  private lazy val dxIoFunctions = DxFileAccessProtocol(Map.empty, dxPathConfig, runtimeDebugLevel)

  it should "detect and provide legible error for archived files" in {
    val ARCHIVED_PROJ = "ArchivedStuff"
    val dxArchivedProj: DxProject = DxPath.resolveProject(ARCHIVED_PROJ)

    val fileDir: Map[String, Path] = Map(
        s"dx://${ARCHIVED_PROJ}:/Catch22.txt" -> Paths.get("inputs/A"),
        s"dx://${ARCHIVED_PROJ}:/LICENSE" -> Paths.get("inputs/B"),
        s"dx://${ARCHIVED_PROJ}:/README" -> Paths.get("inputs/C")
    )

    // resolve the paths
    val resolvedObjects: Map[String, DxDataObject] =
      DxPath.resolveBulk(fileDir.keys.toVector, dxArchivedProj)
    val filesInManifest: Map[DxFile, Path] = resolvedObjects.map {
      case (dxPath, dataObj) =>
        val dxFile = dataObj.asInstanceOf[DxFile]
        val local: Path = fileDir(dxPath)
        dxFile -> local
    }

    // Creating a manifest should fail, because some of the files are archived
    assertThrows[Exception] {
      DxfuseManifest.apply(filesInManifest, dxIoFunctions)
    }
  }
}
