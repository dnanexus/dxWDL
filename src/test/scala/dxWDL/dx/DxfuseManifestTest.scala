package dxWDL.dx

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}

import dxWDL.util._

class DxfuseManifestTest extends FlatSpec with Matchers {
  private val runtimeDebugLevel = 0
  lazy val homeDir = Paths.get(System.getProperty("user.home"))
  lazy val dxPathConfig = DxPathConfig.apply(homeDir, None, runtimeDebugLevel >= 1)
  lazy private val dxIoFunctions = DxIoFunctions(Map.empty, dxPathConfig, runtimeDebugLevel)

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
    }.toMap

    // Creating a manifest should fail, because some of the files are archived
    assertThrows[Exception] {
      DxfuseManifest.apply(filesInManifest, dxIoFunctions)
    }
  }
}
