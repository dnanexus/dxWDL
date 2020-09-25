// Create a manifest for download agent (https://github.com/dnanexus/dxda).
//
package dx.core.io

import java.nio.file.Path

import dx.api._
import spray.json._

case class DxdaManifest(value: JsObject)

case class DxdaManifestBuilder(dxApi: DxApi) {
  /*
  Start with paths like this:
    "dx://dxWDL_playground:/test_data/fileB",
    "dx://dxWDL_playground:/test_data/fileC",

  and generate a manifest like this:

   val fileA = JsObject("id" -> JsString("file-FGqFGBQ0ffPPkYP19gBvFkZy"),
                         "name" -> JsString("fileA"),
                         "folder" -> JsString("/test_data"))

  JsObject(
      projId1 -> JsArray(fileA, ...),
      projId2 -> JsArray(...)
  )

   */

  // create a manifest entry for a single file
  private def createFileEntry(dxFile: DxFile, destination: Path): JsValue = {
    val destinationFile: java.io.File = destination.toFile
    val name = destinationFile.getName
    val folder = destinationFile.getParent
    JsObject("id" -> JsString(dxFile.id), "name" -> JsString(name), "folder" -> JsString(folder))
  }

  /**
    *
    * @param fileToLocalMapping mapping of
    * @return
    */
  def apply(fileToLocalMapping: Map[DxFile, Path]): Option[DxdaManifest] = {
    if (fileToLocalMapping.isEmpty) {
      return None
    }

    val filesByContainer: Map[DxProject, Vector[DxFile]] =
      fileToLocalMapping.keys.toVector.groupBy { file =>
        // make sure file is pre-described
        assert(file.hasCachedDesc)
        // make sure file is in the live state - archived files cannot be accessed.
        if (file.describe().archivalState != DxArchivalState.Live) {
          throw new Exception(s"file ${file} is not live")
        }
        // create a sub-map per container
        dxApi.project(file.describe().project)
      }

    val idToPath = fileToLocalMapping.map {
      case (dxFile, path) => dxFile.id -> path
    }

    val manifest: Map[String, JsValue] = filesByContainer.map {
      case (dxContainer, containerFiles) =>
        val projectFilesToLocalPath: Vector[JsValue] =
          containerFiles.map { dxFile =>
            createFileEntry(dxFile, idToPath(dxFile.id))
          }
        dxContainer.getId -> JsArray(projectFilesToLocalPath)
    }

    Some(DxdaManifest(JsObject(manifest)))
  }
}
