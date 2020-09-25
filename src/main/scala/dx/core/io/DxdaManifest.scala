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

  // create a manifest for a single file
  private def processFile(dxFile: DxFile, destination: Path): JsValue = {
    val destinationFile: java.io.File = destination.toFile
    val name = destinationFile.getName
    val folder = destinationFile.getParent
    JsObject("id" -> JsString(dxFile.id), "name" -> JsString(name), "folder" -> JsString(folder))
  }

  // The project is just a hint. The files don't have to actually reside in it.
  def apply(fileToLocalMapping: Map[String, (DxFile, Path)]): Option[DxdaManifest] = {
    if (fileToLocalMapping.isEmpty) {
      return None
    }

    // collect all the information per file
    val files: Vector[DxFile] = fileToLocalMapping.values.map(_._1).toVector

    // Make sure they are all in the live state. Archived files cannot be accessed.
    files.map(_.describe()).foreach { desc =>
      if (desc.archivalState != DxArchivalState.Live)
        throw new Exception(
            s"file ${desc.id} is not live, it is in ${desc.archivalState} state"
        )
    }

    // create a sub-map per container
    val fileDescsByContainer: Map[DxProject, Vector[DxFile]] =
      files.groupBy(x => dxApi.project(x.describe().project))

    val m: Map[String, JsValue] = fileDescsByContainer.map {
      case (dxContainer, containerFiles) =>
        val projectFilesToLocalPath: Vector[JsValue] =
          containerFiles.map { dxFile =>
            val (_, local: Path) = fileToLocalMapping(dxFile.id)
            processFile(dxFile, local)
          }
        dxContainer.getId -> JsArray(projectFilesToLocalPath)
    }
    Some(DxdaManifest(JsObject(m)))
  }
}
