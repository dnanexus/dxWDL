// Create a manifest for download agent (https://github.com/dnanexus/dxda).
//

package dxWDL.dx

import java.nio.file.Path
import spray.json._

case class DxdaManifest(value: JsValue)

object DxdaManifest {
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
  private def processFile(desc: DxFileDescribe, destination: Path): JsValue = {
    val destinationFile: java.io.File = destination.toFile
    val name = destinationFile.getName
    val folder = destinationFile.getParent
    JsObject("id" -> JsString(desc.id), "name" -> JsString(name), "folder" -> JsString(folder))
  }

  // The project is just a hint. The files don't have to actually reside in it.
  def apply(file2LocalMapping: Map[String, (DxFile, Path)]): DxdaManifest = {
    // collect all the information per file
    val files: Vector[DxFile] = file2LocalMapping.values.map(_._1).toVector
    val fileDescs: Map[String, (DxFile, DxFileDescribe)] =
      DxFile
        .bulkDescribe(files)
        .map {
          case (dxFile, desc) => dxFile.id -> (dxFile, desc)
        }

    // Make sure they are all in the live state. Archived files cannot be accessed.
    fileDescs.foreach {
      case (_, (dxFile, desc)) =>
        if (desc.archivalState != DxArchivalState.LIVE)
          throw new Exception(
              s"file ${dxFile.id} is not live, it is in ${desc.archivalState} state"
          )
    }

    // create a sub-map per container
    val fileDescsByContainer: Map[DxProject, Map[String, (DxFile, DxFileDescribe)]] =
      fileDescs.foldLeft(Map.empty[DxProject, Map[String, (DxFile, DxFileDescribe)]]) {
        case (accu, (_, (dxFile, dxDesc))) =>
          val container = DxProject.getInstance(dxDesc.project)
          accu.get(container) match {
            case None =>
              accu + (container -> Map(dxFile.id -> (dxFile, dxDesc)))
            case Some(m) =>
              accu + (container -> (m + (dxFile.id -> (dxFile, dxDesc))))
          }
      }

    val m: Map[String, JsValue] = fileDescsByContainer.map {
      case (dxContainer, fileDescs) =>
        val projectFilesToLocalPath: Vector[JsValue] =
          fileDescs.map {
            case (fid, (_, dxDesc)) =>
              val (_, local: Path) = file2LocalMapping(fid)
              processFile(dxDesc, local)
          }.toVector
        dxContainer.getId -> JsArray(projectFilesToLocalPath)
    }
    new DxdaManifest(JsObject(m))
  }
}
