// Create a manifest for dxfuse (https://github.com/dnanexus/dxfuse).
//

package dx.core.io

import java.nio.file.Path

import dx.api.{DxApi, DxArchivalState, DxFile}
import spray.json._

case class DxfuseManifest(value: JsValue)

case class DxfuseManifestBuilder(dxApi: DxApi) {
  def apply(fileToLocalMapping: Map[DxFile, Path],
            workerPaths: DxWorkerPaths): Option[DxfuseManifest] = {
    if (fileToLocalMapping.isEmpty) {
      return None
    }

    val files = fileToLocalMapping.map {
      case (dxFile, path) =>
        // we expect that the files will have already been bulk described
        assert(dxFile.hasCachedDesc)
        // check that the files are not archived
        if (dxFile.describe().archivalState != DxArchivalState.Live) {
          throw new Exception(s"file ${dxFile} is not live")
        }

        val parentDir = path.getParent.toString
        // remove the mountpoint from the directory. We need
        // paths that are relative to the mount point.
        val mountDir = workerPaths.getDxfuseMountDir().toString
        assert(parentDir.startsWith(mountDir))
        val relParentDir = s"/${parentDir.stripPrefix(mountDir)}"

        val desc = dxFile.describe()
        JsObject(
            "file_id" -> JsString(dxFile.id),
            "parent" -> JsString(relParentDir),
            "proj_id" -> JsString(desc.project),
            "fname" -> JsString(desc.name),
            "size" -> JsNumber(desc.size),
            "ctime" -> JsNumber(desc.created),
            "mtime" -> JsNumber(desc.modified)
        )
    }.toVector

    Some(
        DxfuseManifest(
            JsObject("files" -> JsArray(files), "directories" -> JsArray(Vector.empty))
        )
    )
  }
}
