// Create a manifest for dxfuse (https://github.com/dnanexus/dxfuse).
//

package dxWDL.dx

import java.nio.file.Path
import spray.json._

import dxWDL.util.DxIoFunctions

case class DxfuseManifest(value : JsValue)

object DxfuseManifest {
    def apply(file2LocalMapping: Map[DxFile, Path],
              dxIoFunctions : DxIoFunctions) : DxfuseManifest = {
        if (file2LocalMapping.isEmpty)
            return DxfuseManifest(JsNull)

        val files = file2LocalMapping.map{
            case (dxFile, path) =>
                val parentDir = path.getParent().toString

                // remove the mountpoint from the directory. We need
                // paths that are relative to the mount point.
                val mountDir = dxIoFunctions.config.dxfuseMountpoint.toString
                assert(parentDir.startsWith(mountDir))
                val relParentDir = "/" + parentDir.stripPrefix(mountDir)

                val fDesc = dxIoFunctions.fileInfoDir(dxFile)
                val size = fDesc.size match {
                    case None => throw new Exception(s"File is missing the size field ${fDesc}")
                    case Some(x) => x
                }
                JsObject(
                    "proj_id" -> JsString(fDesc.project),
                    "file_id" -> JsString(dxFile.id),
                    "parent" -> JsString(relParentDir),
                    "fname" -> JsString(fDesc.name),
                    "size" -> JsNumber(size),
                    "ctime" -> JsNumber(fDesc.created),
                    "mtime" -> JsNumber(fDesc.modified))
        }.toVector

        DxfuseManifest(
            JsObject("files" -> JsArray(files),
                     "directories" -> JsArray(Vector.empty))
        )
    }
}
