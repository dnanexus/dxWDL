// Create a manifest for dxfs2 (https://github.com/dnanexus/dxfs2).
//

package dxWDL.dx

import com.dnanexus.{DXFile}
import java.nio.file.Path
import spray.json._

import dxWDL.util.DxIoFunctions

case class Dxfs2Manifest(value : JsValue)

object Dxfs2Manifest {
    def apply(file2LocalMapping: Map[DXFile, Path],
              dxIoFunctions : DxIoFunctions) : Dxfs2Manifest = {
        val files = file2LocalMapping.map{
            case (dxFile, path) =>
                val parentDir = path.getParent()
                val fDesc = dxIoFunctions.fileInfoDir(dxFile)
                val size = fDesc.size match {
                    case None => throw new Exception(s"File is missing the size field ${fDesc}")
                    case Some(x) => x
                }
                JsObject(
                    "proj_id" -> JsString(fDesc.container.getId),
                    "file_id" -> JsString(dxFile.getId),
                    "parent" -> JsString(parentDir.toString),
                    "fname" -> JsString(fDesc.name),
                    "size" -> JsNumber(size),
                    "ctime" -> JsNumber(fDesc.created),
                    "mtime" -> JsNumber(fDesc.modified))
        }.toVector

        Dxfs2Manifest(
            JsObject("files" -> JsArray(files),
                     "directories" -> JsArray(Vector.empty))
        )
    }
}
