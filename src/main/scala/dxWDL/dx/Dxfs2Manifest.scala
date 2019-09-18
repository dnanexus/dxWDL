// Create a manifest for dxfs2 (https://github.com/dnanexus/dxfs2).
//

package dxWDL.dx

import com.dnanexus.{DXFile}
import java.nio.file.Path
import spray.json._

case class Dxfs2Manifest(value : JsValue)

object Dxfs2Manifest {
/*
  Start with paths like this:
    "dx://dxWDL_playground:/test_data/fileB",
    "dx://dxWDL_playground:/test_data/fileC",

  and generate a manifest like this:

   val fileB = JsObject(
                   "proj_id" -> JsString("proj-xxxx"),
                   "file_id" -> JsString("file-yyyy"),
                   "parent" -> "/")
   val fileC = JsObject(
                   "proj_id" -> JsString("proj-uuuu"),
                   "file_id" -> JsString("file-vvvv"),
                   "parent" -> "/")

   JsObject("files" -> [fileB, fileC])

 */

    // The project is just a hint. The files don't have to actually reside in it.
    def apply(file2LocalMapping: Map[DXFile, Path]) : Dxfs2Manifest = ???
}
