// Create a manifest for download agent (https://github.com/dnanexus/dxda).
//

package dxWDL.dx

import com.dnanexus.{DXFile, DXContainer}
import java.nio.file.Path
import spray.json._

case class DxdaManifest(value : JsValue)

object DxdaManifest {
/*
  Start with paths like this:
    "dx://dxWDL_playground:/test_data/fileB",
    "dx://dxWDL_playground:/test_data/fileC",

  and generate a manifest like this:

   val fileA = JsObject("id" -> JsString("file-FGqFGBQ0ffPPkYP19gBvFkZy"),
                         "name" -> JsString("fileA"),
                         "folder" -> JsString("/test_data")
                         "parts" -> JsObject(
                         "1" -> JsObject(
                                  "size" -> JsNumber(42),
                                  "md5" -> JsString("71565d7f4dc0760457eb252a31d45964")
 )))

  JsObject(
      projId1 -> JsArray(fileA, ...),
      projId2 -> JsArray(...)
  )

 */

    // create a manifest for a single file
    private def processFile(desc : DxDescribe,
                            destination : Path) : JsValue = {
        val partsRaw : Map[Int, DxFilePart] = desc.parts match {
            case None => throw new Exception(
                s"""|No options for file ${desc.dxobj.getId},
                    |name=${desc.name} folder=${desc.folder}""".stripMargin)
            case Some(x) => x
        }
        val parts: Map[String, JsValue] = partsRaw.map{ case (i, part) =>
            i.toString -> JsObject(
                "size" -> JsNumber(part.size),
                "md5" -> JsString(part.md5))
        }
        val destinationFile : java.io.File = destination.toFile()
        val name = destinationFile.getName()
        val folder = destinationFile.getParent().toString
        JsObject("id" -> JsString(desc.dxobj.getId),
                 "name" -> JsString(name),
                 "folder" -> JsString(folder),
                 "parts" -> JsObject(parts))
    }

    // The project is just a hint. The files don't have to actually reside in it.
    def apply(file2LocalMapping: Map[DXFile, Path]) : DxdaManifest = {

        // collect all the information per file
        val fileDescs : Map[DXFile, DxDescribe] = DxBulkDescribe.apply(file2LocalMapping.keys.toVector,
                                                                       parts = true)

        // create a sub-map per project
        val fileDescsByProject : Map[DXContainer, Map[DXFile, DxDescribe]] =
            fileDescs.foldLeft(Map.empty[DXContainer, Map[DXFile, DxDescribe]]) {
                case (accu, (dxFile, dxDesc)) =>
                    val container : DXContainer = dxDesc.container
                    accu.get(container) match {
                        case None =>
                            accu + (container -> Map(dxFile -> dxDesc))
                        case Some(m) =>
                            accu + (container -> (m + (dxFile -> dxDesc)))
                    }
            }

        val m : Map[String, JsValue] = fileDescsByProject.map{
            case (dxProj, fileDescs) =>
                val projectFilesToLocalPath : Vector[JsValue] =
                    fileDescs.map{
                        case (dxFile, dxDesc) =>
                            val local: Path = file2LocalMapping(dxFile)
                            processFile(dxDesc, local)
                    }.toVector
                dxProj.getId -> JsArray(projectFilesToLocalPath)
        }.toMap
        new DxdaManifest(JsObject(m))
    }
}
