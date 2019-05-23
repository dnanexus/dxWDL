// Create a manifest for download agent (https://github.com/dnanexus/dxda).
//

package dxWDL.dx

import com.dnanexus.{DXDataObject, DXFile, DXProject}
import spray.json._

object CreateManifest {
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
    private def processFile(desc : DxDescribe) : JsValue = {
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
        JsObject("id" -> JsString(desc.dxobj.getId),
                 "name" -> JsString(desc.name),
                 "folder" -> JsString(desc.folder),
                 "parts" -> JsObject(parts))
    }

    // The project is just a hint. The files don't have to actually reside in it.
    def apply(dxPaths: Seq[String],
              dxProject : DXProject) : JsValue = {

        // resolve the paths
        val resolvedObjects : Map[String, DXDataObject] = DxBulkResolve.apply(dxPaths, dxProject)
        val files : Vector[DXFile] = resolvedObjects.values.collect{
            case x : DXFile => x
        }.toVector

        // collect all the information per file
        val fileDescs : Map[DXFile, DxDescribe] = DxBulkDescribe.apply(files, None, parts = true)

        // sort the files by proj
        val filesByProject : Map[DXProject, Vector[DxDescribe]] =
            fileDescs.foldLeft(Map.empty[DXProject, Vector[DxDescribe]]) {
                case (accu, (dxFile, dxDesc)) =>
                    val dxProj = dxDesc.project.get
                    accu.get(dxProj) match {
                        case None =>
                            accu + (dxProj -> Vector(dxDesc))
                        case Some(vec) =>
                            accu + (dxProj -> (vec :+ dxDesc))
                    }
            }

        val m : Map[String, JsValue] = filesByProject.map{ case (dxProj, files) =>
            dxProj.getId -> JsArray(files.map(processFile).toVector)
        }
        JsObject(m)
    }
}
