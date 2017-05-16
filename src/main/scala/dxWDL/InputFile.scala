/** Generate an input file for a dx:workflow based on the
  * WDL input file.
  *
  * In the documentation, we assume the workflow name is "myWorkflow"
  */
package dxWDL

import com.dnanexus.{DXFile, DXProject, DXSearch}
import java.nio.file.{Path, Paths, Files}
import scala.collection.JavaConverters._
import spray.json._
import spray.json.DefaultJsonProtocol
import Utils.UNIVERSAL_FILE_PREFIX

object InputFile {

    private def lookupFile(dxPath: String) : JsValue = {
        val components = dxPath.split(":/")
        if (components.length != 2)
            throw new Exception(s"Path ${dxPath} must specify project and file name")
        val projName = components(0)
        val fullPath = Paths.get(components(1))
        val folder = fullPath.getParent.toString
        val fileName = fullPath.getFileName.toString

        val proj = DXProject.getInstance(projName)
        val found:List[DXFile] = DXSearch.findDataObjects().nameMatchesExactly(fileName)
            .inFolder(proj, folder).withClassFile().execute().asList().asScala.toList
        if (found.length == 0)
            throw new Exception(s"Path ${dxPath} not found")
        if (found.length > 1)
            throw new Exception(s"Found more than one result for ${dxPath}")
        val dxFile = found(0)
        Utils.jsValueOfJsonNode(dxFile.getLinkAsJson)
/*        val buf = s"""|$dnanexus_link": {
                      |"project": ${dxFile.getProject.getId()},
                      |"id": ${dxFile.getId()}
                      |}"""
        buf.parseJson*/
    }

    // 1. Convert fields of the form myWorkflow.xxxx to 0.xxxx. 'common' should
    //    also work, but does not.
    // 2.
    private def dxTranslate(wf: IR.Workflow, wdlInputs: JsObject) : JsObject= {
        val m: Map[String, JsValue] = wdlInputs.fields.map{ case (key, v) =>
            val components = key.split("\\.")
            val dxKey =
                if (components.length == 0) {
                    throw new Exception(s"String ${key} cannot be a JSON field key")
                } else if (components.length == 1) {
                    key
                } else {
                    val call = components.head
                    val suffix = components.tail.mkString(".")
                    val stageName =
                        if (call == wf.name) "0"
                        else call
                    stageName + "." + suffix
                }
            val dxVal =
                v match {
                    case JsString(s) if s.startsWith(UNIVERSAL_FILE_PREFIX) =>
                        // Identify platform file paths by their prefix,
                        // do a lookup, and create a dxlink
                        lookupFile(s.substring(UNIVERSAL_FILE_PREFIX.length))
                    case _ => v
                }
            dxKey -> dxVal
        }.toMap
        JsObject(m)
    }

    // Build a dx input file, based on the wdl input file and the workflow
    def apply(wf: IR.Workflow,
              dxProject: DXProject,
              wdlInputFile: Path,
              dxInputFile: Path,
              verbose: Boolean) : Unit = {
        // read the input file
        val wdlInputs: JsObject = Utils.readFileContent(wdlInputFile).parseJson.asJsObject
        wdlInputs.fields.foreach{ case (key, v) =>
            System.err.println(s"${key} -> ${v}")
        }
        val dxInputs: JsObject = dxTranslate(wf, wdlInputs)
        Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
    }
}
