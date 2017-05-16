/** Generate an input file for a dx:workflow based on the
  * WDL input file.
  *
  * In the documentation, we assume the workflow name is "myWorkflow"
  */
package dxWDL

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.dnanexus.{DXAPI, DXDataObject, DXJSON, DXFile, DXProject, DXSearch}
import java.nio.file.{Path, Paths, Files}
import scala.collection.JavaConverters._
import spray.json._
import spray.json.DefaultJsonProtocol
import Utils.UNIVERSAL_FILE_PREFIX

object InputFile {

    private def lookupProject(projName: String): DXProject = {
        if (projName.startsWith("project-")) {
            // A project ID
            DXProject.getInstance(projName)
        } else {
            // A project name, resolve it
            val req: ObjectNode = DXJSON.getObjectBuilder()
                .put("name", projName)
                .put("limit", 2)
                .build()
            val rep = DXAPI.systemFindProjects(req, classOf[JsonNode])
            val repJs:JsValue = Utils.jsValueOfJsonNode(rep)

            val results = repJs.asJsObject.fields.get("results") match {
                case Some(JsArray(x)) => x
                case _ => throw new Exception(
                    s"Bad response from systemFindProject API call (${repJs.prettyPrint}), when resolving project ${projName}.")
            }
            if (results.length > 1)
                throw new Exception(s"Found more than one project named ${projName}")
            if (results.length == 0)
                throw new Exception(s"Project ${projName} not found")
            results(0).asJsObject.fields.get("id") match {
                case Some(JsString(id)) => DXProject.getInstance(id)
                case _ => throw new Exception(s"Bad response from SystemFindProject API call ${repJs.prettyPrint}")
            }
        }
    }

    private def lookupFile(dxProject: Option[DXProject], fileName: String): DXFile = {
        if (fileName.startsWith("file-")) {
            // A file ID
            DXFile.getInstance(fileName)
        } else {
            val fullPath = Paths.get(fileName)
            var folder = fullPath.getParent.toString
            if (!folder.startsWith("/"))
                folder = "/" + folder
            val baseName = fullPath.getFileName.toString

            val found:List[DXFile] = dxProject match  {
                case Some(x) =>
                    DXSearch.findDataObjects().nameMatchesExactly(baseName)
                        .inFolder(x, folder).withClassFile().execute().asList().asScala.toList
                case None =>
                    throw new Exception("File lookup requires project context")
            }
            if (found.length == 0)
                throw new Exception(s"File ${fileName} not found in project ${dxProject}")
            if (found.length > 1)
                throw new Exception(s"Found more than one file named ${fileName} in project ${dxProject}")
            found(0)
        }
    }

    private def lookupDxPath(dxPath: String, crntDxProject: DXProject) : JsValue = {
        val components = dxPath.split(":/")
        val dxFile: DXFile =
            if (components.length > 2) {
                throw new Exception(s"Path ${dxPath} cannot more than two components")
            } else if (components.length == 2) {
                val projName = components(0)
                val fileName = components(1)
                val dxProject = lookupProject(projName)
                lookupFile(Some(dxProject), fileName)
            } else if (components.length == 1) {
                val fileName = components(0)
                lookupFile(None, fileName)
            } else {
                throw new Exception(s"Path ${dxPath} is invalid")
            }
        Utils.jsValueOfJsonNode(dxFile.getLinkAsJson)
    }

    // 1. Convert fields of the form myWorkflow.xxxx to 0.xxxx. 'common' should
    //    also work, but does not.
    // 2.
    private def dxTranslate(wf: IR.Workflow, dxProject: DXProject, wdlInputs: JsObject) : JsObject= {
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
                        lookupDxPath(s.substring(UNIVERSAL_FILE_PREFIX.length), dxProject)
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
        val dxInputs: JsObject = dxTranslate(wf, dxProject, wdlInputs)
        Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
    }
}
