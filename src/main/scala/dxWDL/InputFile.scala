/** Generate an input file for a dx:workflow based on the
  WDL input file.

  In the documentation, we assume the workflow name is "myWorkflow"

For example, this is a Cromwell input file for workflow optionals:
{
  "optionals.arg1": 10,
  "optionals.mul2.i": 5,
  "optionals.add.a" : 1,
  "optionals.add.b" : 3
}

This is the dx JSON input:
{
  "0.arg1": 10,
  "stage-xxxx.i": 5,
  "stage-yyyy.a": 1,
  "stage-yyyy.b": 3
}
  */
package dxWDL

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.dnanexus.{DXAPI, DXDataObject, DXJSON, DXFile, DXProject, DXSearch, DXWorkflow}
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

    private def lookupDxPath(dxPath: String) : DXFile = {
        val components = dxPath.split(":/")
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
    }

    private def translateValue(v: JsValue) : JsValue= {
        v match {
            case JsString(s) if s.startsWith(UNIVERSAL_FILE_PREFIX) =>
                // Identify platform file paths by their prefix,
                // do a lookup, and create a dxlink
                val dxFile: DXFile = lookupDxPath(s.substring(UNIVERSAL_FILE_PREFIX.length))
                Utils.jsValueOfJsonNode(dxFile.getLinkAsJson)
            case JsArray(a) =>
                JsArray(a.map(x => translateValue(x)))
            case _ => v
        }
    }

    private def lookupStage(name: String,
                            wf: IR.Workflow,
                            stageDict: Map[String, DXWorkflow.Stage]) : String= {
        stageDict.get(name) match {
            case None =>
                System.err.println(s"stage dictionary: ${stageDict}")
                throw new Exception(s"Stage ${name} not found for workflow ${wf.name}")
            case Some(x) => x.getId()
        }
    }

    // Translate entries in the Cromwell input file, into a valid JSON
    // dx input file
    private def dxTranslate(wf: IR.Workflow,
                            wdlInputs: JsObject,
                            stageDict: Map[String, DXWorkflow.Stage]) : JsObject= {
        val m: Map[String, JsValue] = wdlInputs.fields.map{ case (key, v) =>
            val components = key.split("\\.")
            val dxKey: Option[String] =
                components.length match {
                    case 0|1 =>
                        System.err.println(s"Assuming ${key} is a comment, skipping")
                        None
                    case 2|3 if (components(0).startsWith("##")) =>
                        // Comments
                        None
                    case 2 =>
                        // workflow inputs are passed to the initial
                        // stage (common).
                        assert(components(0) == wf.name)
                        val varName = components(1)
                        val stageName = lookupStage(wf.name + "_" + Utils.COMMON, wf, stageDict)
                        Some(stageName + "." + varName)
                    case 3 =>
                        // parameters for call
                        // TODO: support calls inside scatters
                        assert(components(0) == wf.name)
                        val stageName = lookupStage(components(1), wf, stageDict)
                        val varName = components(2)
                        Some(stageName + "." + varName)
                    case _ =>
                        throw new Exception(s"String ${key} has too many components")
                }
            val dxVal = translateValue(v)
            dxKey match {
                case None => None
                case Some(x) => Some(x -> dxVal)
            }
        }.flatten.toMap
        JsObject(m)
    }

    // Build a dx input file, based on the wdl input file and the workflow
    def apply(wf: IR.Workflow,
              stageDict: Map[String, DXWorkflow.Stage],
              inputPath: Path,
              verbose: Boolean) : Unit = {
        // read the input file xxxx.json
        val wdlInputs: JsObject = Utils.readFileContent(inputPath).parseJson.asJsObject
        if (verbose) {
            wdlInputs.fields.foreach{ case (key, v) =>
                System.err.println(s"${key} -> ${v}")
            }
        }

        // translate the key-value entries
        val dxInputs: JsObject = dxTranslate(wf, wdlInputs, stageDict)

        // write back out as xxxx.dx.json
        val filename = Utils.replaceFileSuffix(inputPath, ".dx.json")
        val dxInputFile = inputPath.getParent().resolve(filename)
        Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
    }
}
