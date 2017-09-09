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
import com.dnanexus.{DXFile, DXAPI, DXJSON, DXProject, DXSearch, DXWorkflow}
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._
import spray.json._
import Utils.{UNIVERSAL_FILE_PREFIX, DXWorkflowStage}

case class InputFile(verbose: Utils.Verbose) {
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
            val dxProject = Utils.lookupProject(projName)
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
                            stageDict: Map[String, DXWorkflowStage]) : DXWorkflowStage= {
        stageDict.get(name) match {
            case None =>
                System.err.println(s"stage dictionary: ${stageDict}")
                throw new Exception(s"Stage ${name} not found for workflow ${wf.name}")
            case Some(x) => x
        }
    }

    // Translate entries in the Cromwell input file, into a valid JSON
    // dx input file
    private def dxTranslate(wf: IR.Workflow,
                            wdlInputs: JsObject,
                            stageDict: Map[String, DXWorkflowStage],
                            callDict: Map[String, String]) : JsObject= {
        val m: Map[String, JsValue] = wdlInputs.fields.map{ case (key, v) =>
            val components = key.split("\\.")
            val dxKey: Option[String] =
                components.length match {
                    case 0|1 =>
                        // Comments
                        System.err.println(s"Skipping ${key}")
                        None
                    case 2|3 if (components(0).startsWith("##")) =>
                        // Comments
                        System.err.println(s"Skipping ${key}")
                        None
                    case 2 =>
                        // workflow inputs are passed to the initial
                        // stage (common).
                        assert(components(0) == wf.name)
                        val varName = components(1)
                        val stage = lookupStage(wf.name + "_" + Utils.COMMON, wf, stageDict)
                        Some(s"${stage.getId}.${varName}")
                    case 3 =>
                        // parameters for call
                        assert(components(0) == wf.name)
                        val callName = components(1)
                        val varName = components(2)
                        stageDict.get(callName) match {
                            case Some(stage) =>
                                // call is implemented as a stage
                                Some(s"${stage.getId()}.${varName}")
                            case None =>
                                // call is an element in a scatter, or a larger applet
                                val stage = callDict.get(callName) match {
                                    case Some(x) => lookupStage(x, wf, stageDict)
                                    case None =>
                                        throw new Exception(s"Call ${callName} not found for workflow ${wf.name}")
                                }
                                Some(s"${stage.getId}.${callName}_${varName}")
                        }
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

    // Query a platform workflow, and get a mapping from stage-name to stage-id.
    private def queryWorkflowStages(dxwfl: DXWorkflow) : Map[String, DXWorkflowStage] = {
        val req: ObjectNode = DXJSON.getObjectBuilder()
            .put("fields", DXJSON.getObjectBuilder()
                     .put("stages", true)
                     .build()).build()
        val rep = DXAPI.workflowDescribe(dxwfl.getId(), req, classOf[JsonNode])
        val repJs:JsValue = Utils.jsValueOfJsonNode(rep)
        val stages = repJs.asJsObject.fields.get("stages") match {
            case None => throw new Exception("Failed to get workflow stages")
            case Some(JsArray(x)) => x
            case other => throw new Exception(s"Malformed stages fields ${other}")
        }
        stages.map{ stageMetadata =>
            val id = stageMetadata.asJsObject.fields.get("id") match {
                case None => throw new Exception("workflow stage doesn't have an ID")
                case Some(JsString(x)) => x
                case other => throw new Exception(s"Malformed id field ${other}")
            }
            val name = stageMetadata.asJsObject.fields.get("name") match {
                case None => throw new Exception("workflow stage doesn't have a name")
                case Some(JsString(x)) => x
                case other => throw new Exception(s"Malformed name field ${other}")
            }
            name -> DXWorkflowStage(id)
        }.toMap
    }

    // Build a dx input file, based on the wdl input file and the workflow
    def apply(dxwfl: DXWorkflow,
              ns: IR.Namespace,
              inputPath: Path) : Unit = {
        Utils.trace(verbose.on, s"Translating WDL input file ${inputPath}")
        val callDict = IR.callDict(ns)
        val stageDict: Map[String, DXWorkflowStage] = queryWorkflowStages(dxwfl)

        // read the input file xxxx.json
        val wdlInputs: JsObject = Utils.readFileContent(inputPath).parseJson.asJsObject

        // translate the key-value entries
        val dxInputs: JsObject = ns.workflow match {
            case None => JsObject(Map.empty[String, JsValue])
            case Some(wf) => dxTranslate(wf, wdlInputs, stageDict, callDict)
        }

        // write back out as xxxx.dx.json
        val filename = Utils.replaceFileSuffix(inputPath, ".dx.json")
        val dxInputFile = inputPath.getParent().resolve(filename)
        Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
        Utils.trace(verbose.on, s"Wrote dx JSON input file ${dxInputFile}")
    }
}
