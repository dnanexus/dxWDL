/* Evaluate the workflow outputs, and place intermediate results
 * in a subdirectory.
 */
package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXAnalysis, DXAPI, DXContainer, DXDataObject,
    DXEnvironment, DXJob, DXJSON, DXFile, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.file.Path
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import scala.collection.JavaConverters._
import spray.json._
import spray.json.DefaultJsonProtocol
import wdl4s.{Declaration, WdlNamespaceWithWorkflow, WdlExpression, Workflow, WorkflowOutput}
import wdl4s.types._
import wdl4s.values._
import WdlVarLinks._

object RunnerWorkflowOutputs {

    // find all output files from the analysis
    def analysisFileOutputs(dxAnalysis: DXAnalysis) : Vector[DXFile]= {
        val req: ObjectNode = DXJSON.getObjectBuilder()
            .put("fields",
                 DXJSON.getObjectBuilder().put("output", true)
                     .build()).build()
        val rep = DXAPI.analysisDescribe(dxAnalysis.getId(), req, classOf[JsonNode])
        val repJs:JsValue = Utils.jsValueOfJsonNode(rep)
        val outputs = repJs.asJsObject.fields.get("output") match {
            case None => throw new Exception("Failed to get analysis outputs")
            case Some(x) => x
        }
        val fileOutputs : Vector[DXFile] = WdlVarLinks.findDxFiles(outputs)
        val fileNames = fileOutputs.map(_.describe().getName())
        System.err.println(s"analysis output files=${fileNames}")
        fileOutputs
    }


    // Move all intermediate results to a sub-folder
    def moveIntermediateResultFiles(dxEnv: DXEnvironment,
                                    outputFields: Map[String, JsonNode]) : Unit = {
        val dxProject = dxEnv.getProjectContext()
        val dxProjDesc = dxProject.describe
        val dxAnalysis = dxEnv.getJob.describe.getAnalysis
        val outFolder = dxAnalysis.describe.getFolder
        val intermFolder = outFolder + "/" + Utils.INTERMEDIATE_RESULTS_FOLDER
        System.err.println(s"proj=${dxProjDesc.getName} outFolder=${outFolder}")

        // find all output files from the analysis
        val analysisFiles: Vector[DXFile] = analysisFileOutputs(dxAnalysis)
        if (analysisFiles.isEmpty)
            return

        // find all the object IDs that should be exported
        val exportFiles: Vector[DXFile] = outputFields.map{ case (key, jsn) =>
            val jsValue = Utils.jsValueOfJsonNode(jsn)
            WdlVarLinks.findDxFiles(jsValue)
        }.toVector.flatten
        val exportIds:Set[String] = exportFiles.map(_.getId).toSet
        val exportNames:Seq[String] = exportFiles.map(_.describe().getName())
        System.err.println(s"exportFiles=${exportNames}")

        // Figure out which of the files should be kept
        val intermediateFiles = analysisFiles.filter(x => !(exportIds contains x.getId))
        val iNames:Seq[String] = intermediateFiles.map(_.describe().getName())
        System.err.println(s"intermediate files=${iNames}")
        if (intermediateFiles.isEmpty)
            return

        // Move all non exported results to the subdir. Do this in
        // a single API call, to improve performance.
        val folderContents:DXContainer.FolderContents = dxProject.listFolder(outFolder)
        val subFolders: List[String] = folderContents.getSubfolders().asScala.toList
        if (!(subFolders contains intermFolder)) {
            System.err.println(s"Creating intermediate results sub-folder ${intermFolder}")
            dxProject.newFolder(intermFolder)
        } else {
            System.err.println(s"Intermediate results sub-folder ${intermFolder} already exists")
        }
        dxProject.moveObjects(intermediateFiles.asJava, intermFolder)
    }

    // Convert the environment to yaml, and then pretty
    // print it.
    def prettyPrint(inputs: Map[String, WdlVarLinks]) : String = {
        val m: Map[YamlValue, YamlValue] = inputs.map{
            case (key, wvl) => YamlString(key) -> yaml(wvl)
        }.toMap
        YamlObject(m).print()
    }

    def apply(wf: Workflow,
              jobInputPath : Path,
              jobOutputPath : Path,
              jobInfoPath: Path,
              reorgFiles: Boolean) : Unit = {
        // Figure out input/output types
        val (inputTypes, outputTypes) = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))

        // Parse the inputs, do not download files from the platform,
        // they will be passed as links.
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val inputs: Map[String, WdlVarLinks] = WdlVarLinks.loadJobInputsAsLinks(inputLines,
                                                                                inputTypes)
        System.err.println(s"Initial inputs=\n${prettyPrint(inputs)}")

        // Make sure the workflow elements are all declarations
        val outputDecls: Seq[WorkflowOutput] = wf.children.map {
            case _: Declaration => None
            case wot:WorkflowOutput => Some(wot)
            case _ => throw new Exception("Workflow contains a non declaration")
        }.flatten
        val outputs : Map[String, BValue] = RunnerEval.evalDeclarations(outputDecls, inputs)

        val outputFields: Map[String, JsonNode] = outputs.map {
            case (varName, bValue) =>
                val varNameOrg = varName.stripPrefix("out_")
                WdlVarLinks.genFields(bValue.wvl, varNameOrg)
        }.flatten.toMap
        val m = outputFields.map{ case (varName,jsNode) =>
            (varName, Utils.jsValueOfJsonNode(jsNode))
        }.toMap

        val json = JsObject(m)
        val ast_pp = json.prettyPrint
        System.err.println(s"exported = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)

        if (reorgFiles) {
            // Reorganize directory structure
            val dxEnv = DXEnvironment.create()
            moveIntermediateResultFiles(dxEnv, outputFields)
        }
    }
}
