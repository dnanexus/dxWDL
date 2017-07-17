/* Evaluate the workflow outputs, and place intermediate results
 * in a subdirectory.
 */
package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXContainer, DXDataObject, DXEnvironment, DXJob, DXFile, DXProject}
import com.fasterxml.jackson.databind.JsonNode
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

    // Move all intermediate results to a sub-folder
    def moveIntermediateResultFiles(dxEnv: DXEnvironment,
                                    outputFields: Map[String, JsonNode]) : Unit = {
        val dxProject = dxEnv.getProjectContext()
        val dxProjDesc = dxProject.describe
        val dxJob:DXJob = dxEnv.getJob
        val outFolder = dxJob.describe.getFolder
        val intermFolder = outFolder + "/" + Utils.INTERMEDIATE_RESULTS_FOLDER
        System.err.println(s"proj=${dxProjDesc.getName} outFolder=${outFolder}")

        // find all output files
        val folderContents:DXContainer.FolderContents = dxProject.listFolder(outFolder)
        val allFiles: List[DXDataObject] = folderContents.getObjects.asScala.toList
        System.err.println(s"output files=${allFiles}")
        if (allFiles.isEmpty)
            return

        // find all the object IDs that should be exported
        val exportFiles: Vector[DXFile] = outputFields.map{ case (key, jsn) =>
            val jsValue = Utils.jsValueOfJsonNode(jsn)
            WdlVarLinks.findDxFiles(jsValue)
        }.toVector.flatten
        val exportIds:Set[String] = exportFiles.map(_.getId).toSet

        // Figure out which of the files should be kept
        val intermediateFiles = allFiles.filter(x => !(exportIds contains x.getId))
        System.err.println(s"intermediate files=${intermediateFiles}")
        if (intermediateFiles.isEmpty)
            return

        // Move all non exported results to the subdir. Do this in
        // a single API call, to improve performance.
        val subFolders: List[String] = folderContents.getSubfolders().asScala.toList
        if (!(subFolders contains intermFolder)) {
            System.err.println(s"Creating intermediate results sub-folder ${intermFolder}")
            dxProject.newFolder(intermFolder)
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
