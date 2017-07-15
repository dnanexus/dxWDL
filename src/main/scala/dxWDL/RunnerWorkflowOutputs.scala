/* Evaluate the workflow outputs, and place intermediate results
 * in a subdirectory.
 */
package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXDataObject, DXEnvironment, DXJob, DXFile, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Path
import scala.collection.JavaConverters._
import spray.json._
import spray.json.DefaultJsonProtocol
import wdl4s.{Declaration, WdlNamespaceWithWorkflow, WdlExpression, Workflow, WorkflowOutput}
import wdl4s.types._
import wdl4s.values._
import WdlVarLinks._

object RunnerWorkflowOutputs {

    def getOutputFolder(dxEnv: DXEnvironment) : String = {
        val dxEnv = com.dnanexus.DXEnvironment.create()
        val dxJob:DXJob = dxEnv.getJob
        dxJob.describe.getFolder
    }

    // Move all intermediate results to a sub-folder
    def moveIntermediateResultFiles(dxEnv: DXEnvironment,
                                    outputFields: Map[String, JsonNode]) : Unit = {
        val dxProject = dxEnv.getProjectContext()
        val outFolder = getOutputFolder(dxEnv)
        val intermFolder = outFolder + "/" + Utils.INTERMEDIATE_RESULTS_FOLDER

        // find all output files
        val allFiles: List[DXDataObject] =
            dxProject.listFolder(outFolder).getObjects.asScala.toList
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
        if (intermediateFiles.isEmpty)
            return

        // Move all non exported results to the subdir. Do this in
        // a single API call, to improve performance.
        System.err.println(s"Creating intermediate results sub-folder ${intermFolder}")
        dxProject.newFolder(intermFolder)
        dxProject.moveObjects(intermediateFiles.asJava, intermFolder)
    }

    def apply(wf: Workflow,
              jobInputPath : Path,
              jobOutputPath : Path,
              jobInfoPath: Path) : Unit = {
        // Figure out input/output types
        val (inputTypes, outputTypes) = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))

        // Parse the inputs, do not download files from the platform,
        // they will be passed as links.
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val inputs: Map[String, WdlVarLinks] = WdlVarLinks.loadJobInputsAsLinks(inputLines,
                                                                                inputTypes)
        System.err.println(s"Initial inputs=${inputs}")

        // make sure the workflow elements are all declarations
        val outputDecls: Seq[WorkflowOutput] = wf.children.map {
            case _: Declaration => None
            case wot:WorkflowOutput => Some(wot)
            case _ => throw new Exception("Workflow contains a non declaration")
        }.flatten
        val outputs : Map[String, BValue] = RunnerEval.evalDeclarations(outputDecls, inputs)

        val outputFields: Map[String, JsonNode] = outputs.map {
            case (varName, bValue) => WdlVarLinks.genFields(bValue.wvl, varName)
        }.flatten.toMap
        val m = outputFields.map{ case (varName,jsNode) =>
            (varName, Utils.jsValueOfJsonNode(jsNode))
        }.toMap

        val json = JsObject(m)
        val ast_pp = json.prettyPrint
        System.err.println(s"exported = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)

        // Reorganize directory structure
        val dxEnv = DXEnvironment.create()
        moveIntermediateResultFiles(dxEnv, outputFields)
    }
}
