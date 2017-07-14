/* Evaluate the workflow outputs, and place intermediate results
 * in a subdirectory.
 */
package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXDataObject, DXEnvironment, DXFile, DXProject}
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

    // An ugly hack to get the dx working directory.
    //
    // Since we don't have a dxjava binding, we spawn a shell, and run
    // `dx pwd`. I hope to improve on this down the road.
    def getCurrentWorkingDir() : String = {
        val (outstr, _) =
            try {
                Utils.execCommand("dx pwd")
            } catch {
                case e: Throwable =>
                    throw new AppInternalException("Could not get the dx working directory")
            }
        // The result is PROJ_NAME:WDIR
        val words = outstr.split(":")
        assert(words.length == 2)
        words(1)
    }

    // Move all intermediate results to a sub-folder
    def moveIntermediateResultFiles(dxProject: DXProject,
                                    outputFields: Map[String, JsonNode]) : Unit = {
        val currentDir = getCurrentWorkingDir()
        val iDir = currentDir + "/" + Utils.INTERMEDIATE_RESULTS_FOLDER
        System.err.println(s"Create intermediate results sub-folder ${iDir}")
        dxProject.newFolder(iDir)

        // find all output files
        val allFiles: List[DXDataObject] =
            dxProject.listFolder(currentDir).getObjects.asScala.toList

        // find all the object IDs that should be exported
        val exportFiles: Vector[DXFile] = outputFields.map{ case (key, jsn) =>
            val jsValue = Utils.jsValueOfJsonNode(jsn)
            WdlVarLinks.findDxFiles(jsValue)
        }.toVector.flatten
        val exportIds:Set[String] = exportFiles.map(_.getId).toSet

        // Figure out which of the files should be kept
        val intermediateFiles = allFiles.filter(x => !(exportIds contains x.getId))

        // Move all non exported results to the subdir. Do this in
        // a single API call, to improve performance.
        dxProject.moveObjects(intermediateFiles.asJava, iDir)
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
        val decls: Seq[Declaration] = wf.children.map {
            case decl: Declaration => Some(decl)
            case _:WorkflowOutput => None
            case _ => throw new Exception("Workflow contains a non declaration")
        }.flatten
        val outputs : Map[String, BValue] = RunnerEval.evalDeclarations(decls, inputs)

        // Keep only exported variables
        val exported = outputs.filter{ case (varName, _) => outputTypes contains varName }
        val outputFields: Map[String, JsonNode] = exported.map {
            case (varName, bValue) => WdlVarLinks.genFields(bValue.wvl, varName)
        }.flatten.toMap
        val m = outputFields.map{ case (varName,jsNode) =>
            (varName, Utils.jsValueOfJsonNode(jsNode))
        }.toMap

        val dxEnv = DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()
        moveIntermediateResultFiles(dxProject, outputFields)

        val json = JsObject(m)
        val ast_pp = json.prettyPrint
        System.err.println(s"exported = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
