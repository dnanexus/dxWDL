package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXEnvironment, DXFile, DXJob, InputParameter, OutputParameter}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Path, Paths, Files}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import wdl4s.expression.{NoFunctions, WdlStandardLibraryFunctionsType}
import wdl4s.types._
import wdl4s.values._
import wdl4s.{Call, Declaration, WdlNamespaceWithWorkflow, WdlExpression, Workflow}
import wdl4s.WdlExpression.AstForExpressions
import WdlVarLinks._

// For example, a workflow could start like:
//
// workflow PairedEndSingleSampleWorkflow {
//   String tag = "v1.56"
//   String gvcf_suffix
//   File ref_fasta
//   String cmdline="perf mem -K 100000000 -p -v 3 $tag"
//
//   ...
// }
//
// This mini-applet will perform the toplevel computations. In the above example,
// it will calculate the [cmdline] variable, and provide four outputs:
//     tag:         String
//     gvcf_suffix: String
//     ref_fasta:   File
//     cmdline      String
//
object WorkflowCommenceRunner {
    lazy val dxEnv = DXEnvironment.create()

    def evalDeclarations(declarations: Seq[Declaration],
                         inputs : Map[String, WdlVarLinks]) : Seq[(String, WdlVarLinks)] = {
        var env = inputs
        def lookup(varName : String) : WdlValue =
            env.get(varName) match {
                case Some(x) =>
                    // Make a value from a dx-links structure. This also causes any file
                    // to be downloaded.
                    WdlVarLinks.wdlValueOfInputField(x)
                case None => throw new AppInternalException(s"No value found for variable ${varName}")
            }

        def evalDecl(decl : Declaration) : Option[WdlVarLinks] = {
            (decl.wdlType, decl.expression) match {
                // optional input
                case (WdlOptionalType(_), None) =>
                    inputs.get(decl.unqualifiedName) match {
                        case None => None
                        case Some(x) => Some(x)
                    }

                // compulsory input
                case (_, None) =>
                    Some(inputs(decl.unqualifiedName))

                // declaration to evaluate, not an input
                case (t, Some(expr)) =>
                    val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
                    val wvl = WdlVarLinks.outputFieldOfWdlValue(decl.unqualifiedName, t, v)
                    Some(wvl)
            }
        }

        // Process all the declarations. Take care to add new bindings
        // to the environment, so variables like [cmdline] will be able to
        // access previous results.
        declarations.foreach { decl =>
            val v : Option[WdlVarLinks] = evalDecl(decl)
            v match {
                case Some(v) =>
                    env = env + (decl.unqualifiedName -> v)
                case None =>
                    // optional input that was not provided
                    ()
            }
        }
        env.toList
    }

    def apply(wf: Workflow,
              jobInputPath : Path,
              jobOutputPath : Path,
              jobInfoPath: Path) : Unit = {
        val dxapp : DXApplet = dxEnv.getJob().describe().getApplet()
        val desc : DXApplet.Describe = dxapp.describe()

        // Extract types for closure inputs
        val closureTypes = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        System.err.println(s"WdlType mapping =${closureTypes}")

        // Parse the inputs, do not download files from the platform.
        // They will be passed as links to the tasks.
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val inputs: Map[String, WdlVarLinks] = WdlVarLinks.loadJobInputsAsLinks(inputLines,
                                                                                 closureTypes)
        System.err.println(s"Initial inputs=${inputs}")

        // Figure out if there are any additional declarations that have been lifted
        // TODO: Can we do this just once at compile time?
        val (topDecls, wfBody) = Utils.splitBlockDeclarations(wf.children.toList)
        val (liftedDecls, _) = Utils.liftDeclarations(topDecls, wfBody)
        val outputs : Seq[(String, WdlVarLinks)] = evalDeclarations(topDecls ++ liftedDecls, inputs)
        val outputFields: Map[String, JsonNode] = outputs.map {
            case (varName, wvl) => WdlVarLinks.genFields(wvl, varName)
        }.flatten.toMap
        val m = outputFields.map{ case (varName,jsNode) =>
            (varName, Utils.jsValueOfJsonNode(jsNode))
        } .toMap

        val json = JsObject(m)
        val ast_pp = json.prettyPrint
        System.err.println(s"outputs = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
