/** Evaluate a list of expressions. For example, a workflow could start like:

   workflow PairedEndSingleSampleWorkflow {
     String tag = "v1.56"
     String gvcf_suffix
     File ref_fasta
     String cmdline="perf mem -K 100000000 -p -v 3 $tag"

     ...
   }

   This mini-applet will perform the toplevel computations. In the above example,
   it will calculate the [cmdline] variable, and provide four outputs:
       tag:         String
       gvcf_suffix: String
       ref_fasta:   File
       cmdline      String

  */
package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXEnvironment, DXFile, DXJob, InputParameter, OutputParameter}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Path
import scala.collection.JavaConverters._
import spray.json._
import spray.json.DefaultJsonProtocol
import wdl4s.{Declaration, WdlNamespaceWithWorkflow, WdlExpression, Workflow, WorkflowOutput}
import wdl4s.types._
import wdl4s.values._
import WdlVarLinks._

object RunnerEval {
    def evalDeclarations(declarations: Seq[Declaration],
                         inputs : Map[String, WdlVarLinks]) : Map[String, BValue] = {
        // Environment that includes a cache for values that have
        // already been evaluated.  It is more efficient to make the
        // conversion once, however, that is not the main point
        // here. There are types that require special care, for
        // example files. We need to make sure we download files
        // exactly once, and later, we want to be able to delete them.
        var env: Map[String, (WdlVarLinks, Option[WdlValue])] =
            inputs.map{ case (key, wvl) => key -> (wvl, None) }.toMap

        def evalAndCache(key: String, wvl: WdlVarLinks) : WdlValue = {
            val v: WdlValue = WdlVarLinks.eval(wvl, false)
            env = env + (key -> (wvl, Some(v)))
            v
        }

        def lookup(varName : String) : WdlValue =
            env.get(varName) match {
                case Some((wvl, None)) =>
                    // Make a value from a dx-links structure. This also causes any file
                    // to be downloaded. Keep the result cached.
                    evalAndCache(varName, wvl)
                case Some((_, Some(v))) =>
                    // We have already evalulated this structure
                    v
                case None =>
                    throw new AppInternalException(s"Accessing unbound variable ${varName}")
            }

        def evalDecl(decl : Declaration) : Option[(WdlVarLinks, WdlValue)] = {
            (decl.wdlType, decl.expression) match {
                // optional input
                case (WdlOptionalType(_), None) =>
                    inputs.get(decl.unqualifiedName) match {
                        case None => None
                        case Some(wvl) =>
                            val v: WdlValue = evalAndCache(decl.unqualifiedName, wvl)
                            Some((wvl, v))
                    }

                // compulsory input
                case (_, None) =>
                    inputs.get(decl.unqualifiedName) match {
                        case None =>
                            throw new AppInternalException(s"Accessing unbound variable ${decl.unqualifiedName}")
                        case Some(wvl) =>
                            val v: WdlValue = evalAndCache(decl.unqualifiedName, wvl)
                            Some((wvl, v))
                    }

                // declaration to evaluate, not an input
                case (t, Some(expr)) =>
                    val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
                    val wvl = WdlVarLinks.apply(t, v)
                    env = env + (decl.unqualifiedName -> (wvl, Some(v)))
                    Some((wvl, v))
            }
        }

        // Process all the declarations. Take care to add new bindings
        // to the environment, so variables like [cmdline] will be able to
        // access previous results.
        declarations.map{ decl =>
            evalDecl(decl) match {
                case Some((wvl, wdlValue)) => Some(decl.unqualifiedName -> BValue(wvl,wdlValue))
                case None =>
                    // optional input that was not provided
                    None
            }
        }.flatten.toMap
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
            case _ => throw new Exception("Eval task contains a non declaration")
        }.flatten
        val outputs : Map[String, BValue] = evalDeclarations(decls, inputs)

        // Keep only exported variables
        val exported = outputs.filter{ case (varName, _) => outputTypes contains varName }
        val outputFields: Map[String, JsonNode] = exported.map {
            case (varName, bValue) => WdlVarLinks.genFields(bValue.wvl, varName)
        }.flatten.toMap
        val m = outputFields.map{ case (varName,jsNode) =>
            (varName, Utils.jsValueOfJsonNode(jsNode))
        }.toMap

        val json = JsObject(m)
        val ast_pp = json.prettyPrint
        System.err.println(s"exported = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
