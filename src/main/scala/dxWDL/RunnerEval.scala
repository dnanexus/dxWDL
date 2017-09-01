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
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Path
import spray.json._
import wdl4s.wdl.{Declaration, DeclarationInterface, WdlTask, WdlWorkflow, WorkflowOutput}
import wdl4s.wdl.types._
import wdl4s.wdl.values._

object RunnerEval {
    def evalDeclarations(declarations: Seq[DeclarationInterface],
                         inputs : Map[String, WdlVarLinks],
                         force: Boolean,
                         taskOpt : Option[(WdlTask, CompilerErrorFormatter)]) : Map[String, BValue] = {
        // Environment that includes a cache for values that have
        // already been evaluated.  It is more efficient to make the
        // conversion once, however, that is not the main point
        // here. There are types that require special care, for
        // example files. We need to make sure we download files
        // exactly once, and later, we want to be able to delete them.
        var env: Map[String, (WdlVarLinks, Option[WdlValue])] =
            inputs.map{ case (key, wvl) => key -> (wvl, None) }.toMap

        def evalAndCache(key: String, wvl: WdlVarLinks) : WdlValue = {
            val v: WdlValue =
                if (wvl.attrs.stream) {
                    WdlVarLinks.eval(wvl, false)
                } else {
                    WdlVarLinks.eval(wvl, force)
                }
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

        def evalDecl(decl : DeclarationInterface) : Option[(WdlVarLinks, WdlValue)] = {
            val attrs = taskOpt match {
                case None => DeclAttrs.empty
                case Some((task, cef)) => DeclAttrs.get(task, decl.unqualifiedName, cef)
            }
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
                            throw new AppInternalException(
                                s"Accessing unbound variable ${decl.unqualifiedName}")
                        case Some(wvl) =>
                            val v: WdlValue = evalAndCache(decl.unqualifiedName, wvl)
                            Some((wvl, v))
                    }

                // declaration to evaluate, not an input
                case (WdlOptionalType(t), Some(expr)) =>
                    try {
                        val vRaw : WdlValue = expr.evaluate(lookup, DxFunctions).get
                        val v: WdlValue = Utils.cast(t, vRaw, decl.unqualifiedName)
                        val wvl = WdlVarLinks.apply(t, attrs, v)
                        env = env + (decl.unqualifiedName -> (wvl, Some(v)))
                        Some((wvl, v))
                    } catch {
                        // trying to access an unbound variable.
                        // Since the result is optional.
                        case e: AppInternalException => None
                    }

                case (t, Some(expr)) =>
                    val vRaw : WdlValue = expr.evaluate(lookup, DxFunctions).get
                    val v: WdlValue = Utils.cast(t, vRaw, decl.unqualifiedName)
                    val wvl = WdlVarLinks.apply(t, attrs, v)
                    env = env + (decl.unqualifiedName -> (wvl, Some(v)))
                    Some((wvl, v))
            }
        }

        // Process all the declarations. Take care to add new bindings
        // to the environment, so variables like [cmdline] will be able to
        // access previous results.
        declarations.map{ decl =>
            evalDecl(decl) match {
                case Some((wvl, wdlValue)) =>
                    Some(decl.unqualifiedName -> BValue(wvl, wdlValue))
                case None =>
                    // optional input that was not provided
                    None
            }
        }.flatten.toMap
    }

    def apply(wf: WdlWorkflow,
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
            case _ => throw new Exception("Eval workflow contains a non declaration")
        }.flatten
        val outputs : Map[String, BValue] = evalDeclarations(decls, inputs, false, None)

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
