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
import java.nio.file.Path
import spray.json._
import Utils.appletLog
import wdl4s.wdl.{Declaration, DeclarationInterface, WdlTask, WdlExpression, WdlWorkflow, WorkflowOutput}
import wdl4s.wdl.types._
import wdl4s.wdl.values._

object RunnerEval {
    def evalDeclarations(declarations: Seq[DeclarationInterface],
                         inputs : Map[String, WdlVarLinks],
                         force: Boolean,
                         taskOpt : Option[(WdlTask, CompilerErrorFormatter)],
                         ioDir: IODirection.Value) : Map[String, BValue] = {
        // Environment that includes a cache for values that have
        // already been evaluated.  It is more efficient to make the
        // conversion once, however, that is not the main point
        // here. There are types that require special care, for
        // example files. We need to make sure we download files
        // exactly once, and later, we want to be able to delete them.
        var env: Map[String, (WdlVarLinks, Option[WdlValue])] =
            inputs.map{ case (key, wvl) => key -> (wvl, None) }.toMap

        def wvlEvalCache(key: String, wvl: WdlVarLinks) : WdlValue = {
            env.get(key) match {
                case Some((_,Some(v))) => v
                case _ =>
                    val v: WdlValue =
                        if (wvl.attrs.stream) {
                            WdlVarLinks.eval(wvl, false, ioDir)
                        } else {
                            WdlVarLinks.eval(wvl, force, ioDir)
                        }
                    env = env + (key -> (wvl, Some(v)))
                    v
            }
        }

        def lookup(varName : String) : WdlValue =
            env.get(varName) match {
                case Some((wvl, None)) =>
                    // Make a value from a dx-links structure. This also causes any file
                    // to be downloaded. Keep the result cached.
                    wvlEvalCache(varName, wvl)
                case Some((_, Some(v))) =>
                    // We have already evaluated this structure
                    v
                case None =>
                    throw new UnboundVariableException(s"${varName}")
            }

        def evalDeclBase(decl:DeclarationInterface,
                         expr:WdlExpression,
                         attrs:DeclAttrs) : (WdlVarLinks, WdlValue) = {
            appletLog(s"evaluating ${decl}")
            val vRaw : WdlValue = expr.evaluate(lookup, DxFunctions).get
            val w: WdlValue = Utils.cast(decl.wdlType, vRaw, decl.unqualifiedName)
            val wvl = WdlVarLinks.importFromWDL(decl.wdlType, attrs, w, ioDir)
            env = env + (decl.unqualifiedName -> (wvl, Some(w)))
            (wvl, w)
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
                            val v: WdlValue = wvlEvalCache(decl.unqualifiedName, wvl)
                            Some((wvl, v))
                    }

                // compulsory input
                case (_, None) =>
                    inputs.get(decl.unqualifiedName) match {
                        case None =>
                            throw new UnboundVariableException(s"${decl.unqualifiedName}")
                        case Some(wvl) =>
                            val v: WdlValue = wvlEvalCache(decl.unqualifiedName, wvl)
                            Some((wvl, v))
                    }

                // declaration to evaluate, not an input
                case (WdlOptionalType(t), Some(expr)) =>
                    try {
                        // An optional type
                        inputs.get(decl.unqualifiedName) match {
                            case None =>
                                Some(evalDeclBase(decl, expr, attrs))
                            case Some(wvl) =>
                                // An overriding value was provided, use it instead
                                // of evaluating the right hand expression
                                val v:WdlValue = wvlEvalCache(decl.unqualifiedName, wvl)
                                Some(wvl, v)
                        }
                    } catch {
                        // Trying to access an unbound variable. Since
                        // the result is optional, we can just let it go.
                        case e: UnboundVariableException => None
                    }

                case (t, Some(expr)) =>
                    Some(evalDeclBase(decl, expr, attrs))
            }
        }

        // Process all the declarations. Take care to add new bindings
        // to the environment, so variables like [cmdline] will be able to
        // access previous results.
        val outputs = declarations.map{ decl =>
            evalDecl(decl) match {
                case Some((wvl, wdlValue)) =>
                    Some(decl.unqualifiedName -> BValue(wvl, wdlValue))
                case None =>
                    // optional input that was not provided
                    None
            }
        }.flatten.toMap
        appletLog(s"Eval env=${env}")
        outputs
    }

    def apply(wf: WdlWorkflow,
              jobInputPath : Path,
              jobOutputPath : Path,
              jobInfoPath: Path) : Unit = {
        // Figure out input/output types
        //val (inputTypes, outputTypes) = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        val (inputSpec, outputSpec) = Utils.loadExecInfo

        // Parse the inputs, do not download files from the platform,
        // they will be passed as links.
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val inputs: Map[String, WdlVarLinks] = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputSpec)
        appletLog(s"Initial inputs=${inputs}")

        // make sure the workflow elements are all declarations
        val decls: Seq[Declaration] = wf.children.map {
            case decl: Declaration => Some(decl)
            case _:WorkflowOutput => None
            case _ => throw new Exception("Eval workflow contains a non declaration")
        }.flatten
        val outputs : Map[String, BValue] = evalDeclarations(decls, inputs, false, None,
                                                             IODirection.Download)

        // Keep only exported variables
        val exported = outputs.filter{ case (varName, _) => outputSpec contains varName }
        val outputFields: Map[String, JsValue] = exported.map {
            case (varName, bValue) => WdlVarLinks.genFields(bValue.wvl, varName)
        }.flatten.toMap

        val json = JsObject(outputFields)
        val ast_pp = json.prettyPrint
        appletLog(s"exported = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
