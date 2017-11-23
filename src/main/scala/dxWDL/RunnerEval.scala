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
import com.dnanexus.IOClass
import spray.json._
import Utils.appletLog
import wdl4s.wdl.{Declaration, DeclarationInterface, WdlExpression, WdlWorkflow, WorkflowOutput}
import wdl4s.wdl.types._
import wdl4s.wdl.values._

object RunnerEval {
    def evalDeclarations(declarations: Seq[DeclarationInterface],
                         envInputs : Map[String, WdlValue]) : Map[DeclarationInterface, WdlValue] = {
        // Environment that includes a cache for values that have
        // already been evaluated.  It is more efficient to make the
        // conversion once, however, that is not the main point
        // here. There are types that require special care, for
        // example files. We need to make sure we download files
        // exactly once, and later, we want to be able to delete them.
        var env: Map[String, WdlValue] = envInputs

        def lookup(varName : String) : WdlValue =
            env.get(varName) match {
                case Some(v) => v
                case None => throw new UnboundVariableException(s"${varName}")
            }

        def evalAndCache(decl:DeclarationInterface,
                         expr:WdlExpression) : WdlValue = {
            appletLog(s"evaluating ${decl}")
            val vRaw : WdlValue = expr.evaluate(lookup, DxFunctions).get
            val w: WdlValue = Utils.cast(decl.wdlType, vRaw, decl.unqualifiedName)
            env = env + (decl.unqualifiedName -> w)
            w
        }

        def evalDecl(decl : DeclarationInterface) : WdlValue = {
            (decl.wdlType, decl.expression) match {
                // optional input
                case (WdlOptionalType(t), None) =>
                    envInputs.get(decl.unqualifiedName) match {
                        case None => WdlOptionalValue(t, None)
                        case Some(wdlValue) => wdlValue
                    }

                // compulsory input
                case (_, None) =>
                    envInputs.get(decl.unqualifiedName) match {
                        case None =>
                            throw new UnboundVariableException(s"${decl.unqualifiedName}")
                        case Some(wdlValue) => wdlValue
                    }

                // declaration to evaluate, not an input
                case (WdlOptionalType(t), Some(expr)) =>
                    try {
                        evalAndCache(decl, expr)
                    } catch {
                        // Trying to access an unbound variable. Since
                        // the result is optional, we can just let it go.
                        case e: UnboundVariableException =>
                            WdlOptionalValue(t, None)
                    }

                case (t, Some(expr)) =>
                    evalAndCache(decl, expr)
            }
        }

        // Process all the declarations. Take care to add new bindings
        // to the environment, so variables like [cmdline] will be able to
        // access previous results.
        val results = declarations.map{ decl =>
            decl -> evalDecl(decl)
        }.toMap
        appletLog(s"Eval env=${env}")
        results
    }


    def apply(wf: WdlWorkflow,
              inputSpec: Map[String, IOClass],
              outputSpec: Map[String, IOClass],
              inputs: Map[String, WdlVarLinks]) : Map[String, JsValue] = {
        appletLog(s"Initial inputs=${inputs}")

        // make sure the workflow elements are all declarations
        val decls: Seq[Declaration] = wf.children.map {
            case decl: Declaration => Some(decl)
            case _:WorkflowOutput => None
            case _ => throw new Exception("Eval workflow contains a non declaration")
        }.flatten

        val envInput: Map[String, WdlValue] = inputs.map{ case (key, wvl) =>
            val w:WdlValue = WdlVarLinks.eval(wvl, false, IODirection.Zero)
            key -> w
        }
        val outputs : Map[DeclarationInterface, WdlValue] = evalDeclarations(decls, envInput)

        // Keep only exported variables
        val exported = outputs.filter{ case (decl, _) => outputSpec contains decl.unqualifiedName }
        exported.map {
            case (decl, wdlValue) =>
                val wvl = WdlVarLinks.importFromWDL(decl.wdlType,
                                                    DeclAttrs.empty, wdlValue, IODirection.Zero)
                WdlVarLinks.genFields(wvl, decl.unqualifiedName)
        }.flatten.toMap
    }
}
