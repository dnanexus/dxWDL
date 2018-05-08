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
package dxWDL.runner

// DX bindings
import spray.json._
import dxWDL._
import wdl.draft2.model.{Declaration, DeclarationInterface, WdlExpression, WdlWorkflow, WorkflowOutput}
import wom.types._
import wom.values._

object Eval {
    val verbose = true

    def evalDeclarations(declarations: Seq[DeclarationInterface],
                         envInputs : Map[String, WomValue]) : Map[DeclarationInterface, WomValue] = {
        // Environment that includes a cache for values that have
        // already been evaluated.  It is more efficient to make the
        // conversion once, however, that is not the main point
        // here. There are types that require special care, for
        // example files. We need to make sure we download files
        // exactly once, and later, we want to be able to delete them.
        var env: Map[String, WomValue] = envInputs

        def lookup(varName : String) : WomValue = env.get(varName) match {
            case Some(v) => v
            case None =>
                // A value for this variable has not been passed. Check if it
                // is optional.
                val varDefinition = declarations.find(_.unqualifiedName == varName) match {
                    case Some(x) => x
                    case None => throw new Exception(
                        s"Cannot find declaration for variable ${varName}")
                }
                varDefinition.womType match {
                    case WomOptionalType(t) => WomOptionalValue(t, None)
                    case _ =>  throw new UnboundVariableException(s"${varName}")
                }
        }

        // coerce a WDL value to the required type (if needed)
        def cast(wdlType: WomType, v: WomValue, varName: String) : WomValue = {
            val retVal =
                if (v.womType != wdlType) {
                    // we need to convert types
                    Utils.appletLog(verbose, s"casting ${v.womType} to ${wdlType}")
                    wdlType.coerceRawValue(v).get
                } else {
                    // no need to change types
                    v
                }
            retVal
        }

        def evalAndCache(decl:DeclarationInterface,
                         expr:WdlExpression) : WomValue = {
            val vRaw : WomValue = expr.evaluate(lookup, DxFunctions).get
            Utils.appletLog(verbose, s"evaluating ${decl} -> ${vRaw}")
            val w: WomValue = cast(decl.womType, vRaw, decl.unqualifiedName)
            env = env + (decl.unqualifiedName -> w)
            w
        }

        def evalDecl(decl : DeclarationInterface) : WomValue = {
            (decl.womType, decl.expression) match {
                // optional input
                case (WomOptionalType(t), None) =>
                    envInputs.get(decl.unqualifiedName) match {
                        case None => WomOptionalValue(t, None)
                        case Some(wdlValue) => wdlValue
                    }

                // compulsory input
                case (_, None) =>
                    envInputs.get(decl.unqualifiedName) match {
                        case None =>
                            throw new UnboundVariableException(s"${decl.unqualifiedName}")
                        case Some(wdlValue) => wdlValue
                    }

                case (_, Some(expr)) if (envInputs contains decl.unqualifiedName) =>
                    // An overriding value was provided, use it instead
                    // of evaluating the right hand expression
                    envInputs(decl.unqualifiedName)

                case (_, Some(expr)) =>
                    // declaration to evaluate, not an input
                    evalAndCache(decl, expr)
            }
        }

        // Process all the declarations. Take care to add new bindings
        // to the environment, so variables like [cmdline] will be able to
        // access previous results.
        val results = declarations.map{ decl =>
            decl -> evalDecl(decl)
        }.toMap
        Utils.appletLog(verbose, s"Eval env=${env}")
        results
    }


    def apply(wf: WdlWorkflow,
              inputSpec: Map[String, DXIOParam],
              outputSpec: Map[String, DXIOParam],
              inputs: Map[String, WdlVarLinks]) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"Initial inputs=${inputs}")

        // make sure the workflow elements are all declarations
        val decls: Seq[Declaration] = wf.children.map {
            case decl: Declaration => Some(decl)
            case _:WorkflowOutput => None
            case _ => throw new Exception("Eval workflow contains a non declaration")
        }.flatten

        val envInput: Map[String, WomValue] = inputs.map{ case (key, wvl) =>
            val w:WomValue = WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)
            key -> w
        }
        val outputs : Map[DeclarationInterface, WomValue] = evalDeclarations(decls, envInput)

        // Keep only exported variables
        val exported = outputs.filter{ case (decl, _) => outputSpec contains decl.unqualifiedName }
        exported.map {
            case (decl, wdlValue) =>
                val wvl = WdlVarLinks.importFromWDL(decl.womType,
                                                    DeclAttrs.empty, wdlValue, IODirection.Zero)
                WdlVarLinks.genFields(wvl, decl.unqualifiedName)
        }.flatten.toMap
    }
}
