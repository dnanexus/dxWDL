/*

Execute a limitied WDL workflow on the platform. The workflow can
contain declarations, nested if/scatter blocks, and one call
location. A simple example is `wf_scat`.

workflow wf_scat {
    String pattern
    Array[Int] numbers = [1, 3, 7, 15]
    Array[Int] index = range(length(numbers))

    scatter (i in index) {
        call inc {input: i=numbers[i]}
    }

  output {
    Array[Int] inc1_result = inc1.result
  }
}

A nested example:

workflow wf_cond {
   Int x

   if (x > 10) {
      scatter (b in [1, 3, 5, 7])
          call add {input: a=x, b=b}
   }
   output {
      Array[Int]? add_result = add.result
   }
}
*/

package dxWDL.runner

// DX bindings
import com.dnanexus._
import dxWDL._
import java.nio.file.{Path, Paths, Files}
import scala.collection.mutable.HashMap
import spray.json._
import wdl._
import wdl.expression._
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wom.values._
import wom.types._
import wdl.WdlExpression.AstForExpressions

case class MiniWorkflow(exportVars: Set[String],
                        cef: CompilerErrorFormatter,
                        orgInputs: JsValue,
                        collectSubjob: Boolean,
                        verbose: Boolean) {
    // A runtime representation of a WDL variable
    case class RVar(name: String,
                    womType: WomType,
                    value: WomValue)

    // An environment where WDL expressions can be evaluated
    type Env = Map[String, RVar]

    // check if a variable should be exported from this applet
    private def isExported(varName: String) : Boolean = {
        exportVars contains (Utils.transformVarName(varName))
    }

    // find the sequence of applets inside the scatter block. In the example,
    // these are: [inc1, inc2]
    private def findApplets(scope: Scope,
                            linkInfo: Map[String, ExecLinkInfo])
            : Seq[(WdlCall, ExecLinkInfo)] = {
        // Match each call with its dx:applet
        scope.children.map {
            case call: WdlTaskCall =>
                val dxAppletName = call.task.name
                linkInfo.get(dxAppletName) match {
                    case Some(x) => Some((call, x))
                    case None =>
                        throw new AppInternalException(
                            s"Could not find linking information for ${dxAppletName}")
                }
            case _ => None
        }.flatten
    }

    private def wdlValueFromWVL(wvl: WdlVarLinks) : WomValue =
        WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)

    private def wdlValueToWVL(t:WomType, wdlValue:WomValue) : WdlVarLinks =
        WdlVarLinks.importFromWDL(t, DeclAttrs.empty, wdlValue, IODirection.Zero)


    /**
      In the workflow below, we want to correctly pass the [k] value
      to each [inc] Task invocation.

    scatter (k in integers) {
        call inc as inc {input: i=k}
    }
      */
    private def buildAppletInputs(call: WdlCall,
                                  apLinkInfo: ExecLinkInfo,
                                  env : Env) : JsValue = {
        val callName = call.unqualifiedName
        val appInputs: Map[String, Option[WdlVarLinks]] = apLinkInfo.inputs.map{
            case (varName, wdlType) =>
                // The rhs is [k], the varName is [i]
                val rhs: Option[(String,WdlExpression)] =
                    call.inputMappings.find{ case(key, expr) => key == varName }
                val wvl:Option[WdlVarLinks] = rhs match {
                    case None =>
                        // A value for [i] is not provided in the call.
                        // Check if it was passed in the environment
                        env.get(s"${callName}_${varName}") match {
                            case None =>
                                if (Utils.isOptional(wdlType)) {
                                    None
                                } else {
                                    val provided = call.inputMappings.map{
                                        case (key, expr) => key}.toVector
                                    throw new AppInternalException(
                                        s"""|Could not find binding for required variable ${varName}.
                                            |The call bindings are ${provided}""".stripMargin.trim)
                                }
                            case Some(ElemTop(wvl)) => Some(wvl)
                            case Some(ElemCall(callOutputs)) =>
                                throw new Exception(cef.undefinedMemberAccess(call.ast))
                    }
                case Some((_, expr)) =>
                    Some(wvlEvalExpression(expr, env))
                }
                varName -> wvl
        }
        val m = appInputs.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, Some(wvl))) =>
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
            case (accu, (varName, None)) =>
                accu
        }
        val mNonNull = m.filter{ case (key, value) => value != null && value != JsNull}
        JsObject(mNonNull)
    }

    // Load from disk a mapping of applet name to id. We
    // need this in order to call the right version of other
    // applets.
    private def loadLinkInfo(dxProject: DXProject) : Map[String, ExecLinkInfo]= {
        Utils.appletLog(s"Loading link information")
        val linkSourceFile: Path = Paths.get("/" + Utils.LINK_INFO_FILENAME)
        if (!Files.exists(linkSourceFile)) {
            Map.empty
        } else {
            val info: String = Utils.readFileContent(linkSourceFile)
            try {
                info.parseJson.asJsObject.fields.map {
                    case (key:String, jso) =>
                        key -> ExecLinkInfo.readJson(jso, dxProject)
                    case _ =>
                        throw new AppInternalException(s"Bad JSON")
                }.toMap
            } catch {
                case e : Throwable =>
                    throw new AppInternalException(s"Link JSON information is badly formatted ${info}")
            }
        }
    }

    private def evalStatement(scope, env: Env) : Env {
    }

    def apply(wf: WdlWorkflow,
              inputs : Map[String, WdlVarLinks]) : JsValue = {
        Utils.appletLog(s"inputs=${inputs}")

        // Get handles for the referenced dx:applets
        val dxEnv = DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()
        val linkInfo = loadLinkInfo(dxProject)
        Utils.appletLog(s"link info=${linkInfo}")
        val applets : Seq[(WdlCall, ExecLinkInfo)] = findApplets(scope, linkInfo)

        // build the environment from the dx:applet inputs
        val envBgn = inputs.map{
            case (varName, wvl) =>
                val wdlValue = wdlValueFromWVL(wvl)
                val rVar = RVar(varName, wvl.womType, wdlValue)
                varName -> rVar
        }.toMap

        // evaluate each of the statements in the workflow
        val envEnd = wf.children.foldLeft(envBgn) {
        }

        val blockOutputs : Map[String, WdlVarLinks] = scope match {
            case scatter:Scatter =>
                // Lookup the array we are looping on, it is guarantied to be a variable.
                val collection = lookup(outerEnv, scatter.collection.toWomString)
                evalScatter(scatter, collection, applets, outerEnv)
            case cond:If =>
                // Lookup the condition variable
                val condition = lookup(outerEnv, cond.condition.toWomString)
                evalIf(cond, condition, applets, outerEnv)
            case x =>
                throw new Exception(cef.notCurrentlySupported(x.ast, "scope element"))
        }
        Utils.appletLog(s"block outputs=${blockOutputs}")

        // Add the declarations at the beginning of the
        // workflow. Ignore non-exported values.
        val js_outputs: Map[String, JsValue] =
            (blockOutputs ++ preDecls)
                .filter{ case (varName, _) => isExported(varName) }
                .map{ case (varName, wvl) => WdlVarLinks.genFields(wvl, varName) }
                .flatten
                .toMap

        // outputs as JSON
        JsObject(js_outputs)
    }
}

object MiniWorkflow {
    def apply(wf: WdlWorkflow,
              inputSpec: Map[String, Utils.DXIOParam],
              outputSpec: Map[String, Utils.DXIOParam],
              inputs: Map[String, WdlVarLinks],
              orgInputs: JsValue,
              collectSubjob: Boolean) : Map[String, JsValue] = {
        Utils.appletLog(s"WomType mapping =${inputSpec}")
        val exportVars = outputSpec.keys.toSet
        Utils.appletLog(s"exportVars=${exportVars}")

        // Run the workflow
        val cef = new CompilerErrorFormatter("", wf.wdlSyntaxErrorFormatter.terminalMap)
        val r = MiniWorkflow(exportVars, cef, orgInputs, collectSubjob, false)
        val json = r.apply(wf, inputs)
        json.asJsObject.fields
    }
}
