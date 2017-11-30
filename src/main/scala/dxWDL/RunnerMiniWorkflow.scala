/** Execute a small workflow on the platform. Such a workflow
(1) starts with declarations
(2) has exactly one block command (scatter, if, etc.)
(3) ends with an output section.

Workflow wf_scat shows a scatter example. The scatter block has two
calls, and it iterates over the "numbers" array. It is legal to have
declarations requiring evaluation at the top of the block, and, inside
the scatter block.

workflow wf_scat {
    String pattern
    Array[Int] numbers = [1, 3, 7, 15]
    Array[Int] index = range(length(numbers))

    scatter (i in index) {
        call inc as inc1 {input: i=numbers[i]}
        call inc as inc2 {input: i=inc1.incremented}
    }

  output {
    Array[Int] inc1_result = inc1.result
    Array[Int] inc2_result = inc2.result
  }
}

A workflow with a conditional block is wf_cond.

workflow wf_cond {
   Int x
   Int y

   if (x > 10) {
      call Add {input: a=x, b=y}
      call Mul {input: a=Add.result, b=3}
   }
   output {
      Add.result
      Mul.result
   }
}
  */

package dxWDL

// DX bindings
import com.dnanexus._
import java.nio.file.{Path, Paths, Files}
import scala.collection.mutable.HashMap
import spray.json._
import Utils.{AppletLinkInfo, appletLog, callUniqueName, transformVarName}
import wdl4s.wdl._
import wdl4s.wdl.expression._
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wdl4s.wdl.values._
import wdl4s.wdl.types._
import wdl4s.wdl.WdlExpression.AstForExpressions

case class RunnerMiniWorkflow(exportVars: Set[String],
                              cef: CompilerErrorFormatter,
                              orgInputs: JsValue,
                              collectSubjob: Boolean,
                              verbose: Boolean) {
    // An environment element could be one of:
    // - top level value
    // - dictionary of values returned from a call
    sealed trait Elem
    case class ElemTop(wvl: WdlVarLinks) extends Elem
    case class ElemCall(outputs: Map[String, WdlVarLinks]) extends Elem
    type Env = Map[String, Elem]

    // check if a variable should be exported from this applet
    private def isExported(varName: String) : Boolean = {
        exportVars contains (transformVarName(varName))
    }
    // find the sequence of applets inside the scatter block. In the example,
    // these are: [inc1, inc2]
    private def findApplets(scope: Scope,
                            linkInfo: Map[String, AppletLinkInfo])
            : Seq[(WdlCall, AppletLinkInfo)] = {
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

    private def wdlValueFromWVL(wvl: WdlVarLinks) : WdlValue =
        WdlVarLinks.eval(wvl, false, IODirection.Zero)

    private def wdlValueToWVL(t:WdlType, wdlValue:WdlValue) : WdlVarLinks =
        WdlVarLinks.importFromWDL(t, DeclAttrs.empty, wdlValue, IODirection.Zero)

    private def evalDeclarationsWVL(declarations: Seq[DeclarationInterface],
                                    envInputs : Map[String, WdlVarLinks])
            : Map[String, WdlVarLinks] = {
        val inputs:Map[String, WdlValue] = envInputs.map{
            case (key, wvl) => key -> wdlValueFromWVL(wvl)
        }.toMap
        val env = RunnerEval.evalDeclarations(declarations, inputs)
        env.map { case (decl, wdlValue) =>
            decl.unqualifiedName -> wdlValueToWVL(decl.wdlType, wdlValue)
        }.toMap
    }

    // Evaluate a call input expression, and return a WdlVarLink structure. It
    // is passed on to another dx:job.
    //
    // The front end pass makes sure that the range of expressions is
    // limited: constants, variables, and member accesses.
    private def wvlEvalExpression(expr: WdlExpression, env : Env) : WdlVarLinks = {
        expr.ast match {
            case t: Terminal =>
                val srcStr = t.getSourceString
                t.getTerminalStr match {
                    case "identifier" =>
                        env.get(srcStr) match {
                            case Some(ElemTop(x)) => x
                            case _ => throw new Exception(cef.missingVarRef(t))
                        }
                    case _ =>
                        // a constant
                        def nullLookup(varName : String) : WdlValue = {
                            throw new Exception(cef.expressionMustBeConstOrVar(expr))
                        }
                        val wdlValue = expr.evaluate(nullLookup, NoFunctions).get
                        wdlValueToWVL(wdlValue.wdlType, wdlValue)
                }

            case a: Ast if a.isMemberAccess =>
                // An expression such as A.B.C.D. The components
                // are (A,B,C,D), and there must be at least two of them.
                val fqn = WdlExpression.toString(a)
                val components = fqn.split("\\.").toList
                components match {
                    case eTop::eMid::tail if (env contains eTop) =>
                        env(eTop) match {
                            case ElemTop(wvl) =>
                                // This is a map, pair, or object, and we need
                                // to do a member lookup.
                                WdlVarLinks.memberAccess(wvl, eMid::tail)
                            case ElemCall(callOutputs) =>
                                // Accessing a returned value from a call
                                callOutputs.get(eMid) match {
                                    case Some(wvl) => WdlVarLinks.memberAccess(wvl, tail)
                                    case _ =>
                                        throw new Exception(cef.undefinedMemberAccess(a))
                                }
                        }
                    case _ =>
                        throw new Exception(cef.undefinedMemberAccess(a))
                }

            case _:Ast =>
                throw new Exception(cef.expressionMustBeConstOrVar(expr))
        }
    }


    /**
      In the workflow below, we want to correctly pass the [k] value
      to each [inc] Task invocation.

    scatter (k in integers) {
        call inc as inc {input: i=k}
    }
      */
    private def buildAppletInputs(call: WdlCall,
                                  apLinkInfo: AppletLinkInfo,
                                  env : Env) : JsValue = {
        val callName = callUniqueName(call)
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
        JsObject(m)
    }

    // Create a mapping from the job output variables to json values. These
    // are variables that can be referenced by other calls.
    private def jobOutputEnv(call: WdlCall,
                             dxJob: DXJob) : Env = {
        val prefix = callUniqueName(call)
        val task = Utils.taskOfCall(call)
        val retValues = task.outputs
            .map { tso => tso.unqualifiedName -> WdlVarLinks(
                      tso.wdlType,
                      DeclAttrs.empty,
                      DxlJob(dxJob, tso.unqualifiedName)) }
            .toMap
        Map(prefix -> ElemCall(retValues))
    }

    private def flattenEnv(scEnv: Env) : Map[String, WdlVarLinks] = {
        scEnv.map{
            case (key, ElemTop(wvl)) => Map(key -> wvl)
            case (callName, ElemCall(outputs)) =>
                outputs.map{ case (varName, wvl) => callName + "." + varName -> wvl }.toMap
        }.flatten.toMap
    }

    // Gather outputs from calls in a scatter block into arrays. Essentially,
    // take a list of WDL values, and convert them into a WDL array. The complexity
    // comes in, because we need to carry around Json links to platform data objects.
    private def gatherOutputs(scOutputs : Seq[Env]) : Map[String, WdlVarLinks] = {
        // collect values for each key into lists
        val m : HashMap[String, Vector[WdlVarLinks]] = HashMap.empty
        scOutputs.foreach{ env =>
            val fenv = flattenEnv(env)
            fenv.map{ case (key, wvl) =>
                m(key) = m.get(key) match {
                    case None => Vector(wvl)
                    case Some(l) => l :+ wvl
                }
            }
        }
        m.map{ case (key, wvlVec) => key -> WdlVarLinks.merge(wvlVec) }
            .toMap
    }

    // Launch a subjob to collect the outputs
    private def launchCollectSubjob(childJobs: Vector[DXJob],
                                    calls: Vector[WdlCall]) : Map[String, WdlVarLinks] = {
        appletLog(s"""|launching collect subjob
                      |child jobs=${childJobs}""".stripMargin)
        if (childJobs.isEmpty)
            return Map.empty

        // Run a sub-job with the "collect" entry point.
        // We need to provide the exact same inputs.
        val dxSubJob : DXJob = Utils.runSubJob("collect", None, orgInputs, childJobs)

        // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
        // is exactly the same as the parent, we can immediately exit the parent job.
        calls.foldLeft(Map.empty[String, WdlVarLinks]) {
            case (accu, call) =>
                val prefix = callUniqueName(call)
                val promiseMap = call.outputs.map{ cao =>
                    val wvl = WdlVarLinks(cao.wdlType,
                                          DeclAttrs.empty,
                                          DxlJob(dxSubJob, prefix + "_" + cao.unqualifiedName))
                    (prefix + "." + cao.unqualifiedName) -> wvl
                }
                accu ++ promiseMap
        }
    }

    // Launch a job for each call, and link them with JBORs. Do not
    // wait for the jobs to complete, because that would require
    // leaving an auxiliary instance up for the duration of the subjob
    // executions.  Return the variables calculated.
    private def evalScatter(scatter : Scatter,
                            collection : WdlVarLinks,
                            calls : Seq[(WdlCall, AppletLinkInfo)],
                            outerEnv : Map[String, WdlVarLinks]) : Map[String, WdlVarLinks] = {
        appletLog(s"evalScatter")

        // add the top declarations in the scatter block to the
        // environment
        val (topDecls,_) = Utils.splitBlockDeclarations(scatter.children.toList)

        val collElements : Seq[WdlVarLinks] = WdlVarLinks.unpackWdlArray(collection)
        var scJobOutputs = Vector.empty[Env]
        var scTopOutputs = Vector.empty[Env]
        var childJobs = Vector.empty[DXJob]
        var launchSeqNum = 0
        collElements.foreach { case elem =>
            // Bind the iteration variable inside the loop
            val envWithIterItem: Map[String, WdlVarLinks] = outerEnv + (scatter.item -> elem)
            appletLog(s"envWithIterItem= ${envWithIterItem}")

            // calculate declarations at the top of the block
            var innerEnvRaw: Map[String, WdlVarLinks] = evalDeclarationsWVL(topDecls, envWithIterItem)
            val topOutputs = innerEnvRaw
                .filter{ case (varName, _) => isExported(varName) }
                .map{ case (varName, wvl) => varName -> ElemTop(wvl) }
                .toMap
            // export top variables
            scTopOutputs = scTopOutputs :+ topOutputs
            innerEnvRaw = innerEnvRaw ++ envWithIterItem
            var innerEnv:Env = innerEnvRaw.map{ case(key, wvl) => key -> ElemTop(wvl) }.toMap

            calls.foreach { case (call,apLinkInfo) =>
                val inputs : JsValue = buildAppletInputs(call, apLinkInfo, innerEnv)
                val callUnqName = callUniqueName(call)
                appletLog(s"call=${callUnqName} inputs=${inputs}")

                // We may need to run a collect subjob. Add the call
                // name, and the sequence number, to each applet run,
                // so the collect subjob will be able to put the
                // results back together.
                val dxJob : DXJob = apLinkInfo.dxApplet
                    .newRun()
                    .setRawInput(Utils.jsonNodeOfJsValue(inputs))
                    .putProperty("call", callUnqName)
                    .putProperty("seq_number", launchSeqNum.toString)
                    .run()
                val jobOutputs : Env = jobOutputEnv(call, dxJob)

                // add the job outputs to the environment. This makes them available to the applets
                // that come next.
                innerEnv = innerEnv ++ jobOutputs
                scJobOutputs = scJobOutputs :+ jobOutputs
                childJobs = childJobs :+ dxJob
                launchSeqNum += 1
            }
        }

        // Gather phase.
        val topVars = gatherOutputs(scTopOutputs)
        val childJobVars =
            if (!collectSubjob) {
                // Collect call outputs in arrays, do not wait
                // for the jobs to complete.
                gatherOutputs(scJobOutputs)
            } else {
                // The output types are complex, requiring a subjob.
                launchCollectSubjob(childJobs,
                                    calls.map{case (x,_) => x}.toVector)
            }
        topVars ++ childJobVars
    }

    // Check the condition. If false, return immediately. If true,
    // execute the top declarations, and then launch a subjobs for
    // each call. Return a JBORs for the results, do not
    // wait for the subjobs to complete, because that would require
    // leaving an auxiliary instance up for the duration of the
    // executions.
    private def evalIf(cond : If,
                       condition : WdlVarLinks,
                       calls : Seq[(WdlCall, AppletLinkInfo)],
                       outerEnv: Map[String, WdlVarLinks]) : Map[String, WdlVarLinks] = {
        appletLog(s"evalIf")

        // Evaluate condition
        val condValue:WdlValue = WdlVarLinks.eval(condition, true, IODirection.Download)
        val b = condValue match {
            case WdlBoolean(b) => b
            case _ => throw new AppInternalException("conditional expression is not boolean")
        }
        if (!b)
            return Map.empty
        appletLog(s"condition is true")

        // Evaluate the declarations at the top of the block
        val (topDecls,_) = Utils.splitBlockDeclarations(cond.children.toList)
        var allOutputs = Vector.empty[Env]

        // calculate declarations at the top of the block
        val innerEnvRaw: Map[String, WdlVarLinks] = evalDeclarationsWVL(topDecls, outerEnv)
        val topOutputs = innerEnvRaw
            .filter{ case (varName, _) => isExported(varName) }
            .map{ case (varName, wvl) => varName -> ElemTop(wvl) }
            .toMap

        // export top variables
        allOutputs = allOutputs :+ topOutputs
        var innerEnv:Env = (innerEnvRaw ++ outerEnv).map{
            case(key, wvl) => key -> ElemTop(wvl)
        }.toMap

        // iterate over the calls
        calls.foreach { case (call,apLinkInfo) =>
            val inputs : JsValue = buildAppletInputs(call, apLinkInfo, innerEnv)
            appletLog(s"call=${callUniqueName(call)} inputs=${inputs}")
            val dxJob: DXJob = apLinkInfo.dxApplet
                .newRun()
                .setRawInput(Utils.jsonNodeOfJsValue(inputs))
                .run()
            val jobOutputs: Env = jobOutputEnv(call, dxJob)

            // add the job outputs to the environment. This makes them available to the applets
            // that come next.
            innerEnv = innerEnv ++ jobOutputs
            allOutputs = allOutputs :+ jobOutputs
        }

        // Convert results into outputs
        val m : HashMap[String, WdlVarLinks] = HashMap.empty
        allOutputs.foreach{ env =>
            val fenv = flattenEnv(env)
            fenv.map{ case (key, wvl) =>
                m(key) = m.get(key) match {
                    case None => wvl
                    case Some(l) => throw new Exception("duplicate keys")
                }
            }
        }
        m.toMap
    }

    // Load from disk a mapping of applet name to id. We
    // need this in order to call the right version of other
    // applets.
    private def loadLinkInfo(dxProject: DXProject) : Map[String, AppletLinkInfo]= {
        appletLog(s"Loading link information")
        val linkSourceFile: Path = Paths.get("/" + Utils.LINK_INFO_FILENAME)
        if (!Files.exists(linkSourceFile)) {
            Map.empty
        } else {
            val info: String = Utils.readFileContent(linkSourceFile)
            try {
                info.parseJson.asJsObject.fields.map {
                    case (key:String, jso) =>
                        key -> AppletLinkInfo.readJson(jso, dxProject)
                    case _ =>
                        throw new AppInternalException(s"Bad JSON")
                }.toMap
            } catch {
                case e : Throwable =>
                    throw new AppInternalException(s"Link JSON information is badly formatted ${info}")
            }
        }
    }

    // Evaluate expressions at the beginning of the workflow
    private def evalPreDeclarations(decls: Seq[Declaration],
                                    inputs: Map[String, WdlVarLinks]) : Map[String, WdlVarLinks] = {
        // keep only expressions to calculate (non inputs)
        val exprDecls = decls.filter(decl => decl.expression != None)

        // evaluate the expressions, given the workflow inputs
        evalDeclarationsWVL(exprDecls, inputs)
    }

    // Split a workflow into the top declarations,
    // the and the main scope (Scatter, If, etc.)
    private def workflowSplit(wf: WdlWorkflow) : (Vector[Declaration], Scope) = {
        val (topDecls, rest) = Utils.splitBlockDeclarations(wf.children.toList)
        val scope: Scope = rest.head match {
            case scatter:Scatter => scatter
            case cond:If => cond
            case x => throw new Exception(cef.notCurrentlySupported(x.ast, "scope element"))
        }
        (topDecls.toVector, scope)
    }

    // Search for a variable in the environment.
    private def lookup(outerEnv: Map[String, WdlVarLinks],
                       varName: String) : WdlVarLinks = {
        outerEnv.get(varName) match {
            case None =>
                appletLog(s"outerEnv=${outerEnv}")
                throw new AppInternalException(s"Variable ${varName} not found in environment")
            case Some(wvl) => wvl
        }
    }

    def apply(wf: WdlWorkflow,
              inputs : Map[String, WdlVarLinks]) : JsValue = {
        appletLog(s"inputs=${inputs}")

        // Evaluate the declarations prior to the main block, and add them to the environment.
        // This is the environment outside the block.
        val (decls, scope) = workflowSplit(wf)
        val preDecls = evalPreDeclarations(decls, inputs)
        val outerEnv = inputs ++ preDecls

        // Get handles for the referenced dx:applets
        val dxEnv = DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()
        val linkInfo = loadLinkInfo(dxProject)
        appletLog(s"link info=${linkInfo}")
        val applets : Seq[(WdlCall, AppletLinkInfo)] = findApplets(scope, linkInfo)

        val blockOutputs : Map[String, WdlVarLinks] = scope match {
            case scatter:Scatter =>
                // Lookup the array we are looping on, it is guarantied to be a variable.
                val collection = lookup(outerEnv, scatter.collection.toWdlString)
                evalScatter(scatter, collection, applets, outerEnv)
            case cond:If =>
                // Lookup the condition variable
                val condition = lookup(outerEnv, cond.condition.toWdlString)
                evalIf(cond, condition, applets, outerEnv)
            case x =>
                throw new Exception(cef.notCurrentlySupported(x.ast, "scope element"))
        }
        appletLog(s"block outputs=${blockOutputs}")

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

object RunnerMiniWorkflow {
    def apply(wf: WdlWorkflow,
              inputSpec: Map[String, IOClass],
              outputSpec: Map[String, IOClass],
              inputs: Map[String, WdlVarLinks],
              orgInputs: JsValue,
              collectSubjob: Boolean) : Map[String, JsValue] = {
        appletLog(s"WdlType mapping =${inputSpec}")
        val exportVars = outputSpec.keys.toSet
        appletLog(s"exportVars=${exportVars}")

        // Run the workflow
        val cef = new CompilerErrorFormatter(wf.wdlSyntaxErrorFormatter.terminalMap)
        val r = RunnerMiniWorkflow(exportVars, cef, orgInputs, collectSubjob, false)
        val json = r.apply(wf, inputs)
        json.asJsObject.fields
    }
}
