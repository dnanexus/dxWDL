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

import Collect.ChildExecDesc
import com.dnanexus._
import dxWDL._
import java.nio.file.{Path, Paths, Files}
import scala.util.{Failure, Success}
import spray.json._
import wdl._
import wdl.types.WdlCallOutputsObjectType
import wom.values._
import wom.types._

case class WfFragment(execSeqMap: Map[Int, ChildExecDesc],
                      execLinkInfo: Map[String, ExecLinkInfo],
                      exportVars: Set[String],
                      cef: CompilerErrorFormatter,
                      orgInputs: JsValue,
                      runMode: RunnerWfFragmentMode.Value,
                      verbose: Boolean) {
    // A runtime representation of a WDL variable, or a call.
    //

    // the aggregated values of a declaration
    case class ElemWom( name: String,
                        womType: WomType,
                        value: WomValue )

    // The aggregated values of a declaration.
    // The core problem is that we can't use the wdl.values.WdlCallOutputObject. It
    // throw an exceptionwhen we add the optional modifier to it.
    sealed trait Aggr
    case class AggrCall(name: String,
                        seqNum: Int,
                        dxExec: DXExecution ) extends Aggr
    case class AggrCallArray(children: Vector[Aggr]) extends Aggr
    case class AggrCallOption(child: Option[Aggr]) extends Aggr

    val callNames: Set[String] = execLinkInfo.keys.toSet

    // An environment where WDL expressions and calls are evaluated
    // and aggregated.
    case class Env(launched: Map[Int, (WdlCall, DXExecution)],
                   decls: Map[String, ElemWom],
                   calls: Map[String, Aggr])
    object Env {
        // An empty environment
        val empty = Env(Map.empty, Map.empty, Map.empty)

        // Merge two environments. The Major parameter should
        // have all the declarations already defined.
        def mergeTwo(envMajor: Env, envChild: Env) : Env = {
            // merge the declarations
            val mrgDecls = envChild.decls.foldLeft(envMajor.decls) {
                case (accu, (name, elem)) =>
                    val accuElem = accu.get(name) match {
                        case None => throw new Exception(s"variable ${name} is unbound ")
                        case Some(x) => x
                    }
                    val elemWom = (accuElem, elem) match {
                        case (ElemWom(_, tArray, WomArray(_, arrValues)),
                              ElemWom(_,t, value)) =>
                            // append the values to a vector
                            assert(tArray == WomArrayType(t))
                            ElemWom(name,
                                    WomArrayType(t),
                                    WomArray(WomArrayType(t), arrValues :+ value))

                        case (other,_) =>
                            throw new Exception(s"variable ${name} is bound to an incorrect value ${other}")
                    }
                    accu + (name -> elemWom)
            }

            // merge the calls
            val mrgCalls = envChild.calls.foldLeft(envMajor.calls) {
                case (accu, (name,elem)) =>
                    val accuElem = accu.get(name) match {
                        case None => throw new Exception(s"variable ${name} is unbound ")
                        case Some(x) => x
                    }
                    val aggr = accuElem match {
                        case AggrCallArray(children) =>
                            AggrCallArray(children :+ elem)
                        case other =>
                            throw new Exception(s"variable ${name} is bound to an incorrect value ${other}")
                    }
                    accu + (name -> aggr)
            }

            Env(envMajor.launched ++ envChild.launched,
                mrgDecls,
                mrgCalls)
        }

        // Remove all the bindings of environment [b] from [a]
        def removeKeys(env: Env, keys: Set[String]) : Env = {
            val decls = env.decls.filter{ case (k, _) => !(keys contains k) }
            env.copy(decls = decls)
        }

        // Create an enviroment with an empty array for each
        // variable.
        def emptyArrays(varTypes: Map[String, WomType]) : Env = {
            val calls = varTypes.flatMap{
                case (varName, t) if (callNames contains varName) =>
                    // a task/workflow call
                    Some(varName -> AggrCallArray(Vector.empty))
                case (_,_) => None
            }

            // regular declaration
            val decls = varTypes.flatMap{
                case (varName, t) if !(callNames contains varName) =>
                    Some(varName -> ElemWom(varName,
                                            t,
                                            WomArray(t, Vector.empty)))
                case (_,_) => None
            }.toMap

            Env(Map.empty, declsm, calls)
        }

        // Environment with a None value for each variable.
        def emptyOptionals(varTypes: Map[String, WomType]) : Env = {
            val calls = varTypes.flatMap{
                case (varName, t) if (callNames contains name) =>
                    // a task/workflow call
                    Some(varName -> AggrCallOption(None))
                case (_,_) => None
            }

            // regular declarations
            val decls = varTypes.flatMap{
                case (varName, t) if !(callNames contains name) =>
                    Some(varName -> ElemWom(varName,
                                            WomOptionalType(t),
                                            WomOptionalValue(WomOptionalType(t), None)))
                case _ => None
            }.toMap

            Env(Map.empty, decls, calls)
        }

        def applyOptionalModifier(env: Env,
                                  varTypes: Map[String, WomType]) : Env = {
            val emptyEnv = emptyOptionals(varTypes)

            val calls = envEmpty.calls.map{
                case (name, defaultVal) =>
                    val aggr = env.get(name) match {
                        case None =>
                            defaultVal
                        case Some(aggr) =>
                            val t:WomType = varTypes(name)
                            ElemCallOption(name, WomOptionalType(t), Some(aggr))
                    }
                    name -> aggr
            }

            val decls = envEmpty.decls.map {
                case (name, ElemWom(_, t, value)) =>
                    val elemWom = env.get(name) match {
                        case None =>
                            ElemWom(name, t, value)
                        case Some(elem) =>
                            ElemWom(name, t, Some(elem.value))
                    }
                    name -> elemWom
            }
            Env(env.launched, decls, calls)
        }
    }

    var gSeqNum = 0
    private def launchSeqNum() : Int = {
        gSeqNum += 1
        gSeqNum
    }

    private def wdlValueFromWVL(wvl: WdlVarLinks) : WomValue =
        WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)

    private def wdlValueToWVL(t:WomType, wdlValue:WomValue) : WdlVarLinks = {
        WdlVarLinks.importFromWDL(t, DeclAttrs.empty, wdlValue, IODirection.Zero)
    }

    private def lookupInEnv(env: Env)(varName : String) : WomValue =
        env.decls.get(varName) match {
            case Some(ElemWom(_,_,value)) =>
                value
            case _ =>
                throw new UnboundVariableException(s"${varName}")
        }

    /**
      In the workflow below, we want to correctly pass the [k] value
      to each [inc] Task invocation.

    scatter (k in integers) {
        call inc as inc {input: i=k}
    }
      */
    private def buildAppletInputs(call: WdlCall,
                                  linkInfo: ExecLinkInfo,
                                  env : Env) : JsValue = {
        def lookup = lookupInEnv(env)(_)
        val inputs: Map[String, WomValue] = linkInfo.inputs.flatMap{
            case (varName, wdlType) =>
                // The rhs is [k], the varName is [i]
                val rhs: Option[(String,WdlExpression)] =
                    call.inputMappings.find{ case(key, expr) => key == varName }
                rhs match {
                    case None =>
                        // No binding for this input. It might be optional.
                        wdlType match {
                            case WomOptionalType(_) => None
                            case _ =>
                                throw new AppInternalException(
                                    s"""|Call ${call.unqualifiedName} does not have a binding for required
                                        |variable ${varName}.""".stripMargin.trim)
                        }
                    case Some((_, expr)) =>
                        expr.evaluate(lookup, DxFunctions) match {
                            case Success(womValue) => Some(varName -> womValue)
                            case Failure(f) =>
                                System.err.println(s"Failed to evaluate expression ${expr.toWomString}")
                                throw f
                        }

                }
        }

        val wvlInputs = inputs.map{ case (name, womValue) =>
            val womType = linkInfo.inputs(name)
            name -> wdlValueToWVL(womType, womValue)
        }.toMap
        val m = wvlInputs.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, wvl)) =>
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
        }
        val mNonNull = m.filter{
            case (key, value) =>
                if (value == null) false
                else if (value == JsNull) false
                else if (value.isInstanceOf[JsArray]) {
                    val jsa = value.asInstanceOf[JsArray]
                    if (jsa.elements.length == 0) false
                    else true
                } else
                    true
        }
        JsObject(mNonNull)
    }

        // coerce a WDL value to the required type (if needed)
    private def cast(wdlType: WomType, v: WomValue, varName: String) : WomValue = {
        val retVal =
            if (v.womType != wdlType) {
                // we need to convert types
                wdlType.coerceRawValue(v).get
            } else {
                // no need to change types
                v
            }
        retVal
    }


    // Here, we use the flat namespace assumption. We use
    // unqualified names as Fully-Qualified-Names, because
    // task and workflow names are unique.
    private def calleeGetName(call: WdlCall) : String = {
        call match {
            case tc: WdlTaskCall =>
                tc.task.unqualifiedName
            case wfc: WdlWorkflowCall =>
                wfc.calledWorkflow.unqualifiedName
        }
    }

    private def execCall(call: WdlCall, env: Env) : (Int, DXExecution, Env) = {
        val calleeName = calleeGetName(call)
        val eInfo = execLinkInfo.get(calleeName) match {
            case None =>
                throw new AppInternalException(
                    s"Could not find linking information for ${call.unqualifiedName}")
            case Some(eInfo) => eInfo
        }
        val callInputs:JsValue = buildAppletInputs(call, eInfo, env)
        Utils.appletLog(s"Call ${call.unqualifiedName}   inputs = ${callInputs}")

        // We may need to run a collect subjob. Add the call
        // name, and the sequence number, to each execution invocation,
        // so the collect subjob will be able to put the
        // results back together.
        val seqNum: Int = launchSeqNum()
        val dxExec =
            if (eInfo.dxExec.isInstanceOf[DXApplet]) {
                val applet = eInfo.dxExec.asInstanceOf[DXApplet]
                val dxJob :DXJob = applet.newRun()
                    .setRawInput(Utils.jsonNodeOfJsValue(callInputs))
                    .setName(call.unqualifiedName)
                    .putProperty("call", call.unqualifiedName)
                    .putProperty("seq_number", seqNum.toString)
                    .run()
                dxJob
            } else if (eInfo.dxExec.isInstanceOf[DXWorkflow]) {
                val workflow = eInfo.dxExec.asInstanceOf[DXWorkflow]
                val dxAnalysis :DXAnalysis = workflow.newRun()
                    .setRawInput(Utils.jsonNodeOfJsValue(callInputs))
                    .setName(call.unqualifiedName)
                    .putProperty("call", call.unqualifiedName)
                    .putProperty("seq_number", seqNum.toString)
                    .run()
                dxAnalysis
            } else {
                throw new Exception(s"Unsupported execution ${eInfo.dxExec}")
            }
        val env2 = env.copy(launched = env.launched + (seqNum -> (call, dxExec)))
        (seqNum, dxExec, env2)
    }


    // Analyze a block of statements, return the types for all declarations
    private def statementsToTypes(statements: Seq[Scope]) : Map[String, WomType] = {
        statements.foldLeft(Map.empty[String, WomType]){
            case (accu, decl:Declaration) =>
                // base case: a declaration
                accu + (decl.unqualifiedName -> decl.womType)

            case (accu, call:WdlCall) =>
                // calls are presented as an object type
                accu + (call.unqualifiedName -> WdlCallOutputsObjectType(call))

            case (accu, ssc:Scatter) =>
                // In a scatter, each variable becomes an array
                val innerEnv = statementsToTypes(ssc.children)
                val outerEnv = innerEnv.map{ case (name, t) => name -> WomArrayType(t) }
                accu ++ outerEnv

            case (accu, ifStmt:If) =>
                // In a conditional, each variable becomes an option.
                // Do not create Optional[Optional] types.
                val innerEnv = statementsToTypes(ifStmt.children)
                val outerEnv = innerEnv.map{
                    case (name, WomOptionalType(t)) => name -> WomOptionalType(t)
                    case (name, t) => name -> WomOptionalType(t)
                }.toMap
                accu ++ outerEnv

            case (accu, wfo:WorkflowOutput) =>
                /// ignore workflow outputs
                accu

            case (_, other) =>
                throw new Exception(cef.notCurrentlySupported(
                                        other.ast,s"element ${other.getClass.getName}"))
        }
    }

    // Figure out the workflow output types
    private def wfOutputTypes(wf: WdlWorkflow) : Map[String, WomType] = {
        wf.outputs.map{ wfo =>
            val srcName = wfo.unqualifiedName.stripPrefix("out_")
            srcName -> wfo.womType
        }.toMap
    }

    // Merge environments that are the result of a scatter.
    private def scatterMergeEnvs(varTypes: Map[String, WomType],
                                 childEnvs: Seq[Env]) : Env = {
        val emptyEnv = Env.emptyArrays(varTypes)
        childEnvs.foldLeft(emptyEnv) {
            case (accuEnv, childEnv) =>
                Env.mergeTwo(accuEnv, childEnv)
        }
    }

    // For each element in the collection, perform all the
    // body statements
    private def evalScatter(ssc:Scatter,
                            envBgn: Env) : Env = {
        def lookup = lookupInEnv(envBgn)(_)

        val collection:WomArray = ssc.collection.evaluate(lookup, DxFunctions) match {
            case Success(a: WomArray) => a
            case Success(other) => throw new Exception(s"scatter collection is not an array, it is: ${other}")
            case Failure(f) =>
                System.err.println(s"Failed to evaluate scatter collection ${ssc.collection.toWomString}")
                throw f
        }
        val itemType = collection.womType match {
            case WomArrayType(t) => t
            case other => throw new Exception(s"collection does not have an array type ${other}")
        }
        val sscEnvs: Seq[Env] = collection.value.map{ itemVal =>
            val item = ElemWom(ssc.item, itemType, itemVal)
            val envInner = envBgn + (ssc.item -> item)
            val envEnd = ssc.children.foldLeft(envInner) {
                case (env2, stmt2) => evalStatement(stmt2, env2)
            }
            // We don't want to duplicate the environment we
            // started with.
            Env.removeKeys(envEnd, (envBgn.keys.toSet + ssc.item))
        }
        //Utils.appletLog(s"evalScatter sscEnvs=${sscEnvs}")
        val typeMap = statementsToTypes(ssc.children)
        envBgn ++ scatterMergeEnvs(typeMap, sscEnvs)
    }

    private def evalIf(ifStmt: If, env: Env) : Env = {
        // evaluate the condition
        def lookup = lookupInEnv(env)(_)
        val condValue = ifStmt.condition.evaluate(lookup, DxFunctions) match {
            case Success(WomBoolean(value)) => value
            case Success(other) => throw new Exception(s"condition has non boolean value ${other}")
            case Failure(f) =>
                System.err.println(s"Failed to evaluate condition ${ifStmt.condition.toWomString}")
                throw f
        }
        val childEnv: Env =
            if (condValue) {
                // Condition is true, evaluate the sub block
                val envEnd = ifStmt.children.foldLeft(env) {
                    case (env2, stmt2) => evalStatement(stmt2, env2)
                }
                // don't duplicate the top environment
                Env.removeKeys(envEnd, env.keys.toSet)
            } else {
                // condition is false
                Env.empty
            }

        // Add the optional modifier to all elements.
        // If the inner type is already Optional, do not make it an Optional[Optional].
        val s2t = statementsToTypes(ifStmt.children)

        // add optional modifiers to the environment
        val env2 = Env.applyOptionalModifier(childEnv, s2t)

        env ++ env2
    }

    private def evalStatement(stmt: Scope, env: Env) : Env = {
        def lookup = lookupInEnv(env)(_)
        stmt match {
            case decl:Declaration =>
                val wValue = decl.expression match {
                    case None if (env contains decl.unqualifiedName) =>
                        // An input variable, read it from the environment
                        val elem = env(decl.unqualifiedName)
                        elem.value
                    case None =>
                        // A declaration with no value, such as:
                        //   String buffer
                        // Accessing "buffer" will result in an exception.
                        null
                    case Some(expr) =>
                        val vRaw : WomValue = expr.evaluate(lookup, DxFunctions).get
                        cast(decl.womType, vRaw, decl.unqualifiedName)
                }
                val elem = ElemWom(decl.unqualifiedName, decl.womType, wValue)
                env + (decl.unqualifiedName -> elem)

            case call:WdlCall =>
                val (rElem,env2) = runMode match {
                    case RunnerWfFragmentMode.Launch =>
                        val (seqNum, dxExec, env2) = execCall(call, env)
                        // Embed the execution-id into the call object. This allows
                        // referencing the job/analysis results later on.
                        val elem = AggrCall(call.unqualifiedName, seqNum, dxExec)
                        (elem, env2)
                    case RunnerWfFragmentMode.Collect =>
                        // Deterministically find the launch sequence number,
                        // and retrieve the job-id
                        val seqNum:Int = launchSeqNum()
                        val childInfo = execSeqMap(seqNum)
                        val elem = AggrCall(call.unqualifiedName, seqNum, childInfo.exec)
                        (elem, env)
                }
                env2 + (call.unqualifiedName -> rElem)

            case ssc:Scatter =>
                // Evaluate the collection, and iterate on the inner block N times.
                // This results in N environments that need to be merged.
                evalScatter(ssc, env)

            case cond:If =>
                // Evaluate the condition, and then the body of the block.
                // Add the None/Some type to all the resulting types/values.
                evalIf(cond, env)

            case _:WorkflowOutput =>
                // ignore workflow outputs
                env

            case other =>
                throw new Exception(cef.notCurrentlySupported(
                                        stmt.ast,s"element ${other.getClass.getName}"))
        }
    }


    // Create a mapping from the job output variables to json values. These
    // are variables that can be referenced by other calls.
    private def execOutputs(call: WdlCall,
                            dxExec: DXExecution) : Map[String, WdlVarLinks] = {
        call.outputs.map{ tso =>
            val fullName = s"${call.unqualifiedName}.${tso.unqualifiedName}"
            fullName -> WdlVarLinks(
                tso.womType,
                DeclAttrs.empty,
                DxlExec(dxExec, tso.unqualifiedName))
        }.toMap
    }

    // Launch a subjob to collect the outputs
    private def launchCollectSubjob(childJobs: Vector[DXExecution],
                                    calls: Vector[WdlCall],
                                    exportTypes: Map[String, WomType]) : Map[String, WdlVarLinks] = {
        assert(!childJobs.isEmpty)
        Utils.appletLog(s"""|launching collect subjob
                            |child jobs=${childJobs}""".stripMargin)

        // Run a sub-job with the "collect" entry point.
        // We need to provide the exact same inputs.
        val dxSubJob : DXJob = Utils.runSubJob("collect", None, orgInputs, childJobs)

        // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
        // is exactly the same as the parent, we can immediately exit the parent job.
        exportTypes.foldLeft(Map.empty[String, WdlVarLinks]) {
            case (accu, (eVarName, t)) =>
                val wvl = WdlVarLinks(t,
                                      DeclAttrs.empty,
                                      DxlExec(dxSubJob, eVarName))
                accu + (eVarName -> wvl)
        }.toMap
    }


    // Figure out the types of the output variables
    private def calcExportVarTypes(wf: WdlWorkflow) : Map[String, WomType] = {
        val oTypes = wfOutputTypes(wf)
        exportVars.map{ eVarName =>
            val womType = oTypes.get(eVarName) match {
                case None => throw new Exception(s"Can't figure out type of export variable ${eVarName}")
                case Some(t) => t
            }
            eVarName -> womType
        }.toMap
    }

    // Aggregate all the values for a particular call field.
    //
    // Look at the outputs of each execution, and retrieve the
    // particular field name
    private def collectCallField(fieldName: String,
                                 t: WomType,
                                 aggr: Aggr) : WomValue = {
        (aggr, t) match {
            case (AggrCall(_,seqNum,_), _) =>
                // The field may be missing, put in a JsNull in this case.
                val childDesc = execSeqMap(seqNum)
                val jsv = childDesc.outputs.asJsObject.fields.get(fieldName) match {
                    case None => JsNull
                    case Some(jsv) => jsv
                }
                // Import the value from the dx-executable, to a local WOM value.
                // Avoid any downloads
                val wvl = WdlVarLinks.importFromDxExec(ioParam, DeclAttrs.empty, jsv)
                WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)

            case (AggrCallArray(children), WomArrayType(tInner)) =>
                // recurse into the children, build WomValues
                val children2 = children.map{ child => collectCallField(fieldName, tInner, child) }
                WomArray(t, children2)

            case (AggrCallOption(None), _) =>
                WomOptionalValue(t, None)

            case (AggrCallOption(Some(child)), WomOptionalType(tInner)) =>
                val child = collectCallField(fieldName, tInner, child)
                WomOptionalValue(t, Some(child))
        }
    }

    def apply(wf: WdlWorkflow,
              inputs: Map[String, WdlVarLinks]) : Map[String, WdlVarLinks] = {
        // build the environment from the dx:applet inputs
        val envBgn: Map[String, RtmElem] = inputs.map{
            case (varName, wvl) =>
                val wdlValue = wdlValueFromWVL(wvl)
                val rVar = RtmElem(varName, wvl.womType, wdlValue)
                varName -> rVar
        }.toMap
        Utils.appletLog(s"env = ${envBgn}")

        // evaluate each of the statements in the workflow
        val envEnd = wf.children.foldLeft(envBgn) {
            case (env, stmt) => evalStatement(stmt, env)
        }
        Utils.appletLog(s"runMode=${runMode}")
        val exportTypes = calcExportVarTypes(wf)

        runMode match {
            case RunnerWfFragmentMode.Launch =>
                val (calls, childJobs) = envEnd.launched.values.unzip
                Utils.appletLog(s"childJobs=${childJobs}")

                if (childJobs.isEmpty) {
                    // There are no calls, we can return the results immediately
                    envEnd.decls.flatMap{
                        case (varName, ElemWom(_,womType, value)) =>
                            Some(varName -> wdlValueToWVL(womType, value))
                    }.toMap
                } else {
                    // Launch a subjob to collect and marshal the results.
                    //
                    launchCollectSubjob(childJobs.toVector, calls.toVector, exportTypes)
                }

            case RunnerWfFragmentMode.Collect =>
                // aggregate all the declaration
                val declResults: Map[String, WdlVarLinks] = envEnd.decls.map{
                    case (name, ElemWom(_, womType,value)) =>
                        name -> wdlValueToWVL(womType, value)
                }.toMap

                // aggregate call results
                val callResults: Map[String, WdlVarLinks] =
                    envEnd.calls.foldLeft(Map.empty){
                        case (accu, (callName, aggr)) =>
                            val call = wf.findCallByName(callName).get
                            val fields = call.outputs.foreach{ cot =>
                                val fullName = s"${call.unqualifiedName}.${cot.unqualifiedName}"
                                val t = exportTypes(fullName)
                                val value: WomValue = collectCallField(cot.unqualifiedName, t, aggr)
                                fullNAme -> wdlValueToWVL(cot.womType, value)
                            }
                            accu ++ fields
                    }
                declResults ++ callResults
        }
    }
}

object WfFragment {
    // Load from disk a mapping of applet name to id. We
    // need this in order to call the right version of other
    // applets.
    private def loadLinkInfo(dxProject: DXProject) : Map[String, ExecLinkInfo] = {
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


    def apply(wf: WdlWorkflow,
              inputSpec: Map[String, DXIOParam],
              outputSpec: Map[String, DXIOParam],
              inputs: Map[String, WdlVarLinks],
              orgInputs: JsValue,
              runMode: RunnerWfFragmentMode.Value) : Map[String, JsValue] = {
        val wdlCode: String = WdlPrettyPrinter(false, None, None).apply(wf, 0).mkString("\n")
        Utils.appletLog(s"Workflow source code:")
        Utils.appletLog(wdlCode)
        Utils.appletLog(s"Input spec: ${inputSpec}")
        Utils.appletLog(s"Inputs: ${inputs}")

        val exportVars = outputSpec.keys.toSet
        Utils.appletLog(s"exportVars=${exportVars}")

        // Get handles for the referenced dx:applets
        val dxEnv = DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()
        val execLinkInfo = loadLinkInfo(dxProject)
        Utils.appletLog(s"link info=${execLinkInfo}")

        val execSeqMap: Map[Int, ChildExecDesc] = runMode match {
            case RunnerWfFragmentMode.Launch =>
                Map.empty
            case RunnerWfFragmentMode.Collect =>
                Collect.executableFromSeqNum()
        }

        // Run the workflow
        val cef = new CompilerErrorFormatter("", wf.wdlSyntaxErrorFormatter.terminalMap)
        val r = WfFragment(execSeqMap, execLinkInfo, exportVars, cef, orgInputs, runMode, false)
        val wvlVarOutputs = r.apply(wf, inputs)

        // convert from WVL to JSON
        val jsVarOutputs: Map[String, JsValue] =
            wvlVarOutputs.foldLeft(Map.empty[String, JsValue]) {
                case (accu, (varName, wvl)) =>
                    val fields = WdlVarLinks.genFields(wvl, varName)
                    accu ++ fields.toMap
            }

        Utils.appletLog(s"outputSpec= ${outputSpec}")
        Utils.appletLog(s"jsVarOutputs= ${jsVarOutputs}")
        jsVarOutputs
    }
}
