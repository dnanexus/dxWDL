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
import com.fasterxml.jackson.databind.JsonNode
import dxWDL._
import dxWDL.Utils.{isOptional, makeOptional, stripOptional}
import java.nio.file.{Path, Paths, Files}
import scala.util.{Failure, Success}
import spray.json._
import wdl.draft2.model._
import wom.values._
import wom.types._

case class WfFragment(nswf: WdlNamespaceWithWorkflow,
                      instanceTypeDB: InstanceTypeDB,
                      execSeqMap: Map[Int, ChildExecDesc],
                      execLinkInfo: Map[String, ExecLinkInfo],
                      cef: CompilerErrorFormatter,
                      orgInputs: JsValue,
                      runMode: RunnerWfFragmentMode.Value,
                      verbose: Boolean) {

    def makeOptionalWomValue(t: WomType,
                             v: WomValue) : WomValue = {
        v match {
            case WomOptionalValue(WomOptionalType(WomOptionalType(_)),_) =>
                throw new Exception(s"double optional type")
            case WomOptionalValue(_, Some(WomOptionalValue(_,_))) =>
                throw new Exception("double optional value")
            case WomOptionalValue(_, _) =>
                v
            case _ =>
                WomOptionalValue(stripOptional(t), Some(v))
        }
    }

    def makeOptionalNone(t: WomType) : WomValue = {
        WomOptionalValue(stripOptional(t), None)
    }

    // A runtime representation of a WDL variable, or a call.
    //

    // the aggregated values of a declaration
    case class ElemWom( name: String,
                        womType: WomType,
                        value: WomValue ) {
        override def toString =
            s"${name}  ${womType.toDisplayString}  ${value.toWomString}"
    }

    // The aggregated values of a declaration.
    // The core problem is that we can't use the wdl.values.WdlCallOutputObject. It
    // throw an exceptionwhen we add the optional modifier to it.
    sealed trait Aggr
    case class AggrCall(name: String,
                        seqNum: Int,
                        dxExec: DXExecution ) extends Aggr
    case class AggrCallArray(children: Vector[Aggr]) extends Aggr
    case class AggrCallOption(child: Option[Aggr]) extends Aggr

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
                            if (tArray != WomArrayType(t)) {
                                System.err.println(s"tArray=${tArray}  t=${t}")
                                assert (tArray == WomArrayType(t))
                            }
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
        // call and variable. Used to merge environments resulting
        // from a scatter.
        def scatterInit(blockTypeMap: Map[String, WomType],
                        blockCallNames: Seq[String]) : Env = {
            // regular declarations
            val decls = blockTypeMap.map{
                case (varName, t) =>
                    varName -> ElemWom(varName,
                                       WomArrayType(t),
                                       WomArray(WomArrayType(t), Vector.empty))
            }.toMap

            // task/workflow calls
            val calls = blockCallNames.map{
                case callName =>
                    callName -> AggrCallArray(Vector.empty)
            }.toMap

            Env(Map.empty, decls, calls)
        }

        // Environment with a None value for each variable.
        def emptyOptionals(blockTypeMap: Map[String, WomType],
                           blockCallNames: Vector[String]) : Env = {
            // regular declarations, avoid creating Optional[Optional[_]] types.
            val decls = blockTypeMap.map{
                case (varName, t) =>
                    varName -> ElemWom(varName, makeOptional(t), makeOptionalNone(t))
            }.toMap

            val calls = blockCallNames.map{
                case callName =>
                    callName -> AggrCallOption(None)
            }.toMap

            Env(Map.empty, decls, calls)
        }

        def applyOptionalModifier(env: Env,
                                  blockTypeMap: Map[String, WomType],
                                  blockCallNames: Vector[String]) : Env = {
            val emptyEnv = emptyOptionals(blockTypeMap, blockCallNames)
/*            System.err.println("===  applyOptionalModifier")
            emptyEnv.decls.foreach{ case (_, elem) =>
                System.err.println(s"    ${elem.name}   ${elem.womType}  ${elem.value}")
            }*/

            val calls = emptyEnv.calls.map{
                case (name, defaultVal) =>
                    val aggr = env.calls.get(name) match {
                        case None =>
                            defaultVal
                        case Some(aggr) =>
                            AggrCallOption(Some(aggr))
                    }
                    name -> aggr
            }

            val decls = emptyEnv.decls.map {
                case (name, ElemWom(_, t, value)) =>
                    assert(isOptional(t))
                    val elemWom = env.decls.get(name) match {
                        case None =>
                            ElemWom(name, t, value)
                        case Some(elem) =>
                            ElemWom(name, t,
                                    makeOptionalWomValue(t, elem.value))
                    }
                    name -> elemWom
            }

/*            System.err.println()
            decls.foreach{ case (_, elem) =>
                System.err.println(s"    ${elem.name}   ${elem.womType}  ${elem.value}")
            }
            System.err.println("===")*/

            Env(env.launched, decls, calls)
        }

        def concat(envA: Env, envB: Env) : Env = {
            Env(envA.launched ++ envB.launched,
                envA.decls ++ envB.decls,
                envA.calls ++ envB.calls)
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

    private def lookupInEnv(env: Env)(varName : String) : WomValue = {
        val retval = env.decls.get(varName) match {
            case Some(ElemWom(_,_,value)) =>
                value
            case _ =>
                throw new UnboundVariableException(s"${varName}")
        }
        //System.err.println(s"lookupInEnv ${varName}  -> ${retval}")
        retval
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
                                  env : Env) : (Map[String, WdlVarLinks], JsValue) = {
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
        (wvlInputs, JsObject(mNonNull))
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

    // If this is a call to a task that computes the required instance type at runtime,
    // do the calculation right now. This saves a job relaunch down the road.
    private def preCalcInstanceType(task: WdlTask,
                                    taskInputs:Map[String, WdlVarLinks]) : Option[String] = {
        val taskRunner = new Task(task, instanceTypeDB, cef, true)
        try {
            val iType = taskRunner.calcInstanceType(taskInputs)
            Utils.appletLog(verbose, s"Precalculated instance type for ${task.unqualifiedName}: ${iType}")
            Some(iType)
        } catch {
            case e : Throwable =>
                Utils.appletLog(verbose,
                                s"""|Failed to precalculate the instance type for
                                    |task ${task.unqualifiedName}.
                                    |
                                    |${e}
                                    |""".stripMargin)
                None
        }
    }

    private def execCall(call: WdlCall,
                         env: Env,
                         callNameHint: Option[String]) : (Int, DXExecution) = {
        val calleeName = calleeGetName(call)
        val eInfo = execLinkInfo.get(calleeName) match {
            case None =>
                throw new AppInternalException(
                    s"Could not find linking information for ${call.unqualifiedName}")
            case Some(eInfo) => eInfo
        }
        val (callInputsWvl, callInputs:JsValue) = buildAppletInputs(call, eInfo, env)
        Utils.appletLog(verbose, s"Call ${call.unqualifiedName}   inputs = ${callInputs}")

        // We may need to run a collect subjob. Add the call
        // name, and the sequence number, to each execution invocation,
        // so the collect subjob will be able to put the
        // results back together.
        val seqNum: Int = launchSeqNum()
        val dbgName = callNameHint match {
            case None => call.unqualifiedName
            case Some(hint) => s"${call.unqualifiedName} ${hint}"
        }
        val dxExecId = eInfo.dxExec.getId
        val dxExec =
            if (dxExecId.startsWith("app-")) {
                val applet = DXApplet.getInstance(dxExecId)
                val fields = Map(
                    "name" -> JsString(dbgName),
                    "input" -> callInputs,
                    "properties" -> JsObject("call" ->  JsString(call.unqualifiedName),
                                             "seq_number" -> JsString(seqNum.toString))
                )
                // If this is a task that specifies the instance type
                // at runtime, launch it in the requested instance.
                val instanceType = nswf.findTask(calleeName) match {
                    case None => None
                    case Some(task) => preCalcInstanceType(task, callInputsWvl)
                }
                val instanceFields = instanceType match {
                    case None => Map.empty
                    case Some(iType) =>
                        Map("systemRequirements" -> JsObject(
                                "main" -> JsObject("instanceType" -> JsString(iType))
                            ))
                }
                val req = JsObject(fields ++ instanceFields)
                val retval: JsonNode = DXAPI.appletRun(applet.getId,
                                                       Utils.jsonNodeOfJsValue(req),
                                                       classOf[JsonNode])
                val info: JsValue =  Utils.jsValueOfJsonNode(retval)
                val id:String = info.asJsObject.fields.get("id") match {
                    case Some(JsString(x)) => x
                    case _ => throw new AppInternalException(
                        s"Bad format returned from jobNew ${info.prettyPrint}")
                }
                DXJob.getInstance(id)

            } else if (dxExecId.startsWith("workflow-")) {
                val workflow = DXWorkflow.getInstance(dxExecId)
                val dxAnalysis :DXAnalysis = workflow.newRun()
                    .setRawInput(Utils.jsonNodeOfJsValue(callInputs))
                    .setName(dbgName)
                    .putProperty("call", call.unqualifiedName)
                    .putProperty("seq_number", seqNum.toString)
                    .run()
                dxAnalysis
            } else {
                throw new Exception(s"Unsupported execution ${eInfo.dxExec}")
            }
        (seqNum, dxExec)
    }


    // Analyze a block of statements, return the types for all declarations
    private def statementsToTypes(statements: Seq[Scope]) : Map[String, WomType] = {
        statements.foldLeft(Map.empty[String, WomType]){
            case (accu, decl:Declaration) =>
                // base case: a declaration
                accu + (decl.unqualifiedName -> decl.womType)

            case (accu, call:WdlCall) =>
                // calls are handleded in statementsToCallNames
                accu

            case (accu, ssc:Scatter) =>
                // In a scatter, each variable becomes an array
                val innerEnv = statementsToTypes(ssc.children)
                val outerEnv = innerEnv.map{ case (name, t) => name -> WomArrayType(t) }
                accu ++ outerEnv

            case (accu, ifStmt:If) =>
                // In a conditional, each variable becomes an option.
                // Do not create Optional[Optional[_]] types.
                val innerEnv = statementsToTypes(ifStmt.children)
                val outerEnv = innerEnv.map{
                    case (name, t) => name -> Utils.makeOptional(t)
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

    // Make a list of all the calls made inside this statement block
    private def statementsToCallNames(statements: Seq[Scope]) : Vector[String] = {
        statements.foldLeft(Vector.empty[String]){
            case (accu, decl:Declaration) =>
                accu

            case (accu, call:WdlCall) =>
                accu :+ call.unqualifiedName

            case (accu, ssc:Scatter) =>
                accu ++ statementsToCallNames(ssc.children)

            case (accu, ifStmt:If) =>
                accu ++ statementsToCallNames(ifStmt.children)

            case (accu, wfo:WorkflowOutput) =>
                /// ignore workflow outputs
                accu

            case (_, other) =>
                throw new Exception(cef.notCurrentlySupported(
                                        other.ast,s"element ${other.getClass.getName}"))
        }
    }

    // Merge environments that are the result of a scatter.
    private def scatterMergeEnvs(blockTypeMap: Map[String, WomType],
                                 blockCallNames: Seq[String],
                                 childEnvs: Seq[Env]) : Env = {
        val emptyEnv = Env.scatterInit(blockTypeMap, blockCallNames)
        childEnvs.foldLeft(emptyEnv) {
            case (accuEnv, childEnv) =>
                Env.mergeTwo(accuEnv, childEnv)
        }
    }

    // create a short, easy to read, description for a scatter element.
    def readableNameForScatterItem(item: WomValue) : Option[String] = {
        item match {
            case WomBoolean(_) | WomInteger(_) | WomFloat(_) =>
                Some(item.toWomString)
            case WomString(s) =>
                Some(s)
            case WomSingleFile(path) =>
                val p = Paths.get(path).getFileName()
                Some(p.toString)
            case WomPair(l, r) =>
                val ls = readableNameForScatterItem(l)
                val rs = readableNameForScatterItem(r)
                (ls, rs) match {
                    case (Some(ls1), Some(rs1)) => Some(s"(${ls1}, ${rs1})")
                    case _ => None
                }
            case WomOptionalValue(_, Some(x)) =>
                readableNameForScatterItem(x)
            case _ =>
                None
        }
    }

    // For each element in the collection, perform all the
    // body statements
    private def evalScatter(ssc:Scatter,
                            envBgn: Env) : Env = {
        def lookup = lookupInEnv(envBgn)(_)

        val collection:WomArray = ssc.collection.evaluate(lookup, DxFunctions) match {
            case Success(a: WomArray) => a
            case Success(m: WomMap) => m.asArray
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
            val envInner = envBgn.copy(decls = envBgn.decls + (ssc.item -> item))
            val envEnd = ssc.children.foldLeft(envInner) {
                case (env2, stmt2) =>
                    val callHint = readableNameForScatterItem(itemVal)
                    evalStatement(stmt2, env2, callHint)
            }
            // We don't want to duplicate the environment we
            // started with.
            Env.removeKeys(envEnd, (envBgn.decls.keys.toSet + ssc.item))
        }
        val blockTypeMap = statementsToTypes(ssc.children)
        val blockCallNames = statementsToCallNames(ssc.children)
        val sme = scatterMergeEnvs(blockTypeMap, blockCallNames, sscEnvs)
        Env.concat(envBgn, sme)
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
                    case (env2, stmt2) => evalStatement(stmt2, env2, None)
                }
                // don't duplicate the top environment
                Env.removeKeys(envEnd, env.decls.keys.toSet)
            } else {
                // condition is false
                Env.empty
            }

        // Add the optional modifier to all elements.
        // If the inner type is already Optional, do not make it an Optional[Optional].
        val s2t = statementsToTypes(ifStmt.children)
        val blockCallNames = statementsToCallNames(ifStmt.children)

        // add optional modifiers to the environment
        val env2 = Env.applyOptionalModifier(childEnv, s2t, blockCallNames)
        Env.concat(env, env2)
    }

    private def evalStatement(stmt: Scope,
                              env: Env,
                              callNameHint: Option[String]) : Env = {
        def lookup = lookupInEnv(env)(_)
        stmt match {
            case decl:Declaration =>
                val wValue = decl.expression match {
                    case None if (env.decls contains decl.unqualifiedName) =>
                        // An input variable, read it from the environment
                        val elem = env.decls(decl.unqualifiedName)
                        elem.value
                    case None if isOptional(decl.womType) =>
                        // An optional input, make it None
                        makeOptionalNone(decl.womType)
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
                env.copy(decls = env.decls + (decl.unqualifiedName -> elem))

            case call:WdlCall =>
                runMode match {
                    case RunnerWfFragmentMode.Launch =>
                        // Embed the execution-id into the call object. This allows
                        // referencing the job/analysis results later on.
                        val (seqNum, dxExec) = execCall(call, env, callNameHint)
                        val elem = AggrCall(call.unqualifiedName, seqNum, dxExec)
                        env.copy(launched = env.launched + (seqNum -> (call, dxExec)),
                                 calls = env.calls + (call.unqualifiedName -> elem))
                    case RunnerWfFragmentMode.Collect =>
                        // Deterministically find the launch sequence number,
                        // and retrieve the job-id
                        val seqNum:Int = launchSeqNum()
                        val childInfo = execSeqMap(seqNum)
                        val elem = AggrCall(call.unqualifiedName, seqNum, childInfo.exec)
                        env.copy(calls = env.calls + (call.unqualifiedName -> elem))
                }

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


    // Launch a subjob to collect the outputs
    private def launchCollectSubjob(childJobs: Vector[DXExecution],
                                    calls: Vector[WdlCall],
                                    exportTypes: Map[String, WomType]) : Map[String, WdlVarLinks] = {
        assert(!childJobs.isEmpty)
        Utils.appletLog(verbose, s"""|launching collect subjob
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
        wf.outputs.map{ wfo =>
            val srcName = wfo.unqualifiedName.stripPrefix("out_")
            srcName -> wfo.womType
        }.toMap
    }

    // Aggregate all the values for a particular call field.
    //
    // Look at the outputs of each execution, and retrieve the
    // particular field name
    private def collectCallField(fieldName: String,
                                 womType: WomType,
                                 aggr: Aggr) : WomValue = {
        (aggr, womType) match {
            case (AggrCall(_,seqNum,_), _) =>
                // The field may be missing, put in a JsNull in this case.
                val childDesc = execSeqMap(seqNum)
                val jsv = childDesc.outputs.asJsObject.fields.get(fieldName) match {
                    case None => JsNull
                    case Some(jsv) => jsv
                }

                // Import the value from the dx-executable, to a local WOM value.
                // Avoid any downloads
                val wvl = WdlVarLinks.importFromDxExec(womType, DeclAttrs.empty, jsv)
                val womValue = WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)
                womValue

            case (AggrCallArray(children), WomArrayType(tInner)) =>
                // recurse into the children, build WomValues
                val children2 = children.map{ child => collectCallField(fieldName, tInner, child) }
                WomArray(WomArrayType(tInner), children2)

            case (AggrCallOption(None), _) =>
                makeOptionalNone(womType)

            case (AggrCallOption(Some(child)), WomOptionalType(tInner)) =>
                val value = collectCallField(fieldName, tInner, child)
                makeOptionalWomValue(womType, value)
        }
    }

    // The WDL workflow has no scatters or if blocks.
    private def isSimpleCode(wf: WdlWorkflow) : Boolean = {
        wf.children.forall{
            case _:Declaration => true
            case _:WdlCall => true
            case _:Scatter => false
            case _:If => false
            case _:WorkflowOutput => true
            case other =>
                throw new Exception(cef.notCurrentlySupported(
                                        other.ast,
                                        s"element ${other.getClass.getName}"))
        }
    }

    // create promises to this call. This allows returning
    // from the parent job immediately.
    private def genPromisesForCall(call: WdlCall,
                                   dxExec: DXExecution,
                                   exportTypes: Map[String, WomType]) : Map[String, WdlVarLinks] = {
        call.outputs.map{ cot =>
            val eVarName = cot.unqualifiedName
            val oName = s"${call.unqualifiedName}_${eVarName}"
            assert(exportTypes contains oName)
            oName -> WdlVarLinks(cot.womType,
                                 DeclAttrs.empty,
                                 DxlExec(dxExec, eVarName))
        }.toMap
    }

    def apply(inputs: Map[String, WdlVarLinks]) : Map[String, WdlVarLinks] = {
        // build the environment from the dx:applet inputs
        val inputDecls: Map[String, ElemWom] = inputs.map{
            case (varName, wvl) =>
                val wdlValue = wdlValueFromWVL(wvl)
                val rVar = ElemWom(varName, wvl.womType, wdlValue)
                varName -> rVar
        }.toMap
        val envBgn = Env(Map.empty, inputDecls, Map.empty)
        Utils.appletLog(verbose, s"envBgn = ${envBgn.decls}")

        // evaluate each of the statements in the workflow
        val envEnd = nswf.workflow.children.foldLeft(envBgn) {
            case (env, stmt) => evalStatement(stmt, env, None)
        }
        val exportTypes = calcExportVarTypes(nswf.workflow)
        //Utils.appletLog(verbose, s"exportTypes = ${exportTypes}")

        runMode match {
            case RunnerWfFragmentMode.Launch =>
                val (calls, childJobs) = envEnd.launched.values.unzip
                Utils.appletLog(verbose, s"childJobs=${childJobs}")

                val declOutputs = envEnd.decls.flatMap{
                    case (varName, ElemWom(_,womType, value)) if exportTypes contains varName =>
                        Some(varName -> wdlValueToWVL(womType, value))
                    case (_,_) =>
                        None
                }.toMap

                val promises : Map[String, WdlVarLinks] =
                    if (childJobs.isEmpty) {
                        // There are no calls, we can return the results immediately
                        Map.empty
                    } else if (childJobs.size == 1 && isSimpleCode(nswf.workflow)) {
                        // The WDL workflow has no scatters or if blocks. We can return
                        // promises to the calls.
                        val (_, (call, dxJob)) = envEnd.launched.head
                        genPromisesForCall(call, dxJob, exportTypes)
                    } else {
                        // Launch a subjob to collect and marshal the call results.
                        // Remove the declarations already calculated
                        val declarations = declOutputs.keys.toSet
                        val remaining = exportTypes.filter{ case (name,_) => !(declarations contains name) }
                        launchCollectSubjob(childJobs.toVector, calls.toVector, remaining)
                    }
                declOutputs ++ promises

            case RunnerWfFragmentMode.Collect =>
                // aggregate call results
                val callResults: Map[String, WdlVarLinks] =
                    envEnd.calls.foldLeft(Map.empty[String, WdlVarLinks]){
                        case (accu, (callName, aggr)) =>
                            val call = nswf.workflow.findCallByName(callName).get
                            val fields = call.outputs.map{ cot =>
                                val fullName = s"${call.unqualifiedName}_${cot.unqualifiedName}"
                                val womType = exportTypes(fullName)
                                val value: WomValue = collectCallField(cot.unqualifiedName, womType, aggr)
                                fullName -> wdlValueToWVL(womType, value)
                            }.toMap
                            accu ++ fields
                    }
                callResults
        }
    }
}

object WfFragment {
    // Load from disk a mapping of applet name to id. We
    // need this in order to call the right version of other
    // applets.
    private def loadLinkInfo(dxProject: DXProject,
                             verbose: Boolean) : Map[String, ExecLinkInfo] = {
        Utils.appletLog(verbose, s"Loading link information")
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


    def apply(nswf: WdlNamespaceWithWorkflow,
              instanceTypeDB: InstanceTypeDB,
              inputSpec: Map[String, DXIOParam],
              outputSpec: Map[String, DXIOParam],
              inputs: Map[String, WdlVarLinks],
              orgInputs: JsValue,
              runMode: RunnerWfFragmentMode.Value,
              verbose: Boolean) : Map[String, JsValue] = {
        val wdlCode: String = WdlPrettyPrinter(false, None).apply(nswf, 0).mkString("\n")
        Utils.appletLog(verbose, s"Workflow source code:")
        Utils.appletLog(verbose, wdlCode, 10000)
        //Utils.appletLog(verbose, s"Input spec: ${inputSpec}")
        Utils.appletLog(verbose, s"Inputs: ${inputs}")
        Utils.appletLog(verbose, s"runMode=${runMode}")

        // Get handles for the referenced dx:applets
        val dxEnv = DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()
        val execLinkInfo = loadLinkInfo(dxProject, verbose)
        Utils.appletLog(verbose, s"link info=${execLinkInfo}")

        val execSeqMap: Map[Int, ChildExecDesc] = runMode match {
            case RunnerWfFragmentMode.Launch =>
                Map.empty
            case RunnerWfFragmentMode.Collect =>
                Collect.executableFromSeqNum()
        }

        // Run the workflow
        val wf = nswf.workflow
        val cef = new CompilerErrorFormatter("", wf.wdlSyntaxErrorFormatter.terminalMap)
        val r = WfFragment(nswf, instanceTypeDB, execSeqMap, execLinkInfo, cef, orgInputs, runMode, verbose)
        val wvlVarOutputs = r.apply(inputs)

        // convert from WVL to JSON
        val jsVarOutputs: Map[String, JsValue] =
            wvlVarOutputs.foldLeft(Map.empty[String, JsValue]) {
                case (accu, (varName, wvl)) =>
                    val fields = WdlVarLinks.genFields(wvl, varName)
                    accu ++ fields.toMap
            }

        //Utils.appletLog(verbose, s"outputSpec= ${outputSpec}")
        //Utils.appletLog(verbose, s"jsVarOutputs= ${jsVarOutputs}")
        jsVarOutputs
    }
}
