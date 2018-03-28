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
import spray.json._
import wdl._
import wdl.types.WdlCallOutputsObjectType
import wdl.values.WdlCallOutputsObject
import wom.values._
import wom.types._


case class MiniWorkflow(execLinkInfo: Map[String, ExecLinkInfo],
                        exportVars: Option[Set[String]],
                        cef: CompilerErrorFormatter,
                        orgInputs: JsValue,
                        collectSubjob: Boolean,
                        verbose: Boolean) {
    // A runtime representation of a WDL variable, or a call.
    //
    // A call has the type WdlCallOutputsObjectType, and the value
    // WdlCallOutputsObject. Calling a dx:executable will take
    // a while to complete, so we store in the WdlCallOutputsObject
    // map the executable-id.
    case class RtmElem(name: String,
                       womType: WomType,
                       value: WomValue)

    // An environment where WDL expressions and calls are evaluated
    type Env = Map[String, RtmElem]

    var seqNum = 0
    private def launchSeqNum : Int = {
        seqNum += 1
        seqNum
    }

    // check if a variable should be exported from this applet
    private def isExported(varName: String) : Boolean = {
        exportVars match {
            case None =>
                // export everything
                true
            case Some(ev) =>
                // export only select fields
                ev contains (Utils.transformVarName(varName))
        }
    }

    private def wdlValueFromWVL(wvl: WdlVarLinks) : WomValue =
        WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)

    private def wdlValueToWVL(t:WomType, wdlValue:WomValue) : WdlVarLinks = {
        WdlVarLinks.importFromWDL(t, DeclAttrs.empty, wdlValue, IODirection.Zero)
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
        val inputs: Map[String, RtmElem] = linkInfo.inputs.flatMap{
            case (varName, wdlType) =>
                // The rhs is [k], the varName is [i]
                val rhs: Option[(String,WdlExpression)] =
                    call.inputMappings.find{ case(key, expr) => key == varName }
                val rElem:Option[RtmElem] = rhs match {
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
                        // The value must be a simple variable
                        val varRef = expr.toWomString
                        env.get(varRef) match {
                            case None =>
                                throw new AppInternalException(
                                    s"""|In call ${call.unqualifiedName}, variable ${varName} is set
                                        |to ${varRef}, which is unbound.""".stripMargin.trim)
                            case Some(rElem) => Some(rElem)
                        }
                }
                rElem match {
                    case None => None
                    case Some(r) => Some(varName -> r)
                }
        }
        val wvlInputs = inputs.map{ case (name, rElem) =>
            name -> wdlValueToWVL(rElem.womType, rElem.value)
        }.toMap
        val m = wvlInputs.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, wvl)) =>
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
        }
        val mNonNull = m.filter{ case (key, value) => value != null && value != JsNull}
        JsObject(mNonNull)
    }

    private def lookupInEnv(env: Env)(varName : String) : WomValue =
        env.get(varName) match {
            case Some(RtmElem(_,_,value)) => value
            case _ =>  throw new UnboundVariableException(s"${varName}")
        }

        // coerce a WDL value to the required type (if needed)
    private def cast(wdlType: WomType, v: WomValue, varName: String) : WomValue = {
        val retVal =
            if (v.womType != wdlType) {
                // we need to convert types
                //Utils.appletLog(s"casting ${v.womType} to ${wdlType}")
                wdlType.coerceRawValue(v).get
            } else {
                // no need to change types
                v
            }
        retVal
    }


    private def execCall(call: WdlCall, env: Env) : DXExecution = {
        val eInfo = execLinkInfo.get(call.unqualifiedName) match {
            case None =>
                throw new AppInternalException(
                    s"Could not find linking information for ${call.unqualifiedName}")
            case Some(eInfo) => eInfo
        }
        val callInputs:JsValue = buildAppletInputs(call, eInfo, env)

        // We may need to run a collect subjob. Add the call
        // name, and the sequence number, to each execution invocation,
        // so the collect subjob will be able to put the
        // results back together.
        if (eInfo.dxExec.isInstanceOf[DXApplet]) {
            val applet = eInfo.dxExec.asInstanceOf[DXApplet]
            val dxJob :DXJob = applet.newRun()
                .setRawInput(Utils.jsonNodeOfJsValue(callInputs))
                .setName(call.unqualifiedName)
                .putProperty("call", call.unqualifiedName)
                .putProperty("seq_number", launchSeqNum.toString)
                .run()
            dxJob
        } else if (eInfo.dxExec.isInstanceOf[DXWorkflow]) {
            val workflow = eInfo.dxExec.asInstanceOf[DXWorkflow]
            val dxAnalysis :DXAnalysis = workflow.newRun()
                .setRawInput(Utils.jsonNodeOfJsValue(callInputs))
                .setName(call.unqualifiedName)
                .putProperty("call", call.unqualifiedName)
                .putProperty("seq_number", launchSeqNum.toString)
                .run()
            dxAnalysis
        } else {
            throw new Exception(s"Unsupported execution ${eInfo.dxExec}")
        }
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
                // In a wdl.If, each variables becomes an option
                val innerEnv = statementsToTypes(ifStmt.children)
                val outerEnv = innerEnv.map{ case (name, t) => name -> WomOptionalType(t) }
                accu ++ outerEnv

            case (_, other) =>
                throw new Exception(cef.notCurrentlySupported(
                                        other.ast,s"element ${other.getClass.getName}"))
        }
    }

    // Remove all the bindings of environment [b] from [a]
    private def envRemoveKeys(env: Env, keys: Set[String]) = {
        env.filter{ case (k, _) => !(keys contains k) }
    }

    // Merge environments that are the result of a scatter.
    private def scatterMergeEnvs(typeMap: Map[String, WomType],
                                 childEnvs: Seq[Env]) : Env = {
        // Start with an enviroment with an empty array for each
        // variable.
        val emptyEnv:Env = typeMap.map{ case (varName, t) =>
            varName -> RtmElem(varName,
                               WomArrayType(t),
                               WomArray(WomArrayType(t), Vector.empty))
        }.toMap

        // Merge two environments
        def mergeTwo(envMajor: Env, envChild: Env) : Env = {
            envChild.foldLeft(envMajor) {
                case (envAccu, (_, RtmElem(name, t, valueChild))) =>
                    val rElem = envAccu.get(name) match {
                        case None =>
                            throw new Exception(s"variable ${name} is unbound ")
                        case Some(RtmElem(_, tArray, WomArray(_, arrValues))) =>
                            // append the values to a vector
                            assert(tArray == WomArrayType(t))
                            RtmElem(name,
                                    WomArrayType(t),
                                    WomArray(WomArrayType(t), arrValues.toVector :+ valueChild))
                        case Some(other) =>
                            throw new Exception(s"variable ${name} is bound to an incorrect value ${other}")
                    }
                    envAccu + (name -> rElem)
            }
        }
        childEnvs.foldLeft(emptyEnv) {
            case (accuEnv, childEnv) =>
                mergeTwo(accuEnv, childEnv)
        }
    }

    // For each element in the collection, perform all the
    private def evalScatter(ssc:Scatter,
                            envBgn: Env,
                            collectionType: WomType,
                            collection: WomValue) : Env = {
        val collectionArr:WomArray = collection match {
            case _:WomArray => collection.asInstanceOf[WomArray]
            case other => throw new Exception(s"Bad value ${other} for scatter collection")
        }
        val itemType:WomType = collectionType match {
            case WomArrayType(t) => t
            case _ => throw new Exception("Sanity")
        }
        val sscEnvs: Seq[Env] = collectionArr.value.map{ itemVal =>
            val item = RtmElem(ssc.item, itemType, itemVal)
            val envInner = envBgn + (ssc.item -> item)
            val envEnd = ssc.children.foldLeft(envInner) {
                case (env2, stmt2) => evalStatement(stmt2, env2)
            }
            // We don't want to duplicate the environment we
            // started with.
            envRemoveKeys(envEnd, (envBgn.keys.toSet + ssc.item))
        }
        val typeMap = statementsToTypes(ssc.children)
        envBgn ++ scatterMergeEnvs(typeMap, sscEnvs)
    }

    // The compiler ensures that the condition is a simple variable
    private def evalIf(ifStmt: If, env: Env) : Env = {
        val exprVar = ifStmt.condition.toWomString
        val condValue = env.get(exprVar) match {
            case Some(RtmElem(_, WomBooleanType, WomBoolean(value))) => value
            case other => throw new Exception(
                s"environment has non boolean value ${other} for variable ${exprVar}")
        }
        val childEnv: Env =
            if (condValue) {
                // Condition is true, evaluate the sub block
                val envEnd = ifStmt.children.foldLeft(env) {
                    case (env2, stmt2) => evalStatement(stmt2, env2)
                }
                // don't duplicate the top environment
                envRemoveKeys(envEnd, env.keys.toSet)
            } else {
                Map.empty
            }

        // Add the optional modifier to all elements
        val s2t = statementsToTypes(ifStmt.children)
        val envIfBlock:Env = s2t.map{ case (name, t: WomType) =>
            val rElem = childEnv.get(name) match {
                case None =>
                    RtmElem(name, WomOptionalType(t), WomOptionalValue(t, None))
                case Some(RtmElem(_,t2, value)) =>
                    assert(t == t2)
                    RtmElem(name, WomOptionalType(t), WomOptionalValue(t, Some(value)))
            }
            name -> rElem
        }.toMap
        env ++ envIfBlock
    }

    private def evalStatement(stmt: Scope, env: Env) : Env = {
        def lookup = lookupInEnv(env)(_)
        stmt match {
            case decl:Declaration =>
                val wValue = decl.expression match {
                    case None =>
                        // A declaration with no value, such as:
                        //   String buffer
                        // Accessing "buffer" will result in an exception.
                        null
                    case Some(expr) =>
                        val vRaw : WomValue = expr.evaluate(lookup, DxFunctions).get
                        cast(decl.womType, vRaw, decl.unqualifiedName)
                }
                val elem = RtmElem(decl.unqualifiedName, decl.womType, wValue)
                env + (decl.unqualifiedName -> elem)

            case call:WdlCall =>
                val dxExec = execCall(call, env)
                // Embed the execution-id into the call object. This allows
                // referencing the job/analysis results later on.
                val rElem = RtmElem(call.unqualifiedName,
                                    WdlCallOutputsObjectType(call),
                                    WdlCallOutputsObject(call,
                                                         Map("dxExec" -> WomString(dxExec.getId))))
                env + (call.unqualifiedName -> rElem)

            case ssc:Scatter =>
                // Evaluate the collection, and iterate on the inner block N times.
                // This results in N environments that need to be merged.
                //
                // Note: The compiler ensures that the collection is a simple variable.
                val collectionVarName = ssc.collection.toWomString
                val RtmElem(_, womType, value) = env(collectionVarName)
                evalScatter(ssc, env, womType, value)

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

    def apply(wf: WdlWorkflow,
              inputs: Map[String, WdlVarLinks]) : Map[String, WdlVarLinks] = {
        // build the environment from the dx:applet inputs
        val envBgn: Map[String, RtmElem] = inputs.map{
            case (varName, wvl) =>
                val wdlValue = wdlValueFromWVL(wvl)
                val rVar = RtmElem(varName, wvl.womType, wdlValue)
                varName -> rVar
        }.toMap

        // evaluate each of the statements in the workflow
        val envEnd = wf.children.foldLeft(envBgn) {
            case (env, stmt) => evalStatement(stmt, env)
        }

        // Collect output variables, and convert to JSON
        val wvlVarOutputs: Map[String, WdlVarLinks] =
            envEnd.flatMap{
                case (varName, RtmElem(_,womType, value)) if (isExported(varName)) =>
                    // an exported  variable
                    Some(varName -> wdlValueToWVL(womType, value))
                case _ =>
                    None
            }
        wvlVarOutputs
    }
}

object MiniWorkflow {
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


    // Represent unbound optional inputs as None
    private def addUnboundOptionals(wf: WdlWorkflow,
                                    inputSpec: Map[String, DXIOParam],
                                    inputs: Map[String, WdlVarLinks]) : Map[String, WdlVarLinks] = {
        val (topDecls,_) = Utils.splitBlockDeclarations(wf.children.toList)
        inputSpec.map{ case (name, _) =>
            val wvl = inputs.get(name) match {
                case Some(wvl) =>
                    // The argument is already bound
                    wvl
                case None =>
                    // unbound argument
                    val decl = topDecls.find(_.unqualifiedName == name)
                    decl match {
                        case None =>
                            throw new Exception(s"cannot find declaration for input argument ${name}")
                        case Some(d) => d.womType match {
                            case WomOptionalType(t) =>
                                val wdlValue = WomOptionalValue(t, None)
                                WdlVarLinks.importFromWDL(d.womType,
                                                          DeclAttrs.empty,
                                                          wdlValue,
                                                          IODirection.Zero)
                            case _ =>
                                throw new Exception(s"Unbound compulsory argument ${name}")
                        }
                    }
            }
            name -> wvl
        }.toMap
    }

    def apply(wf: WdlWorkflow,
              inputSpec: Map[String, DXIOParam],
              outputSpec: Map[String, DXIOParam],
              inputs: Map[String, WdlVarLinks],
              orgInputs: JsValue,
              collectSubjob: Boolean) : Map[String, JsValue] = {
        Utils.appletLog(s"WomType mapping =${inputSpec}")
        val exportVars = outputSpec.keys.toSet
        Utils.appletLog(s"exportVars=${exportVars}")

        // Get handles for the referenced dx:applets
        val dxEnv = DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()
        val execLinkInfo = loadLinkInfo(dxProject)
        Utils.appletLog(s"link info=${execLinkInfo}")

        // Add unbound arguments that are optional. We can use None as
        // values.
        val allInputs = addUnboundOptionals(wf, inputSpec, inputs)
        Utils.appletLog(s"inputs=${allInputs}")

        // Run the workflow
        val cef = new CompilerErrorFormatter("", wf.wdlSyntaxErrorFormatter.terminalMap)
        val r = MiniWorkflow(execLinkInfo, Some(exportVars), cef, orgInputs, collectSubjob, false)
        val wvlVarOutputs = r.apply(wf, allInputs)

        // convert from WVL to JSON
        val jsVarOutputs: Map[String, JsValue] =
            wvlVarOutputs.foldLeft(Map.empty[String, JsValue]) {
                case (accu, (varName, wvl)) =>
                    val fields = WdlVarLinks.genFields(wvl, varName)
                    accu ++ fields.toMap
            }
        jsVarOutputs
    }
}
