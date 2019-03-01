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

package dxWDL.exec

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import com.dnanexus._
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Paths
import scala.collection.JavaConverters._
import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.expression._
import wom.graph._
import wom.graph.GraphNodePort._
import wom.graph.expression._
import wom.values._
import wom.types._

import dxWDL.util._

case class WfFragRunner(wf: WorkflowDefinition,
                        wfSourceCode: String,
                        instanceTypeDB: InstanceTypeDB,
                        execLinkInfo: Map[String, ExecLinkInfo],
                        dxPathConfig : DxPathConfig,
                        dxIoFunctions : DxIoFunctions,
                        inputsRaw : JsValue,
                        fragInputOutput : WfFragInputOutput,
                        runtimeDebugLevel: Int) {
    private val verbose = runtimeDebugLevel >= 1
    //private val maxVerboseLevel = (runtimeDebugLevel == 2)
    private val collectSubJobs = CollectSubJobs(fragInputOutput.jobInputOutput,
                                                inputsRaw,
                                                instanceTypeDB,
                                                runtimeDebugLevel)

    var gSeqNum = 0
    private def launchSeqNum() : Int = {
        gSeqNum += 1
        gSeqNum
    }

    private def evaluateWomExpression(expr: WomExpression,
                                      womType: WomType,
                                      env: Map[String, WomValue]) : WomValue = {
        val result: ErrorOr[WomValue] =
            expr.evaluateValue(env, dxIoFunctions)
        val value = result match {
            case Invalid(errors) => throw new Exception(
                s"Failed to evaluate expression ${expr} with ${errors}")
            case Valid(x: WomValue) => x
        }

        // cast the result value to the correct type
        // For example, an expression like:
        //   Float x = "3.2"
        // requires casting from string to float
        womType.coerceRawValue(value).get
    }

    private def getCallLinkInfo(call: CallNode) : ExecLinkInfo = {
        val calleeName = call.callable.name
        execLinkInfo.get(calleeName) match {
            case None =>
                throw new AppInternalException(
                    s"Could not find linking information for ${calleeName}")
            case Some(eInfo) => eInfo
        }
    }

    // Figure out what outputs need to be exported.
    private def exportedVarNames() : Set[String] = {
        val dxapp : DXApplet = Utils.dxEnv.getJob().describe().getApplet()
        val desc : DXApplet.Describe = dxapp.describe()
        val outputSpecRaw: List[OutputParameter] = desc.getOutputSpecification().asScala.toList
        val outputNames = outputSpecRaw.map(
            iSpec => iSpec.getName
        )

        // remove auxiliary fields
        outputNames
            .filter( fieldName => !fieldName.endsWith(Utils.FLAT_FILES_SUFFIX))
            .map{ name => Utils.revTransformVarName(name) }
            .toSet
    }

    private def processOutputs(womOutputs: Map[String, WomValue],
                               wvlOutputs: Map[String, WdlVarLinks],
                               exportedVars: Set[String]) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"""|processOutputs
                                     |  exportedVars = ${exportedVars}
                                     |  womOutputs = ${womOutputs.keys}
                                     |  wvlOutputs = ${wvlOutputs.keys}
                                     |""".stripMargin)

        // convert the WOM values to WVLs
        val womWvlOutputs = womOutputs.map{
            case (name, value) =>
                name -> WdlVarLinks.importFromWDL(value.womType, value)
        }.toMap

        // filter anything that should not be exported.
        val exportedWvls = (womWvlOutputs ++ wvlOutputs).filter{
            case (name, wvl) => exportedVars contains name
        }

        // convert from WVL to JSON
        //
        // TODO: check for each variable if it should be output
        exportedWvls.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, wvl)) =>
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
        }.toMap
    }

 /**
      In the workflow below, we want to correctly pass the [k] value
      to each [inc] Task invocation.
    scatter (k in integers) {
        call inc as inc {input: i=k}
   }
   */
    private def buildAppletInputs(call: CallNode,
                                  linkInfo: ExecLinkInfo,
                                  env : Map[String, WomValue]) : JsValue = {
        Utils.appletLog(verbose, s"""|buildAppletInputs (${call.identifier.localName.value})
                                     |env:
                                     |${env.mkString("\n")}
                                     |""".stripMargin)

        val inputs: Map[String, WomValue] = linkInfo.inputs.flatMap{
            case (varName, wdlType) =>
                env.get(varName) match {
                    case None =>
                        // No binding for this input. It might be optional,
                        // it could have a default value. It could also actually be missing,
                        // which will result in a platform error.
                        None
                    case Some(womValue) =>
                        Some(varName -> womValue)
                }
        }

        val wvlInputs = inputs.map{ case (name, womValue) =>
            val womType = linkInfo.inputs(name)
            name -> WdlVarLinks.importFromWDL(womType, womValue)
        }.toMap

        val m = wvlInputs.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, wvl)) =>
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
        }
        JsObject(m)
    }

    private def execCall(call: CallNode,
                         env: Map[String, WomValue],
                         callNameHint: Option[String]) : (Int, DXExecution) = {
        val linkInfo = getCallLinkInfo(call)
        val callName = call.identifier.localName.value
        val calleeName = call.callable.name
        val callInputs:JsValue = buildAppletInputs(call, linkInfo, env)
        Utils.appletLog(verbose, s"""|Call ${callName}
                                     |calleeName= ${calleeName}
                                     |inputs = ${callInputs}""".stripMargin)

        // We may need to run a collect subjob. Add the call
        // name, and the sequence number, to each execution invocation,
        // so the collect subjob will be able to put the
        // results back together.
        val seqNum: Int = launchSeqNum()
        val dbgName = callNameHint match {
            case None => call.identifier.localName.value
            case Some(hint) => s"${callName} ${hint}"
        }
        val dxExecId = linkInfo.dxExec.getId
        val dxExec =
            if (dxExecId.startsWith("app-")) {
                val fields = Map(
                    "name" -> JsString(dbgName),
                    "input" -> callInputs,
                    "properties" -> JsObject("call" ->  JsString(callName),
                                             "seq_number" -> JsString(seqNum.toString))
                )
                val req = JsObject(fields)
                val retval: JsonNode = DXAPI.appRun(dxExecId,
                                                    Utils.jsonNodeOfJsValue(req),
                                                    classOf[JsonNode])
                val info: JsValue =  Utils.jsValueOfJsonNode(retval)
                val id:String = info.asJsObject.fields.get("id") match {
                    case Some(JsString(x)) => x
                    case _ => throw new AppInternalException(
                        s"Bad format returned from jobNew ${info.prettyPrint}")
                }
                DXJob.getInstance(id)
            } else if (dxExecId.startsWith("applet-")) {
                val applet = DXApplet.getInstance(dxExecId)
                val fields = Map(
                    "name" -> JsString(dbgName),
                    "input" -> callInputs,
                    "properties" -> JsObject("call" ->  JsString(callName),
                                             "seq_number" -> JsString(seqNum.toString))
                )
                // TODO: If this is a task that specifies the instance type
                // at runtime, launch it in the requested instance.
                //
                val req = JsObject(fields)
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
                    .putProperty("call", callName)
                    .putProperty("seq_number", seqNum.toString)
                    .run()
                dxAnalysis
            } else {
                throw new Exception(s"Unsupported execution ${linkInfo.dxExec}")
            }
        (seqNum, dxExec)
    }

    // create promises to this call. This allows returning
    // from the parent job immediately.
    private def genPromisesForCall(call: CallNode,
                                   dxExec: DXExecution) : Map[String, WdlVarLinks] = {
        val linkInfo = getCallLinkInfo(call)
        val callName = call.identifier.localName.value
        linkInfo.outputs.map{
            case (varName, womType) =>
                val oName = s"${callName}.${varName}"
                oName -> WdlVarLinks(womType,
                                     DxlExec(dxExec, varName))
        }.toMap
    }

    // This method is exposed so that we can unit-test it.
    def evalExpressions(nodes: Seq[GraphNode],
                        env: Map[String, WomValue],
                        callNode : Option[CallNode]) : Map[String, WomValue] = {
/*        val dbgGraph = nodes.map{node => WomPrettyPrint.apply(node) }.mkString("  \n")
        Utils.appletLog(verbose,
                        s"""|--- evalExpressions
                            |env =
                            |   ${env.mkString("  \n")}
                            |graph =
                            |   ${dbgGraph}
                            |---
                            |""".stripMargin)*/
        nodes.foldLeft(env) {
            // task input expression. The issue here is a mismatch between WDL draft-2 and version 1.0.
            // in an expression like:
            //    call volume { input: i = 10 }
            // the "i" parameter, under WDL draft-2, is compiled as "volume.i"
            // under WDL version 1.0, it is compiled as "i".
            // We just want the "i" component.
            case (env, tcNode : TaskCallInputExpressionNode) if callNode != None =>
                val call = callNode.get
                val localName =
                    if (tcNode.localName.startsWith(call.callable.name + "."))
                        tcNode.localName.substring(call.callable.name.length + 1)
                    else
                        tcNode.localName
                val value : WomValue =
                    evaluateWomExpression(tcNode.womExpression, tcNode.womType, env)
                env + (localName -> value)

            // simple expression
            case (env, eNode: ExpressionNode) =>
                val value : WomValue =
                    evaluateWomExpression(eNode.womExpression, eNode.womType, env)
                env + (eNode.identifier.localName.value -> value)

            // scatter
            case(env, sctNode : ScatterNode) =>
                // WDL has exactly one variable
                assert(sctNode.scatterVariableNodes.size == 1)
                val svNode: ScatterVariableNode = sctNode.scatterVariableNodes.head
                val collectionRaw : WomValue =
                    evaluateWomExpression(svNode.scatterExpressionNode.womExpression,
                                          WomArrayType(svNode.womType),
                                          env)
                val collection : Seq[WomValue] = collectionRaw match {
                    case x: WomArray => x.value
                    case other => throw new AppInternalException(
                        s"Unexpected class ${other.getClass}, ${other}")
                }
                // iterate on the collection
                val vm : Vector[Map[String, WomValue]] =
                    collection.map{ v =>
                        val envInner = env + (svNode.identifier.localName.value -> v)
                        evalExpressions(sctNode.innerGraph.nodes.toSeq, envInner, callNode)
                    }.toVector

                val resultTypes : Map[String, WomArrayType] = sctNode.outputMapping.map{
                    case scp : ScatterGathererPort =>
                        scp.identifier.localName.value -> scp.womType
                }.toMap

                // build a mapping from from result-key to its type
                val initResults : Map[String, (WomType, Vector[WomValue])] = resultTypes.map{
                    case (key, WomArrayType(elemType)) => key -> (elemType, Vector.empty[WomValue])
                    case (_, other) =>
                        throw new AppInternalException(
                            s"Unexpected class ${other.getClass}, ${other}")
                }.toMap

                // merge the vector of results, each of which is a map
                val results : Map[String, (WomType, Vector[WomValue])] =
                    vm.foldLeft(initResults) {
                        case (accu, m) =>
                            accu.map{
                                case (key, (elemType, arValues)) =>
                                    val v : WomValue = m(key)
                                    val vCorrectlyTyped = elemType.coerceRawValue(v).get
                                    key -> (elemType, (arValues :+ vCorrectlyTyped))
                            }.toMap
                    }

                // Add the wom array type to each vector
                results.map{ case (key, (elemType, vv)) =>
                    key -> WomArray(WomArrayType(elemType), vv)
                }

            case (env, cNode: ConditionalNode) =>
                // evaluate the condition
                val condValueRaw : WomValue =
                    evaluateWomExpression(cNode.conditionExpression.womExpression,
                                          WomBooleanType,
                                          env)
                val condValue : Boolean = condValueRaw match {
                    case b: WomBoolean => b.value
                    case other => throw new AppInternalException(
                        s"Unexpected class ${other.getClass}, ${other}")
                }
                // build
                val resultTypes : Map[String, WomType] = cNode.conditionalOutputPorts.map{
                    case cop : ConditionalOutputPort =>
                        cop.identifier.localName.value -> Utils.stripOptional(cop.womType)
                }.toMap
                if (!condValue) {
                    // condition is false, return None for all the values
                    resultTypes.map{ case (key, womType) =>
                        key -> WomOptionalValue(womType, None)
                    }
                } else {
                    // condition is true, evaluate the internal block.
                    val results = evalExpressions(cNode.innerGraph.nodes.toSeq, env, callNode)
                    resultTypes.map{ case (key, womType) =>
                        val value: Option[WomValue] = results.get(key)
                        key -> WomOptionalValue(womType, value)
                    }.toMap
                }

            // Input nodes for a subgraph
            case (env, ogin: OuterGraphInputNode) =>
                //Utils.appletLog(verbose, s"skipping ${ogin.getClass}")
                env

            // Output nodes from a subgraph
            case (env, gon: GraphOutputNode) =>
                //Utils.appletLog(verbose, s"skipping ${gon.getClass}")
                env

            case (env, other) =>
                val dbgGraph = nodes.map{node => WomPrettyPrint.apply(node) }.mkString("\n")
                Utils.appletLog(true, s"""|Error unimplemented type ${other.getClass} while evaluating expressions
                                          |
                                          |env =
                                          |${env}
                                          |
                                          |graph =
                                          |${dbgGraph}
                                          |""".stripMargin)
                throw new Exception(s"${other.getClass} not implemented yet")
        }
    }

    // Get the call from inside the block
    private def getInnerCallFromSimpleBlock(graph: Graph) : CallNode = {
        val calls = graph.nodes.collect {
            case callNode : CallNode => callNode
        }.toVector
        assert(calls.size == 1)
        calls.head
    }

    // Simple conditional subblock. For example:
    //
    //  if (x > 1) {
    //      call Add { input: a=1, b=x+32 }
    //  }
    private def execSimpleConditional(cnNode: ConditionalNode,
                                      env: Map[String, WomValue]) : Map[String, WdlVarLinks] = {
        // Evaluate the condition
        val condValueRaw : WomValue =
            evaluateWomExpression(cnNode.conditionExpression.womExpression,
                                  WomBooleanType,
                                  env)
        val condValue : Boolean = condValueRaw match {
            case b: WomBoolean => b.value
            case other => throw new AppInternalException(
                s"Unexpected class ${other.getClass}, ${other}")
        }
        if (!condValue) {
            // Condition is false, no need to execute the call
            Map.empty
        } else {
            val taskInputs : Vector[TaskCallInputExpressionNode] =
                cnNode.innerGraph.nodes.collect{
                    case tcin : TaskCallInputExpressionNode => tcin
                }.toVector
            val call = getInnerCallFromSimpleBlock(cnNode.innerGraph)

            // evaluate the call inputs, and add to the environment
            val callEnv = evalExpressions(taskInputs, env, Some(call))
            val (_, dxExec) = execCall(call, callEnv,  None)
            val callResults: Map[String, WdlVarLinks] = genPromisesForCall(call, dxExec)

            // Add optional modifier to the return types.
            callResults.map{
                case (key, WdlVarLinks(womType, dxl)) =>
                    // be careful not to make double optionals
                    val optionalType = womType match {
                        case WomOptionalType(_) => womType
                        case _ => WomOptionalType(womType)
                    }
                    key -> WdlVarLinks(optionalType, dxl)
            }.toMap
        }
    }

    // create a short, easy to read, description for a scatter element.
    private def readableNameForScatterItem(item: WomValue) : Option[String] = {
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

    private def execSimpleScatter(sctNode: ScatterNode,
                                  env: Map[String, WomValue]) : Map[String, WdlVarLinks] = {
        // WDL has exactly one variable
        assert(sctNode.scatterVariableNodes.size == 1)
        val svNode: ScatterVariableNode = sctNode.scatterVariableNodes.head
        val collectionRaw : WomValue =
            evaluateWomExpression(svNode.scatterExpressionNode.womExpression,
                                  WomArrayType(svNode.womType),
                                  env)
        val collection : Seq[WomValue] = collectionRaw match {
            case x: WomArray => x.value
            case other => throw new AppInternalException(
                s"Unexpected class ${other.getClass}, ${other}")
        }
        // There must be exactly one call
        val call = getInnerCallFromSimpleBlock(sctNode.innerGraph)

        // We will need to evaluate the task inputs
        val taskInputs : Vector[TaskCallInputExpressionNode] =
            sctNode.innerGraph.nodes.collect{
                case tcin : TaskCallInputExpressionNode => tcin
            }.toVector

        // loop on the collection, call the applet in the inner loop
        val childJobs : Vector[DXExecution] =
            collection.map{ item =>
                val innerEnv = env + (svNode.identifier.localName.value -> item)
                val callInputs = evalExpressions(taskInputs, innerEnv, Some(call))
                val callHint = readableNameForScatterItem(item)
                val (_, dxExec) = execCall(call, callInputs, callHint)
                dxExec
            }.toVector

        // Launch a subjob to collect and marshal the call results.
        // Remove the declarations already calculated
        val resultTypes : Map[String, WomArrayType] = sctNode.outputMapping.map{
            case scp : ScatterGathererPort =>
                scp.identifier.localName.value -> scp.womType
        }.toMap
        collectSubJobs.launch(childJobs, resultTypes)
    }


    def apply(subBlockNr: Int,
              envInitial: Map[String, WomValue],
              runMode: RunnerWfFragmentMode.Value) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
        Utils.appletLog(verbose, s"link info=${execLinkInfo}")
        Utils.appletLog(verbose, s"Environment: ${envInitial}")

        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
        val block = subBlocks(subBlockNr)
        val dbgBlock = block.nodes.map{
            WomPrettyPrintApproxWdl.apply(_)
        }.mkString("\n")
        Utils.appletLog(verbose, s"""|Block ${subBlockNr} to execute:
                                     |${dbgBlock}
                                     |""".stripMargin)

        val (otherNodes, category) = Block.categorize(block)
        val env = evalExpressions(otherNodes, envInitial, None)

        val subblockResults : Map[String, WdlVarLinks] = runMode match {
            case RunnerWfFragmentMode.Launch =>
                // The last node could be a call or a block. All the other nodes
                // are expressions.
                category match {
                    case Block.AllExpressions =>
                        Map.empty

                    // A single call at the end of the block
                    case Block.Call(call: CallNode) =>
                        val (_, dxExec) = execCall(call, env,  None)
                        genPromisesForCall(call, dxExec)

                    // A conditional at the end of the block, with a call inside it
                    case Block.Cond(cnNode, true) =>
                        execSimpleConditional(cnNode, env)

                    // A scatter at the end of the block, with a call inside it
                    case Block.Scatter(sctNode, true) =>
                        execSimpleScatter(sctNode, env)

                    case other =>
                        throw new AppInternalException(s"Unhandled case ${other}")
                }

            // A subjob that collects results from scatters
            case RunnerWfFragmentMode.Collect =>
                val childJobsComplete = collectSubJobs.executableFromSeqNum()
                category match {
                    // A scatter at the end of the block, with a call inside it
                    case Block.Scatter(sctNode, true) =>
                        val call = getInnerCallFromSimpleBlock(sctNode.innerGraph)
                        collectSubJobs.aggregateResults(call, childJobsComplete)

                    case other =>
                        throw new AppInternalException(s"Bad case ${other.getClass} ${other}")
                }
        }

        val exportedVars = exportedVarNames()
        val jsOutputs : Map[String, JsValue] = processOutputs(env, subblockResults, exportedVars)
        val jsOutputsDbgStr = jsOutputs.mkString("\n")
        Utils.appletLog(verbose, s"""|JSON outputs:
                                     |${jsOutputsDbgStr}""".stripMargin)
        jsOutputs
    }
}
