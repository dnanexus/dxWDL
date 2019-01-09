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

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.expression._
import wom.graph._
import wom.graph.expression._
import wom.values._

import dxWDL.util._

case class WfFragRunner(wf: WorkflowDefinition,
                        wfSourceCode: String,
                        instanceTypeDB: InstanceTypeDB,
                        execLinkInfo: Map[String, ExecLinkInfo],
                        dxPathConfig : DxPathConfig,
                        dxIoFunctions : DxIoFunctions,
                        runtimeDebugLevel: Int) {
    private val verbose = runtimeDebugLevel >= 1
    //private val maxVerboseLevel = (runtimeDebugLevel == 2)

    var gSeqNum = 0
    private def launchSeqNum() : Int = {
        gSeqNum += 1
        gSeqNum
    }

    private def evaluateWomExpression(expr: WomExpression, env: Map[String, WomValue]) : WomValue = {
        val result: ErrorOr[WomValue] =
            expr.evaluateValue(env, dxIoFunctions)
        result match {
            case Invalid(errors) => throw new Exception(
                s"Failed to evaluate expression ${expr} with ${errors}")
            case Valid(x: WomValue) => x
        }
    }

    private def processOutputs(wvlOutputs: Map[String, WdlVarLinks],
                               outputNodes: Vector[GraphOutputNode]) : Map[String, JsValue] = {
        val outputNames = outputNodes.map{
            case node =>
                val oPort = node.graphOutputPort
                oPort.name
        }.toSet
        Utils.appletLog(verbose, s"outputNames = ${outputNames}")

        // convert from WVL to JSON
        //
        // TODO: check for each variable if it should be output
        wvlOutputs.foldLeft(Map.empty[String, JsValue]) {
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
                                  env : Env) : (Map[String, WdlVarLinks], JsValue) = {
        def lookup = lookupInEnv(env)(_)
        val inputs: Map[String, WomValue] = linkInfo.inputs.flatMap{
            case (varName, wdlType) =>
                // The rhs is [k], the varName is [i]
                val rhs: Option[(String,WdlExpression)] =
                    call.inputMappings.find{ case(key, expr) => key == varName }
                rhs match {
                    case None =>
                        // No binding for this input. It might be optional,
                        // it could have a default value. It could also actually be missing,
                        // which will result in a platform error.
                        None
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

    private def execCall(call: CallNode,
                         env: Env,
                         callNameHint: Option[String]) : (Int, DXExecution) = {
        val calleeName = call.callable.name
        val callName = call.identifier.localName.value
        val eInfo = execLinkInfo.get(calleeName) match {
            case None =>
                throw new AppInternalException(
                    s"Could not find linking information for ${calleeName}")
            case Some(eInfo) => eInfo
        }
        val callInputs:JsValue = buildAppletInputs(call, eInfo, env)
        Utils.appletLog(verbose, s"Call ${callName}  inputs = ${callInputs}")

        // We may need to run a collect subjob. Add the call
        // name, and the sequence number, to each execution invocation,
        // so the collect subjob will be able to put the
        // results back together.
        val seqNum: Int = launchSeqNum()
        val dbgName = callNameHint match {
            case None => call.identifier.localName.value
            case Some(hint) => s"${callName} ${hint}"
        }
        val dxExecId = eInfo.dxExec.getId
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
                throw new Exception(s"Unsupported execution ${eInfo.dxExec}")
            }
        (seqNum, dxExec)
    }


    def apply(subBlockNr: Int,
              env: Map[String, WomValue]) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
        Utils.appletLog(verbose, s"link info=${execLinkInfo}")
        Utils.appletLog(verbose, s"Workflow source code:")
        Utils.appletLog(verbose, wfSourceCode, 10000)
        Utils.appletLog(verbose, s"Environment: ${env}")

        val graph = wf.innerGraph
        val (inputNodes, subBlocks, outputNodes) = Block.splitIntoBlocks(graph)

        val block = subBlocks(subBlockNr)

        Utils.appletLog(verbose, s"""|Block ${subBlockNr} to execute:
                                     |${block.prettyPrint}
                                     |""".stripMargin)

        // Split the block into expressions to evaluate followed by zero or
        // one calls to another applet/workflow.
        val calls: Vector[CallNode] = block.nodes.collect{
            case x:CallNode => x
        }.toVector
        val otherNodes: Vector[GraphNode] = block.nodes.filter{ x => !x.isInstanceOf[CallNode] }

        val finalEnv : Map[String, WomValue] = otherNodes.foldLeft(env) {
            case (env, node : GraphNode) =>
                node match {
                    case eNode: ExpressionNode =>
                        val value : WomValue = evaluateWomExpression(eNode.womExpression, env)
                        env + (eNode.identifier.workflowLocalName -> value)
                    case other =>
                        throw new Exception(s"${other.getClass} not implemented yet")
                }
        }

        if (calls.isEmpty) {
            // convert the WOM values to WVLs
            val finalWvlEnv = finalEnv.map{ value => WdlVarLinks.importFromWDL(value.womType, value) }
            return processOutputs(finalWvlEnv, outputNodes)
        }
        if (calls.size > 1)
            throw new AppInternalException(
                s"""|There are ${calls.size} calls in the subblock.
                    |There can only be zero or one""".stripMargin.replaceAll("\n", " "))

        // There is a single call, and we need to process it
        val theCall = calls.head


    }
}
