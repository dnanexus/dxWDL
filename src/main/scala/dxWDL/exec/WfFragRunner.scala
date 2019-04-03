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
            case Invalid(errors) =>
                val envDbg = env.map{
                    case (key, value) => s"    ${key} -> ${value.toString}"
                }.mkString("\n")
                throw new Exception(
                    s"""|Failed to evaluate expression ${expr.sourceString}
                        |Errors:
                        |${errors}
                        |
                        |Environment:
                        |${envDbg}
                        |""".stripMargin)
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

    // This method is exposed so that we can unit-test it.
    def evalExpressions(nodes: Seq[GraphNode],
                        env: Map[String, WomValue]) : Map[String, WomValue] = {
        /*if (verbose) {
            val dbgGraph = nodes.map{node => WomPrettyPrint.apply(node) }.mkString("  \n")
        Utils.trace(verbose,
                    s"""|--- evalExpressions
                        |env =
                        |   ${env.mkString("  \n")}
                        |graph =
                        |   ${dbgGraph}
                        |---
                        |""".stripMargin) */
        nodes.foldLeft(env) {
            // simple expression
            case (env, eNode: ExposedExpressionNode) =>
                val value : WomValue =
                    evaluateWomExpression(eNode.womExpression, eNode.womType, env)
                env + (eNode.identifier.localName.value -> value)

            case (env, _ : ExpressionNode) =>
                // create an ephemeral expression, not visible in the environment
                env

            // scatter
            // scatter (K in collection) {
            // }
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
                val iterVarName = svNode.identifier.localName.value
                val vm : Vector[Map[String, WomValue]] =
                    collection.map{ v =>
                        val envInner = env + (iterVarName -> v)
                        evalExpressions(sctNode.innerGraph.nodes.toSeq, envInner)
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
                val fullResults = results.map{ case (key, (elemType, vv)) =>
                    key -> WomArray(WomArrayType(elemType), vv)
                }
                env ++ fullResults

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
                val fullResults : Map[String, WomValue] =
                    if (!condValue) {
                        // condition is false, return None for all the values
                        resultTypes.map{ case (key, womType) =>
                            key -> WomOptionalValue(womType, None)
                        }
                    } else {
                        // condition is true, evaluate the internal block.
                        val results = evalExpressions(cNode.innerGraph.nodes.toSeq, env)
                        resultTypes.map{ case (key, womType) =>
                            val value: Option[WomValue] = results.get(key)
                            key -> WomOptionalValue(womType, value)
                        }
                    }
                env ++ fullResults

            // Input nodes for a subgraph
            case (env, ogin: OuterGraphInputNode) =>
                //Utils.appletLog(verbose, s"skipping ${ogin.getClass}")
                env

            // Output nodes from a subgraph
            case (env, gon: GraphOutputNode) =>
                //Utils.appletLog(verbose, s"skipping ${gon.getClass}")
                env

            case (env, other: CallNode) =>
                throw new Exception(s"calls (${other}) cannot be evaluated as expressions")

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

    private def processOutputs(env: Map[String, WomValue],
                               fragResults: Map[String, WdlVarLinks],
                               exportedVars: Set[String]) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"""|processOutputs
                                     |  env = ${env.keys}
                                     |  fragResults = ${fragResults.keys}
                                     |  exportedVars = ${exportedVars}
                                     |""".stripMargin)

        // convert the WOM values to WVLs
        val envWvl = env.map{
            case (name, value) =>
                name -> WdlVarLinks.importFromWDL(value.womType, value)
        }.toMap

        // filter anything that should not be exported.
        val exportedWvls = (envWvl ++ fragResults).filter{
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
    private def buildCallInputs(callName: String,
                                linkInfo: ExecLinkInfo,
                                env : Map[String, WomValue]) : JsValue = {
        Utils.appletLog(verbose, s"""|buildCallInputs (${callName})
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

    private def execDNAxExecutable(dxExecId: String,
                                   dbgName: String,
                                   callName: String,
                                   callInputs : JsValue) : (Int, DXExecution) = {
        val seqNum: Int = launchSeqNum()
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
                throw new Exception(s"Unsupported execution ${dxExecId}")
            }
        (seqNum, dxExec)
    }

    private def execCall(call: CallNode,
                         callInputs: Map[String, WomValue],
                         callNameHint: Option[String]) : (Int, DXExecution) = {
        val linkInfo = getCallLinkInfo(call)
        val callName = call.identifier.localName.value
        //val calleeName = call.callable.name
        val callInputsJSON : JsValue = buildCallInputs(callName, linkInfo, callInputs)
/*        Utils.appletLog(verbose, s"""|Call ${callName}
                                     |calleeName= ${calleeName}
                                     |inputs = ${callInputsJSON}""".stripMargin)*/

        // We may need to run a collect subjob. Add the call
        // name, and the sequence number, to each execution invocation,
        // so the collect subjob will be able to put the
        // results back together.
        val dbgName = callNameHint match {
            case None => call.identifier.localName.value
            case Some(hint) => s"${callName} ${hint}"
        }
        execDNAxExecutable(linkInfo.dxExec.getId, dbgName, callName, callInputsJSON)
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

    // task input expression. The issue here is a mismatch between WDL draft-2 and version 1.0.
    // in an expression like:
    //    call volume { input: i = 10 }
    // the "i" parameter, under WDL draft-2, is compiled as "volume.i"
    // under WDL version 1.0, it is compiled as "i".
    // We just want the "i" component.
    def evalCallInputs(call: CallNode,
                       env: Map[String, WomValue]) : Map[String, WomValue] = {
        // Find the type required for a call input
        def findWomType(paramName: String) : WomType = {
            val retval = call.inputDefinitionMappings.find{
                case (iDef, iDefPtr) =>
                    (iDef.localName.value == paramName) ||
                        (Utils.getUnqualifiedName(iDef.localName.value) == paramName)
            }
            retval match {
                case None => throw new Exception(s"Could not find ${paramName}")
                case Some((iDef, iDefPtr)) =>
                    iDef.womType
            }
        }
        call.upstream.collect{
            case exprNode: ExpressionNode =>
                val paramName = Utils.getUnqualifiedName(exprNode.identifier.localName.value)
                val expression = exprNode.womExpression
                val womType = findWomType(paramName)
                paramName -> evaluateWomExpression(expression, womType, env)
        }.toMap
    }

    // Evaluate the condition
    private def evalCondition(cnNode: ConditionalNode,
                              env: Map[String, WomValue]) : Boolean = {
        val condValueRaw : WomValue =
            evaluateWomExpression(cnNode.conditionExpression.womExpression,
                                  WomBooleanType,
                                  env)
        val condValue : Boolean = condValueRaw match {
            case b: WomBoolean => b.value
            case other => throw new AppInternalException(
                s"Unexpected class ${other.getClass}, ${other}")
        }
        condValue
    }

    // A subblock containing exactly one call.
    // For example:
    //
    // if (flag) {
    //   call zinc as inc3 { input: a = num}
    // }
    //
    private def execConditionalCall(cnNode: ConditionalNode,
                                    call: CallNode,
                                    env: Map[String, WomValue]) : Map[String, WdlVarLinks] = {
        if (!evalCondition(cnNode, env)) {
            // Condition is false, no need to execute the call
            Map.empty
        } else {
            // evaluate the call inputs, and add to the environment
            val callInputs = evalCallInputs(call, env)
            val (_, dxExec) = execCall(call, callInputs,  None)
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

    // A complex subblock requiring a fragment runner, or a subworkflow
    // For example:
    //
    // if (flag) {
    //   call zinc as inc3 { input: a = num}
    //   call zinc as inc4 { input: a = num + 3 }
    //
    //   Int b = inc4.result + 14
    //   call zinc as inc5 { input: a = b * 4 }
    // }
    //
    private def execConditionalSubblock(cnNode: ConditionalNode,
                                        env: Map[String, WomValue]) : Map[String, WdlVarLinks] = {
        if (!evalCondition(cnNode, env)) {
            // Condition is false, no need to execute the call
            Map.empty
        } else {
            // There must be exactly one sub-workflow
            assert(execLinkInfo.size == 1)
            val (_, linkInfo) = execLinkInfo.toVector.head

            // The subblock is complex, and requires a fragment, or a subworkflow
            val callInputs:JsValue = buildCallInputs(linkInfo.name, linkInfo, env)
            val (_, dxExec) = execDNAxExecutable(linkInfo.dxExec.getId, linkInfo.name, linkInfo.name, callInputs)

            // create promises for results
            linkInfo.outputs.map{
                case (varName, womType) =>
                    // Add optional modifier to the return types.
                    // be careful not to make double optionals
                    val optionalType = womType match {
                        case WomOptionalType(_) => womType
                        case _ => WomOptionalType(womType)
                    }
                    varName -> WdlVarLinks(optionalType,
                                           DxlExec(dxExec, varName))
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
            case WomArray(_, arrValues) =>
                val arrBeginning = arrValues.slice(0, 3)
                val elements = arrBeginning.flatMap(readableNameForScatterItem(_))
                Some("[" + elements.mkString(", ") + "]")
            case _ =>
                None
        }
    }

    // Evaluate the collection on which we are scattering
    private def evalScatterCollection(sctNode: ScatterNode,
                                      env: Map[String, WomValue])
            : (ScatterVariableNode, Seq[WomValue]) = {
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
        (svNode, collection)
    }


    // Launch a subjob to collect and marshal the call results.
    // Remove the declarations already calculated
    private def collectScatter(sctNode: ScatterNode,
                               childJobs : Vector[DXExecution]) : Map[String, WdlVarLinks] = {
        val resultTypes : Map[String, WomArrayType] = sctNode.outputMapping.map{
            case scp : ScatterGathererPort =>
                scp.identifier.localName.value -> scp.womType
        }.toMap
        val promises = collectSubJobs.launch(childJobs, resultTypes)
        val promisesStr = promises.mkString("\n")

        Utils.appletLog(verbose, s"resultTypes=${resultTypes}")
        Utils.appletLog(verbose, s"promises=${promisesStr}")
        promises
    }

    private def execScatterCall(sctNode: ScatterNode,
                                call: CallNode,
                                env: Map[String, WomValue]) : Map[String, WdlVarLinks] = {
        val (svNode, collection) = evalScatterCollection(sctNode, env)

        // loop on the collection, call the applet in the inner loop
        val childJobs : Vector[DXExecution] =
            collection.map{ item =>
                val innerEnv = env + (svNode.identifier.localName.value -> item)
                val callInputs = evalCallInputs(call, innerEnv)
                val callHint = readableNameForScatterItem(item)
                val (_, dxJob) = execCall(call, callInputs, callHint)
                dxJob
            }.toVector

        collectScatter(sctNode, childJobs)
    }

    private def execScatterSubblock(sctNode: ScatterNode,
                                    env: Map[String, WomValue]) : Map[String, WdlVarLinks] = {
        val (svNode, collection) = evalScatterCollection(sctNode, env)

        // There must be exactly one sub-workflow
        assert(execLinkInfo.size == 1)
        val (_, linkInfo) = execLinkInfo.toVector.head

        // loop on the collection, call the applet in the inner loop
        val childJobs : Vector[DXExecution] =
            collection.map{ item =>
                val innerEnv = env + (svNode.identifier.localName.value -> item)
                val callHint = readableNameForScatterItem(item)
                val dbgName = callHint  match {
                    case None => linkInfo.name
                    case Some(hint) => s"${linkInfo.name} ${hint}"
                }

                // The subblock is complex, and requires a fragment, or a subworkflow
                val callInputs:JsValue = buildCallInputs(linkInfo.name, linkInfo, innerEnv)
                val (_, dxJob) = execDNAxExecutable(linkInfo.dxExec.getId, dbgName, linkInfo.name, callInputs)
                dxJob
            }.toVector

        collectScatter(sctNode, childJobs)
    }

    // Does the the graph contain exactly one call? If so, return it
    private def graphContainsJustOneCall(graph: Graph) : Option[CallNode] = {
        val (_, blocks, _) = Block.split(graph, wfSourceCode)
        if (blocks.size != 1)
            return None

        Block.categorize(blocks(0)) match {
            case Block.CallDirect(_, cNode)  => Some(cNode)
            case Block.CallCompound(_, cNode) => Some(cNode)
            case _ => None
        }
    }

    def apply(blockPath: Vector[Int],
              envInitial: Map[String, WomValue],
              runMode: RunnerWfFragmentMode.Value) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
        Utils.appletLog(verbose, s"link info=${execLinkInfo}")
        Utils.appletLog(verbose, s"Environment: ${envInitial}")

        // sort from low to high according to the source lines.
        val callToSrcLine = ParseWomSourceFile.scanForCalls(wfSourceCode)
        val callsLoToHi : Vector[(String, Int)] = callToSrcLine.toVector.sortBy(_._2)

        // Find the fragment block to execute
        val block = Block.getSubBlock(blockPath, wf.innerGraph, callsLoToHi)
        val dbgBlock = block.nodes.map{
            WomPrettyPrintApproxWdl.apply(_)
        }.mkString("\n")
        Utils.appletLog(verbose, s"""|Block ${blockPath} to execute:
                                     |${dbgBlock}
                                     |
                                     |""".stripMargin)

        val catg = Block.categorize(block)
        val env = evalExpressions(catg.nodes, envInitial)

        val fragResults : Map[String, WdlVarLinks] = runMode match {
            case RunnerWfFragmentMode.Launch =>
                // The last node could be a call or a block. All the other nodes
                // are expressions.
                catg match {
                    case Block.AllExpressions(_) =>
                        Map.empty

                    case Block.CallDirect(_,_) =>
                        throw new Exception("sanity, shouldn't reach this state")

                    // A single call at the end of the block
                    case Block.CallCompound(_, call: CallNode) =>
                        val callInputs = evalCallInputs(call, env)
                        val (_, dxExec) = execCall(call, callInputs,  None)
                        genPromisesForCall(call, dxExec)

                    // a conditional with a subblock inside it. We may need to
                    // call a subworkflow.
                    case Block.Cond(_, cnNode) =>
                        graphContainsJustOneCall(cnNode.innerGraph) match {
                            case None =>
                                // subworkflow or fragment
                                execConditionalSubblock(cnNode, env)
                            case Some(call) =>
                                // The block contains a single call. We can execute it
                                // right here, without another job.
                                execConditionalCall(cnNode, call, env)
                        }

                    // a scatter with a subblock inside it. Iterate
                    // on the scatter variable, and call an applet/subworkflow
                    // for each value.
                    case Block.Scatter(_, sctNode) =>
                        graphContainsJustOneCall(sctNode.innerGraph) match {
                            case None =>
                                // subworkflow or fragment
                                execScatterSubblock(sctNode, env)
                            case Some(call) =>
                                // The block contains a single call. We can execute it
                                // right here, without another job.
                                execScatterCall(sctNode, call, env)
                        }
                }

            // A subjob that collects results from scatters
            case RunnerWfFragmentMode.Collect =>
                val childJobsComplete = collectSubJobs.executableFromSeqNum()
                val sctNode = catg match {
                    case Block.Scatter(_, sctNode) => sctNode
                    case other =>
                        throw new AppInternalException(s"Bad case ${other.getClass} ${other}")
                }

                graphContainsJustOneCall(sctNode.innerGraph) match {
                    case None =>
                        // A scatter with a complex sub-block, compiled as a sub-workflow
                        // There must be exactly one sub-workflow
                        assert(execLinkInfo.size == 1)
                        val (_, linkInfo) = execLinkInfo.toVector.head
                        collectSubJobs.aggregateResultsFromGeneratedSubWorkflow(linkInfo, childJobsComplete)
                    case Some(call) =>
                        // scatter with a single call
                        collectSubJobs.aggregateResults(call, childJobsComplete)
                }
        }

        val exportedVars = exportedVarNames()
        val jsOutputs : Map[String, JsValue] = processOutputs(env, fragResults, exportedVars)
        val jsOutputsDbgStr = jsOutputs.mkString("\n")
        Utils.appletLog(verbose, s"""|JSON outputs:
                                     |${jsOutputsDbgStr}""".stripMargin)
        jsOutputs
    }
}
