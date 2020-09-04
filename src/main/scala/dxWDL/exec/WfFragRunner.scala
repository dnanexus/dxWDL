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
import java.nio.file.Paths

import spray.json._
import wom.callable.{CallableTaskDefinition, WorkflowDefinition}
import wom.callable.Callable._
import wom.expression._
import wom.graph._
import wom.graph.GraphNodePort._
import wom.graph.expression._
import wom.values._
import wom.types._
import dxWDL.base._
import dxWDL.dx._
import dxWDL.util._

case class WfFragRunner(wf: WorkflowDefinition,
                        taskDir: Map[String, CallableTaskDefinition],
                        typeAliases: Map[String, WomType],
                        wfSourceCode: String,
                        instanceTypeDB: InstanceTypeDB,
                        execLinkInfo: Map[String, ExecLinkInfo],
                        dxPathConfig: DxPathConfig,
                        dxIoFunctions: DxIoFunctions,
                        inputsRaw: JsValue,
                        fragInputOutput: WfFragInputOutput,
                        jobDesc: DxJobDescribe,
                        defaultRuntimeAttributes: Option[WdlRuntimeAttrs],
                        delayWorkspaceDestruction: Option[Boolean],
                        runtimeDebugLevel: Int,
                        scatterStart: Int = 0,
                        jobsPerScatter: Int = Utils.DEFAULT_JOBS_PER_SCATTER) {
  private val MAX_JOB_NAME = 50
  private val verbose = runtimeDebugLevel >= 1
  //private val maxVerboseLevel = (runtimeDebugLevel == 2)
  private val utlVerbose = Verbose(runtimeDebugLevel >= 1, false, Set.empty)
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(utlVerbose, dxIoFunctions.fileInfoDir, fragInputOutput.typeAliases)
  private val jobInputOutput = fragInputOutput.jobInputOutput
  private val collectSubJobs = CollectSubJobs(jobInputOutput,
                                              inputsRaw,
                                              instanceTypeDB,
                                              delayWorkspaceDestruction,
                                              runtimeDebugLevel,
                                              fragInputOutput.typeAliases,
                                              jobDesc)
  // The source code for all the tasks
  private val taskSourceDir: Map[String, String] =
    ParseWomSourceFile(verbose).scanForTasks(wfSourceCode)

  var gSeqNum = 0
  private def launchSeqNum(): Int = {
    gSeqNum += 1
    gSeqNum
  }

  private def evaluateWomExpression(expr: WomExpression,
                                    womType: WomType,
                                    env: Map[String, WomValue]): WomValue = {
    val result: ErrorOr[WomValue] =
      expr.evaluateValue(env, dxIoFunctions)
    val value = result match {
      case Invalid(errors) =>
        val envDbg = env
          .map {
            case (key, value) => s"    ${key} -> ${value.toString}"
          }
          .mkString("\n")
        throw new Exception(s"""|Failed to evaluate expression ${expr.sourceString}
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

  private def getCallLinkInfo(call: CallNode): ExecLinkInfo = {
    val calleeName = call.callable.name
    execLinkInfo.get(calleeName) match {
      case None =>
        throw new AppInternalException(s"Could not find linking information for ${calleeName}")
      case Some(eInfo) => eInfo
    }
  }

  // This method is exposed so that we can unit-test it.
  def evalExpressions(nodes: Seq[GraphNode], env: Map[String, WomValue]): Map[String, WomValue] = {
    val partialOrderNodes = Block.partialSortByDep(nodes.toSet)
    partialOrderNodes.foldLeft(env) {
      // simple expression
      case (env, eNode: ExposedExpressionNode) =>
        val value: WomValue =
          evaluateWomExpression(eNode.womExpression, eNode.womType, env)
        env + (eNode.identifier.localName.value -> value)

      case (env, _: ExpressionNode) =>
        // create an ephemeral expression, not visible in the environment
        env

      // scatter
      // scatter (K in collection) {
      // }
      case (env, sctNode: ScatterNode) =>
        // WDL has exactly one variable
        assert(sctNode.scatterVariableNodes.size == 1)
        val svNode: ScatterVariableNode = sctNode.scatterVariableNodes.head
        val collectionRaw: WomValue =
          evaluateWomExpression(svNode.scatterExpressionNode.womExpression,
                                WomArrayType(svNode.womType),
                                env)
        val collection: Seq[WomValue] = collectionRaw match {
          case x: WomArray => x.value
          case other =>
            throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
        }

        // iterate on the collection
        val iterVarName = svNode.identifier.localName.value
        val vm: Vector[Map[String, WomValue]] =
          collection.map { v =>
            val envInner = env + (iterVarName -> v)
            evalExpressions(sctNode.innerGraph.nodes.toSeq, envInner)
          }.toVector

        val resultTypes: Map[String, WomArrayType] = sctNode.outputMapping.map {
          case scp: ScatterGathererPort =>
            scp.identifier.localName.value -> scp.womType
        }.toMap

        // build a mapping from from result-key to its type
        val initResults: Map[String, (WomType, Vector[WomValue])] = resultTypes.map {
          case (key, WomArrayType(elemType)) => key -> (elemType, Vector.empty[WomValue])
          case (_, other) =>
            throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
        }.toMap

        // merge the vector of results, each of which is a map
        val results: Map[String, (WomType, Vector[WomValue])] =
          vm.foldLeft(initResults) {
            case (accu, m) =>
              accu.map {
                case (key, (elemType, arValues)) =>
                  val v: WomValue = m(key)
                  val vCorrectlyTyped = elemType.coerceRawValue(v).get
                  key -> (elemType, (arValues :+ vCorrectlyTyped))
              }.toMap
          }

        // Add the wom array type to each vector
        val fullResults = results.map {
          case (key, (elemType, vv)) =>
            key -> WomArray(WomArrayType(elemType), vv)
        }
        env ++ fullResults

      case (env, cNode: ConditionalNode) =>
        // evaluate the condition
        val condValueRaw: WomValue =
          evaluateWomExpression(cNode.conditionExpression.womExpression, WomBooleanType, env)
        val condValue: Boolean = condValueRaw match {
          case b: WomBoolean => b.value
          case other =>
            throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
        }
        // build
        val resultTypes: Map[String, WomType] = cNode.conditionalOutputPorts.map {
          case cop: ConditionalOutputPort =>
            cop.identifier.localName.value -> Utils.stripOptional(cop.womType)
        }.toMap
        val fullResults: Map[String, WomValue] =
          if (!condValue) {
            // condition is false, return None for all the values
            resultTypes.map {
              case (key, womType) =>
                key -> WomOptionalValue(womType, None)
            }
          } else {
            // condition is true, evaluate the internal block.
            val results = evalExpressions(cNode.innerGraph.nodes.toSeq, env)
            resultTypes.map {
              case (key, womType) =>
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
        val dbgGraph = nodes
          .map { node =>
            WomPrettyPrint.apply(node)
          }
          .mkString("\n")
        Utils.appletLog(
            true,
            s"""|Error unimplemented type ${other.getClass} while evaluating expressions
                |
                |env =
                |${env}
                |
                |graph =
                |${dbgGraph}
                |""".stripMargin
        )
        throw new Exception(s"${other.getClass} not implemented yet")
    }
  }

  private def processOutputs(env: Map[String, WomValue],
                             fragResults: Map[String, WdlVarLinks],
                             exportedVars: Set[String]): Map[String, JsValue] = {
    Utils.appletLog(
        verbose,
        s"""|processOutputs
            |  env = ${env.keys}
            |  fragResults = ${fragResults.keys}
            |  exportedVars = ${exportedVars}
            |""".stripMargin
    )

    // convert the WOM values to WVLs
    val envWvl = env.map {
      case (name, value) =>
        name -> wdlVarLinksConverter.importFromWDL(value.womType, value)
    }.toMap

    // filter anything that should not be exported.
    val exportedWvls = (envWvl ++ fragResults).filter {
      case (name, wvl) => exportedVars contains name
    }

    // convert from WVL to JSON
    //
    // TODO: check for each variable if it should be output
    exportedWvls
      .foldLeft(Map.empty[String, JsValue]) {
        case (accu, (varName, wvl)) =>
          val fields = wdlVarLinksConverter.genFields(wvl, varName)
          accu ++ fields.toMap
      }
      .toMap
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
                              env: Map[String, WomValue]): JsValue = {
    Utils.appletLog(
        verbose,
        s"""|buildCallInputs (${callName})
            |env:
            |${env.mkString("\n")}
            |""".stripMargin
    )

    val inputs: Map[String, WomValue] = linkInfo.inputs.flatMap {
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

    val wvlInputs = inputs.map {
      case (name, womValue) =>
        val womType = linkInfo.inputs(name)
        name -> wdlVarLinksConverter.importFromWDL(womType, womValue)
    }.toMap

    val m = wvlInputs.foldLeft(Map.empty[String, JsValue]) {
      case (accu, (varName, wvl)) =>
        val fields = wdlVarLinksConverter.genFields(wvl, varName)
        accu ++ fields.toMap
    }
    JsObject(m)
  }

  // Figure out what instance type to launch at task in. Return None if the instance
  // type is a constant, and is already set.
  private def preCalcInstanceType(task: CallableTaskDefinition,
                                  taskSourceCode: String,
                                  taskInputs: Map[InputDefinition, WomValue]): Option[String] = {
    // Note: if none of these attributes are specified, the return value is None.
    val instanceAttrs = Set("memory", "disks", "cpu")
    val allConst = instanceAttrs.forall { attrName =>
      task.runtimeAttributes.attributes.get(attrName) match {
        case None       => true
        case Some(expr) => WomValueAnalysis.isExpressionConst(WomStringType, expr)
      }
    }
    if (allConst)
      return None

    // There is runtime evaluation for the instance type
    val taskRunner = new TaskRunner(task,
                                    taskSourceCode,
                                    typeAliases,
                                    instanceTypeDB,
                                    dxPathConfig,
                                    dxIoFunctions,
                                    jobInputOutput,
                                    defaultRuntimeAttributes,
                                    delayWorkspaceDestruction,
                                    runtimeDebugLevel)
    try {
      val iType = taskRunner.calcInstanceType(taskInputs)
      Utils.appletLog(verbose, s"Precalculated instance type for ${task.unqualifiedName}: ${iType}")
      Some(iType)
    } catch {
      case e: Throwable =>
        Utils.appletLog(
            verbose,
            s"""|Failed to precalculate the instance type for
                |task ${task.unqualifiedName}.
                |
                |${e}
                |""".stripMargin
        )
        None
    }
  }

  private def execDNAxExecutable(dxExecId: String,
                                 dbgName: String,
                                 callInputs: JsValue,
                                 instanceType: Option[String]): (Int, DxExecution) = {
    // We may need to run a collect subjob. Add the the sequence
    // number to each invocation, so the collect subjob will be
    // able to put the results back together in the correct order.
    val seqNum: Int = launchSeqNum()

    // If this is a task that specifies the instance type
    // at runtime, launch it in the requested instance.
    val dxExec =
      if (dxExecId.startsWith("app-")) {
        val applet = DxApp.getInstance(dxExecId)
        val dxJob = applet.newRun(
            dbgName,
            callInputs,
            instanceType = instanceType,
            properties = Map("seq_number" -> JsString(seqNum.toString)),
            delayWorkspaceDestruction = delayWorkspaceDestruction
        )
        dxJob
      } else if (dxExecId.startsWith("applet-")) {
        val applet = DxApplet.getInstance(dxExecId)
        val dxJob = applet.newRun(
            dbgName,
            callInputs,
            instanceType = instanceType,
            properties = Map("seq_number" -> JsString(seqNum.toString)),
            delayWorkspaceDestruction = delayWorkspaceDestruction
        )
        dxJob
      } else if (dxExecId.startsWith("workflow-")) {
        val workflow = DxWorkflow.getInstance(dxExecId)
        val dxAnalysis: DxAnalysis = workflow.newRun(input = callInputs,
                                                     name = dbgName,
                                                     delayWorkspaceDestruction =
                                                       delayWorkspaceDestruction)
        val props = Map("seq_number" -> seqNum.toString)
        dxAnalysis.setProperties(props)
        dxAnalysis
      } else {
        throw new Exception(s"Unsupported execution ${dxExecId}")
      }
    (seqNum, dxExec)
  }

  private def execCall(call: CallNode,
                       callInputs: Map[String, WomValue],
                       callNameHint: Option[String]): (Int, DxExecution) = {
    val linkInfo = getCallLinkInfo(call)
    val callName = call.identifier.localName.value
    val calleeName = call.callable.name
    val callInputsJSON: JsValue = buildCallInputs(callName, linkInfo, callInputs)
    /*        Utils.appletLog(verbose, s"""|Call ${callName}
                                     |calleeName= ${calleeName}
                                     |inputs = ${callInputsJSON}""".stripMargin)*/

    // This is presented in the UI, to inform the user
    val dbgName = callNameHint match {
      case None       => call.identifier.localName.value
      case Some(hint) => s"${callName} ${hint}"
    }

    // If this is a call to a task that computes the required instance type at runtime,
    // do the calculation right now. This saves a job relaunch down the road.
    val instanceType: Option[String] =
      (taskDir.get(calleeName), taskSourceDir.get(calleeName)) match {
        case (Some(task), Some(taskSourceCode)) =>
          val taskInputs = jobInputOutput.loadInputs(callInputsJSON, task)
          preCalcInstanceType(task, taskSourceCode, taskInputs)
        case (_, _) => None
      }
    execDNAxExecutable(linkInfo.dxExec.getId, dbgName, callInputsJSON, instanceType)
  }

  // create promises to this call. This allows returning
  // from the parent job immediately.
  private def genPromisesForCall(call: CallNode, dxExec: DxExecution): Map[String, WdlVarLinks] = {
    val linkInfo = getCallLinkInfo(call)
    val callName = call.identifier.localName.value
    linkInfo.outputs.map {
      case (varName, womType) =>
        val oName = s"${callName}.${varName}"
        oName -> WdlVarLinks(womType, DxlExec(dxExec, varName))
    }.toMap
  }

  // task input expression. The issue here is a mismatch between WDL draft-2 and version 1.0.
  // in an expression like:
  //    call volume { input: i = 10 }
  // the "i" parameter, under WDL draft-2, is compiled as "volume.i"
  // under WDL version 1.0, it is compiled as "i".
  // We just want the "i" component.
  def evalCallInputs(call: CallNode, env: Map[String, WomValue]): Map[String, WomValue] = {
    // Find the type required for a call input
    def findWomType(paramName: String): WomType = {
      val retval = call.inputDefinitionMappings.find {
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
    call.upstream.collect {
      case exprNode: ExpressionNode =>
        val paramName = Utils.getUnqualifiedName(exprNode.identifier.localName.value)
        val expression = exprNode.womExpression
        val womType = findWomType(paramName)
        paramName -> evaluateWomExpression(expression, womType, env)
    }.toMap
  }

  // Evaluate the condition
  private def evalCondition(cnNode: ConditionalNode, env: Map[String, WomValue]): Boolean = {
    val condValueRaw: WomValue =
      evaluateWomExpression(cnNode.conditionExpression.womExpression, WomBooleanType, env)
    val condValue: Boolean = condValueRaw match {
      case b: WomBoolean => b.value
      case other         => throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
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
                                  env: Map[String, WomValue]): Map[String, WdlVarLinks] = {
    if (!evalCondition(cnNode, env)) {
      // Condition is false, no need to execute the call
      Map.empty
    } else {
      // evaluate the call inputs, and add to the environment
      val callInputs = evalCallInputs(call, env)
      val (_, dxExec) = execCall(call, callInputs, None)
      val callResults: Map[String, WdlVarLinks] = genPromisesForCall(call, dxExec)

      // Add optional modifier to the return types.
      callResults.map {
        case (key, WdlVarLinks(womType, dxl)) =>
          // be careful not to make double optionals
          val optionalType = womType match {
            case WomOptionalType(_) => womType
            case _                  => WomOptionalType(womType)
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
                                      env: Map[String, WomValue]): Map[String, WdlVarLinks] = {
    if (!evalCondition(cnNode, env)) {
      // Condition is false, no need to execute the call
      Map.empty
    } else {
      // There must be exactly one sub-workflow
      assert(execLinkInfo.size == 1)
      val (_, linkInfo) = execLinkInfo.toVector.head

      // The subblock is complex, and requires a fragment, or a subworkflow
      val callInputs: JsValue = buildCallInputs(linkInfo.name, linkInfo, env)
      val (_, dxExec) = execDNAxExecutable(linkInfo.dxExec.getId, linkInfo.name, callInputs, None)

      // create promises for results
      linkInfo.outputs.map {
        case (varName, womType) =>
          // Add optional modifier to the return types.
          // be careful not to make double optionals
          val optionalType = womType match {
            case WomOptionalType(_) => womType
            case _                  => WomOptionalType(womType)
          }
          varName -> WdlVarLinks(optionalType, DxlExec(dxExec, varName))
      }.toMap
    }
  }

  // create a short, easy to read, description for a scatter element.
  private def readableNameForScatterItem(item: WomValue): Option[String] = {
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
          case _                      => None
        }
      case WomOptionalValue(_, Some(x)) =>
        readableNameForScatterItem(x)
      case WomArray(_, arrValues) =>
        // Create a name by concatenating the initial elements of the array.
        // Limit the total size of the name.
        val arrBeginning = arrValues.slice(0, 3)
        val elements = arrBeginning.flatMap(readableNameForScatterItem(_))
        Some(Utils.buildLimitedSizeName(elements, MAX_JOB_NAME))
      case _ =>
        None
    }
  }

  // Evaluate the collection on which we are scattering
  private def evalScatterCollection(
      sctNode: ScatterNode,
      env: Map[String, WomValue]
  ): (ScatterVariableNode, Seq[WomValue], Option[Int]) = {
    // WDL has exactly one variable
    assert(sctNode.scatterVariableNodes.size == 1)
    val svNode: ScatterVariableNode = sctNode.scatterVariableNodes.head
    val collectionRaw: WomValue =
      evaluateWomExpression(svNode.scatterExpressionNode.womExpression,
                            WomArrayType(svNode.womType),
                            env)
    val (collection: Seq[WomValue], next: Option[Int]) = collectionRaw match {
      case x: WomArray if scatterStart == 0 && x.size <= jobsPerScatter =>
        (x.value, None)
      case x: WomArray =>
        val array = x.value
        val scatterEnd = scatterStart + jobsPerScatter
        if (scatterEnd < array.size) {
          (array.slice(scatterStart, scatterEnd), Some(scatterEnd))
        } else {
          (array.drop(scatterStart), None)
        }
      case other =>
        throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
    }
    (svNode, collection, next)
  }

  // A scatter may contain many sub-jobs. Rather than enforce a maximum number of scatter sub-jobs,
  // we instead chain scatters so that any number of sub-jobs can be executed with a maximum number
  // running at one time. For example:
  //
  // ```scatter (i in range(2000)) { ... }```
  //
  // translates to:
  //
  // scatter(1-1000, jobId=A, parents=[])
  // |_exec job 1..1000
  // |_exec scatter(1001..2000, jobId=B, parents=[A]) // does not run until jobs 1-1000 are complete
  //        |_exec job 1001..2000
  //        |_exec collect(parents=[A,B]) // does not run until jobs 1001-2000 are complete
  private def continueScatter(sctNode: ScatterNode,
                              childJobs: Vector[DxExecution],
                              next: Int): Map[String, WdlVarLinks] = {
    val resultTypes: Map[String, WomArrayType] = sctNode.outputMapping.map {
      scp: ScatterGathererPort =>
        scp.identifier.localName.value -> scp.womType
    }.toMap
    val promises = collectSubJobs.launchContinue(childJobs, next, resultTypes)
    val promisesStr = promises.mkString("\n")
    Utils.appletLog(verbose, s"resultTypes=${resultTypes}")
    Utils.appletLog(verbose, s"promises=${promisesStr}")
    promises
  }

  // Launch a subjob to collect and marshal the call results.
  // Remove the declarations already calculated
  private def collectScatter(sctNode: ScatterNode,
                             childJobs: Vector[DxExecution]): Map[String, WdlVarLinks] = {
    val resultTypes: Map[String, WomArrayType] = sctNode.outputMapping.map {
      scp: ScatterGathererPort =>
        scp.identifier.localName.value -> scp.womType
    }.toMap
    val promises = collectSubJobs.launchCollect(childJobs, resultTypes)
    val promisesStr = promises.mkString("\n")
    Utils.appletLog(verbose, s"resultTypes=${resultTypes}")
    Utils.appletLog(verbose, s"promises=${promisesStr}")
    promises
  }

  private def execScatterCall(sctNode: ScatterNode,
                              call: CallNode,
                              env: Map[String, WomValue]): Map[String, WdlVarLinks] = {
    val (svNode, collection, next) = evalScatterCollection(sctNode, env)

    // loop on the collection, call the applet in the inner loop
    val childJobs: Vector[DxExecution] =
      collection.map { item =>
        val innerEnv = env + (svNode.identifier.localName.value -> item)
        val callInputs = evalCallInputs(call, innerEnv)
        val callHint = readableNameForScatterItem(item)
        val (_, dxJob) = execCall(call, callInputs, callHint)
        dxJob
      }.toVector

    next match {
      case Some(index) =>
        // there are remaining chunks - call a continue sub-job
        continueScatter(sctNode, childJobs, index)
      case None =>
        // this is the last chunk - call collect sub-job to gather all the results
        collectScatter(sctNode, childJobs)
    }
  }

  private def execScatterSubblock(sctNode: ScatterNode,
                                  env: Map[String, WomValue]): Map[String, WdlVarLinks] = {
    val (svNode, collection, next) = evalScatterCollection(sctNode, env)

    // There must be exactly one sub-workflow
    assert(execLinkInfo.size == 1)
    val (_, linkInfo) = execLinkInfo.toVector.head

    // loop on the collection, call the applet in the inner loop
    val childJobs: Vector[DxExecution] =
      collection.map { item =>
        val innerEnv = env + (svNode.identifier.localName.value -> item)
        val callHint = readableNameForScatterItem(item)
        val dbgName = callHint match {
          case None       => linkInfo.name
          case Some(hint) => s"${linkInfo.name} ${hint}"
        }

        // The subblock is complex, and requires a fragment, or a subworkflow
        val callInputs: JsValue = buildCallInputs(linkInfo.name, linkInfo, innerEnv)
        val (_, dxJob) = execDNAxExecutable(linkInfo.dxExec.getId, dbgName, callInputs, None)
        dxJob
      }.toVector

    next match {
      case Some(index) =>
        // there are remaning chunks - call a continue sub-job
        continueScatter(sctNode, childJobs, index)
      case None =>
        // this is the last chunk - call collect sub-job to gather all the results
        collectScatter(sctNode, childJobs)
    }
  }

  def apply(blockPath: Vector[Int],
            envInitial: Map[String, WomValue],
            runMode: RunnerWfFragmentMode.Value): Map[String, JsValue] = {
    Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
    Utils.appletLog(verbose, s"link info=${execLinkInfo}")
    Utils.appletLog(verbose, s"Environment: ${envInitial}")

    // sort from low to high according to the source lines.
    val callsLoToHi: Vector[String] =
      ParseWomSourceFile(verbose).scanForCalls(wf.innerGraph, wfSourceCode)

    // Find the fragment block to execute
    val block = Block.getSubBlock(blockPath, wf.innerGraph, callsLoToHi)
    Utils.appletLog(
        verbose,
        s"""|Block ${blockPath} to execute:
            |${WomPrettyPrintApproxWdl.block(block)}
            |
            |""".stripMargin
    )

    // Some of the inputs could be optional. If they are missing,
    // add in a None value.
    val allInputs = Block.closure(block)
    val envInitialFilled: Map[String, WomValue] = allInputs.flatMap {
      case (name, (womType, hasDefaultVal)) =>
        (envInitial.get(name), womType) match {
          case (None, WomOptionalType(t)) =>
            Some(name -> WomOptionalValue(t, None))
          case (None, _) if hasDefaultVal =>
            None
          case (None, _) =>
            // input is missing, and there is no default.
            Utils.warning(utlVerbose,
                          s"input is missing for ${name}, and there is no default at the callee")
            None
          case (Some(x), _) =>
            Some(name -> x)
        }
    }.toMap

    val catg = Block.categorize(block)
    val env = evalExpressions(catg.nodes, envInitialFilled)

    val fragResults: Map[String, WdlVarLinks] = runMode match {
      case RunnerWfFragmentMode.Launch =>
        // The last node could be a call or a block. All the other nodes
        // are expressions.
        catg match {
          case Block.AllExpressions(_) =>
            Map.empty

          case Block.CallDirect(_, _) =>
            throw new Exception("sanity, shouldn't reach this state")

          // A single call at the end of the block
          case Block.CallWithSubexpressions(_, call: CallNode) =>
            val callInputs = evalCallInputs(call, env)
            val (_, dxExec) = execCall(call, callInputs, None)
            genPromisesForCall(call, dxExec)

          // The block contains a call and a bunch of expressions
          // that will be part of the output.
          case Block.CallFragment(_, call: CallNode) =>
            val callInputs = evalCallInputs(call, env)
            val (_, dxExec) = execCall(call, callInputs, None)
            genPromisesForCall(call, dxExec)

          // The block contains a single call. We can execute it
          // right here, without another job.
          case Block.CondOneCall(_, cnNode, call) =>
            execConditionalCall(cnNode, call, env)

          // a conditional with a subblock inside it. We
          // need to call an applet or a subworkflow.
          case Block.CondFullBlock(_, cnNode) =>
            execConditionalSubblock(cnNode, env)

          // a scatter with a subblock inside it. Iterate
          // on the scatter variable, and make the applet call
          // for each value.
          case Block.ScatterOneCall(_, sctNode, call) =>
            assert(scatterStart == 0)
            execScatterCall(sctNode, call, env)

          // Same as previous case, but call a subworkflow
          // or fragment.
          case Block.ScatterFullBlock(_, sctNode) =>
            assert(scatterStart == 0)
            execScatterSubblock(sctNode, env)
        }
      case RunnerWfFragmentMode.Continue =>
        // A subjob that collects results from scatters
        catg match {
          // a scatter with a subblock inside it. Iterate
          // on the scatter variable, and make the applet call
          // for each value.
          case Block.ScatterOneCall(_, sctNode, call) =>
            assert(scatterStart > 0)
            execScatterCall(sctNode, call, env)
          // Same as previous case, but call a subworkflow
          // or fragment.
          case Block.ScatterFullBlock(_, sctNode) =>
            assert(scatterStart > 0)
            execScatterSubblock(sctNode, env)
          case other =>
            throw new AppInternalException(s"Bad case ${other.getClass} ${other}")
        }
      case RunnerWfFragmentMode.Collect =>
        catg match {
          case Block.ScatterOneCall(_, _, call) =>
            // scatter with a single call
            collectSubJobs.aggregateResults(call)

          case Block.ScatterFullBlock(_, _) =>
            // A scatter with a complex sub-block, compiled as a sub-workflow
            // There must be exactly one sub-workflow
            assert(execLinkInfo.size == 1)
            val (_, linkInfo) = execLinkInfo.toVector.head
            collectSubJobs.aggregateResultsFromGeneratedSubWorkflow(linkInfo)

          case other =>
            throw new AppInternalException(s"Bad case ${other.getClass} ${other}")
        }
    }

    // figure out what outputs need to be exported
    val blockOutputs: Map[String, WomType] = Block.outputs(block)
    val exportedVars: Set[String] = blockOutputs.keys.toSet

    val jsOutputs: Map[String, JsValue] = processOutputs(env, fragResults, exportedVars)
    val jsOutputsDbgStr = jsOutputs.mkString("\n")
    Utils.appletLog(verbose, s"""|JSON outputs:
                                 |${jsOutputsDbgStr}""".stripMargin)
    jsOutputs
  }
}
