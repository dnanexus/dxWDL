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

import java.nio.file.Paths
import spray.json._
import wdlTools.eval.{Context => EvalContext, Eval => WdlExprEval, WdlValues}
import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}

import dxWDL.base._
import dxWDL.dx._
import dxWDL.util._

case class WfFragRunner(wf: TAT.Workflow,
                        taskDir: Map[String, TAT.Task],
                        typeAliases: Map[String, WdlTypes.T],
                        document : TAT.Document,
                        instanceTypeDB: InstanceTypeDB,
                        execLinkInfo: Map[String, ExecLinkInfo],
                        dxPathConfig: DxPathConfig,
                        dxIoFunctions: DxIoFunctions,
                        inputsRaw: JsValue,
                        fragInputOutput: WfFragInputOutput,
                        defaultRuntimeAttributes: Option[WdlRuntimeAttrs],
                        delayWorkspaceDestruction: Option[Boolean],
                        runtimeDebugLevel: Int) {
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
                                              fragInputOutput.typeAliases)

  var gSeqNum = 0
  private def launchSeqNum(): Int = {
    gSeqNum += 1
    gSeqNum
  }

  // build an object capable of evaluating WDL expressions
  val evaluator : WdlExprEval = {
    val evalOpts = wdlTools.util.Options(typeChecking = wdlTools.util.TypeCheckingRegime.Strict,
                                         antlr4Trace = false,
                                         localDirectories = Vector.empty,
                                         verbosity = wdlTools.util.Verbosity.Quiet)
    val evalCfg = wdlTools.util.EvalConfig(dxIoFunctions.config.homeDir,
                                           dxIoFunctions.config.tmpDir,
                                           dxIoFunctions.config.stdout,
                                           dxIoFunctions.config.stderr)
    new WdlExprEval(evalOpts, evalCfg, document.version.value, None)
  }

  private def evaluateWomExpression(expr: TAT.Expr,
                                    womType: WdlTypes.T,
                                    env: Map[String, WdlValues.V]): WdlValues.V = {
    evaluator.applyExprAndCoerce(expr, womType, EvalContext(env))
  }

  private def getCallLinkInfo(call: TAT.Call): ExecLinkInfo = {
    val calleeName = call.callee.name
    execLinkInfo.get(calleeName) match {
      case None =>
        throw new AppInternalException(s"Could not find linking information for ${calleeName}")
      case Some(eInfo) => eInfo
    }
  }

  // This method is exposed so that we can unit-test it.
  def evalExpressions(nodes: Seq[TAT.WorkflowElement],
                      env: Map[String, WdlValues.V]): Map[String, WdlValues.V] = {
    nodes.foldLeft(env) {
      case (env, TAT.Declaration(name, wdlType, exprOpt, _)) =>
        val value = exprOpt match {
          case None => WdlValues.V_Null
          case Some(expr) =>
            evaluateWomExpression(expr, wdlType, env)
        }
        env + (name -> value)

      // scatter
      // scatter (K in collection) {
      // }
      case (env, TAT.Scatter(id, expr, body, _)) =>
        val collectionRaw: WdlValues.V =
          evaluateWomExpression(expr, expr.wdlType, env)
        val collection: Vector[WdlValues.V] = collectionRaw match {
          case x: WdlValues.V_Array => x.value
          case other =>
            throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
        }

        // iterate on the collection, evaluate the body N times
        val vm: Vector[Map[String, WdlValues.V]] =
          collection.map { v =>
            val envInner = env + (id -> v)
            evalExpressions(body, envInner)
          }.toVector

        // build a mapping from from result-key to its type
        val resultTypes : Map[String, WdlTypes.T] = Block.allOutputs(body)

        val initResults: Map[String, Vector[WdlValues.V]] = resultTypes.map {
          case (key,_) => key -> Vector.empty[WdlValues.V]
        }.toMap

          // merge the vector of results, each of which is a map
        val results: Map[String, Vector[WdlValues.V]] =
          vm.foldLeft(initResults) {
            case (accu, m) =>
              accu.map {
                case (key, arValues) =>
                  val value : WdlValues.V = m(key)
                  key -> (arValues :+ value)
              }
          }.toMap


        // Add the wom array type to each vector
        val resultsFull = results.map {
          case (key, vv) =>
            key -> WdlValues.V_Array(vv)
        }
        env ++ resultsFull

      case (env, TAT.Conditional(expr, body, _)) =>
        // evaluate the condition
        val condValue : Boolean = evaluateWomExpression(expr, expr.wdlType, env) match {
          case b: WdlValues.V_Boolean => b.value
          case other =>
            throw new AppInternalException(s"Unexpected condition expression value ${other}")
        }

        val resultsFull: Map[String, WdlValues.V] =
          if (!condValue) {
            // condition is false, return None for all the values
            val resultTypes = Block.allOutputs(body)
            resultTypes.map{ case (key, womType) =>
                key -> WdlValues.V_Null
            }
          } else {
            // condition is true, evaluate the internal block.
            val results = evalExpressions(body, env)
            results.map{
              case (key, value) =>
                key -> WdlValues.V_Optional(value)
            }
          }
        env ++ resultsFull

      case (env, other) =>
        throw new Exception(s"type ${other.getClass} while evaluating expressions")
    }
  }

  private def processOutputs(env: Map[String, WdlValues.V],
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
                              env: Map[String, WdlValues.V]): JsValue = {
    Utils.appletLog(
        verbose,
        s"""|buildCallInputs (${callName})
            |env:
            |${env.mkString("\n")}
            |""".stripMargin
    )

    val inputs: Map[String, WdlValues.V] = linkInfo.inputs.flatMap {
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
  private def preCalcInstanceType(task: TAT.Task,
                                  taskInputs: Map[TAT.InputDefinition, WdlValues.V]): Option[String] = {
    // Note: if none of these attributes are specified, the return value is None.
    val instanceAttrs = Set("memory", "disks", "cpu")
    val allConst = instanceAttrs.forall { attrName =>
      task.runtimeAttributes.attributes.get(attrName) match {
        case None       => true
        case Some(expr) => WdlValues.VAnalysis.isExpressionConst(WdlTypes.T_String, expr)
      }
    }
    if (allConst)
      return None

    // There is runtime evaluation for the instance type
    val taskRunner = new TaskRunner(task,
                                    document,
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

  private def execCall(call: TAT.Call,
                       callInputs: Map[String, WdlValues.V],
                       callNameHint: Option[String]): (Int, DxExecution) = {
    val linkInfo = getCallLinkInfo(call)
    val callName = call.actualName
    val calleeName = call.callee.name
    val callInputsJSON: JsValue = buildCallInputs(callName, linkInfo, callInputs)
    /*        Utils.appletLog(verbose, s"""|Call ${callName}
                                     |calleeName= ${calleeName}
                                     |inputs = ${callInputsJSON}""".stripMargin)*/

    // This is presented in the UI, to inform the user
    val dbgName = callNameHint match {
      case None       => call.actualName
      case Some(hint) => s"${callName} ${hint}"
    }

    // If this is a call to a task that computes the required instance type at runtime,
    // do the calculation right now. This saves a job relaunch down the road.
    val instanceType: Option[String] = taskDir.get(calleeName) match {
        case Some(task) =>
          val taskInputs = jobInputOutput.loadInputs(callInputsJSON, task)
          preCalcInstanceType(task, taskInputs)
        case (_, _) => None
      }
    execDNAxExecutable(linkInfo.dxExec.getId, dbgName, callInputsJSON, instanceType)
  }

  // create promises to this call. This allows returning
  // from the parent job immediately.
  private def genPromisesForCall(call: TAT.Call,
                                 dxExec: DxExecution): Map[String, WdlVarLinks] = {
    val linkInfo = getCallLinkInfo(call)
    val callName = call.actualName
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
  def evalCallInputs(call: TAT.Call,
                     env: Map[String, WdlValues.V]): Map[String, WdlValues.V] = {
    call.inputs.map{ case (key, expr) =>
      key -> evaluateWomExpression(expr, expr.wdlType, env)
    }.toMap
  }

  // Evaluate the condition
  private def evalCondition(cnNode: TAT.Conditional,
                            env: Map[String, WdlValues.V]): Boolean = {
    val condValueRaw: WdlValues.V =
      evaluateWomExpression(cnNode.expr, WdlTypes.T_Boolean, env)
    condValueRaw match {
      case b: WdlValues.V_Boolean => b.value
      case other         => throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
    }
  }

  // A subblock containing exactly one call.
  // For example:
  //
  // if (flag) {
  //   call zinc as inc3 { input: a = num}
  // }
  //
  private def execConditionalCall(cnNond : TAT.Conditional,
                                  call: CallNode,
                                  env: Map[String, WdlValues.V]): Map[String, WdlVarLinks] = {
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
            case WdlTypes.T_Optional(_) => womType
            case _                  => WdlTypes.T_Optional(womType)
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
  private def execConditionalSubblock(cnNode: TAT.Conditional,
                                      env: Map[String, WdlValues.V]): Map[String, WdlVarLinks] = {
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
            case WdlTypes.T_Optional(_) => womType
            case _                  => WdlTypes.T_Optional(womType)
          }
          varName -> WdlVarLinks(optionalType, DxlExec(dxExec, varName))
      }.toMap
    }
  }

  // create a short, easy to read, description for a scatter element.
  private def readableNameForScatterItem(item: WdlValues.V): Option[String] = {
    item match {
      case WdlValues.V_Boolean(_) | WdlValues.V_Int(_) | WdlValues.V_Float(_) =>
        Some(item.toWdlValues.V_String)
      case WdlValues.V_String(s) =>
        Some(s)
      case WdlValues.V_File(path) =>
        val p = Paths.get(path).getFileName()
        Some(p.toString)
      case WdlValues.V_Pair(l, r) =>
        val ls = readableNameForScatterItem(l)
        val rs = readableNameForScatterItem(r)
        (ls, rs) match {
          case (Some(ls1), Some(rs1)) => Some(s"(${ls1}, ${rs1})")
          case _                      => None
        }
      case WdlValues.V_OptionalValue(_, Some(x)) =>
        readableNameForScatterItem(x)
      case WdlValues.V_Array(_, arrValues) =>
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
      env: Map[String, WdlValues.V]
  ): (ScatterVariableNode, Seq[WdlValues.V]) = {
    // WDL has exactly one variable
    assert(sctNode.scatterVariableNodes.size == 1)
    val svNode: ScatterVariableNode = sctNode.scatterVariableNodes.head
    val collectionRaw: WdlValues.V =
      evaluateWomExpression(svNode.scatterExpressionNode.womExpression,
                            WdlTypes.T_Array(svNode.womType),
                            env)
    val collection: Seq[WdlValues.V] = collectionRaw match {
      case x: WdlValues.V_Array => x.value
      case other       => throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
    }

    // Limit the number of elements in the collection. Each one spawns a job; this strains the platform
    // at large numbers.
    if (collection.size > Utils.SCATTER_LIMIT) {
      throw new AppInternalException(
          s"""|The scatter iterates over ${collection.size} elements which
              |exeedes the maximum (${Utils.SCATTER_LIMIT})""".stripMargin
            .replaceAll("\n", " ")
      )
    }
    (svNode, collection)
  }

  // Launch a subjob to collect and marshal the call results.
  // Remove the declarations already calculated
  private def collectScatter(sctNode: ScatterNode,
                             childJobs: Vector[DxExecution]): Map[String, WdlVarLinks] = {
    val resultTypes: Map[String, WdlTypes.T_Array] = sctNode.outputMapping.map {
      case scp: ScatterGathererPort =>
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
                              env: Map[String, WdlValues.V]): Map[String, WdlVarLinks] = {
    val (svNode, collection) = evalScatterCollection(sctNode, env)

    // loop on the collection, call the applet in the inner loop
    val childJobs: Vector[DxExecution] =
      collection.map { item =>
        val innerEnv = env + (svNode.identifier.localName.value -> item)
        val callInputs = evalCallInputs(call, innerEnv)
        val callHint = readableNameForScatterItem(item)
        val (_, dxJob) = execCall(call, callInputs, callHint)
        dxJob
      }.toVector

    collectScatter(sctNode, childJobs)
  }

  private def execScatterSubblock(sctNode: ScatterNode,
                                  env: Map[String, WdlValues.V]): Map[String, WdlVarLinks] = {
    val (svNode, collection) = evalScatterCollection(sctNode, env)

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

    collectScatter(sctNode, childJobs)
  }

  def apply(blockPath: Vector[Int],
            envInitial: Map[String, WdlValues.V],
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
    val envInitialFilled: Map[String, WdlValues.V] = allInputs.flatMap {
      case (name, (womType, hasDefaultVal)) =>
        (envInitial.get(name), womType) match {
          case (None, WdlTypes.T_Optional(t)) =>
            Some(name -> WdlValues.V_OptionalValue(t, None))
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
            execScatterCall(sctNode, call, env)

          // Same as previous case, but call a subworkflow
          // or fragment.
          case Block.ScatterFullBlock(_, sctNode) =>
            execScatterSubblock(sctNode, env)
        }

      // A subjob that collects results from scatters
      case RunnerWfFragmentMode.Collect =>
        val childJobsComplete = collectSubJobs.executableFromSeqNum()
        catg match {
          case Block.ScatterOneCall(_, sctNode, call) =>
            // scatter with a single call
            collectSubJobs.aggregateResults(call, childJobsComplete)

          case Block.ScatterFullBlock(_, sctNode) =>
            // A scatter with a complex sub-block, compiled as a sub-workflow
            // There must be exactly one sub-workflow
            assert(execLinkInfo.size == 1)
            val (_, linkInfo) = execLinkInfo.toVector.head
            collectSubJobs.aggregateResultsFromGeneratedSubWorkflow(linkInfo, childJobsComplete)

          case other =>
            throw new AppInternalException(s"Bad case ${other.getClass} ${other}")
        }
    }

    // figure out what outputs need to be exported
    val blockOutputs: Map[String, WdlTypes.T] = Block.outputs(block)
    val exportedVars: Set[String] = blockOutputs.keys.toSet

    val jsOutputs: Map[String, JsValue] = processOutputs(env, fragResults, exportedVars)
    val jsOutputsDbgStr = jsOutputs.mkString("\n")
    Utils.appletLog(verbose, s"""|JSON outputs:
                                 |${jsOutputsDbgStr}""".stripMargin)
    jsOutputs
  }
}
