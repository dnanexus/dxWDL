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

package dx.exec

import java.nio.file.Paths

import dx.{AppInternalException, exec}
import dx.api._
import dx.compiler.WdlRuntimeAttrs
import dx.core.getVersion
import dx.core.io.{DxFileDescCache, DxPathConfig}
import dx.core.languages.wdl._
import spray.json._
import wdlTools.eval.{Eval, WdlValues}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{FileSourceResolver, TraceLevel}

case class WfFragRunner(wf: TAT.Workflow,
                        taskDir: Map[String, TAT.Task],
                        typeAliases: Map[String, WdlTypes.T],
                        document: TAT.Document,
                        instanceTypeDB: InstanceTypeDB,
                        execLinkInfo: Map[String, ExecLinkInfo],
                        dxPathConfig: DxPathConfig,
                        fileResolver: FileSourceResolver,
                        wdlVarLinksConverter: WdlVarLinksConverter,
                        jobInputOutput: JobInputOutput,
                        inputsRaw: JsValue,
                        fragInputOutput: WfFragInputOutput,
                        defaultRuntimeAttributes: Option[WdlRuntimeAttrs],
                        delayWorkspaceDestruction: Option[Boolean],
                        dxApi: DxApi,
                        evaluator: Eval) {
  private val MAX_JOB_NAME = 50
  private val collectSubJobs = CollectSubJobs(
      jobInputOutput,
      inputsRaw,
      instanceTypeDB,
      delayWorkspaceDestruction,
      dxApi,
      // TODO: to we really need to provide an empty cache?
      wdlVarLinksConverter.copy(dxFileDescCache = DxFileDescCache.empty)
  )

  var gSeqNum = 0
  private def launchSeqNum(): Int = {
    gSeqNum += 1
    gSeqNum
  }

  private def evaluateWdlExpression(expr: TAT.Expr,
                                    wdlType: WdlTypes.T,
                                    env: Map[String, (WdlTypes.T, WdlValues.V)]): WdlValues.V = {
    evaluator.applyExprAndCoerce(expr, wdlType, Eval.createBindingsFromEnv(env))
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
  def evalExpressions(
      nodes: Seq[TAT.WorkflowElement],
      env: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, (WdlTypes.T, WdlValues.V)] = {
    nodes.foldLeft(Map.empty[String, (WdlTypes.T, WdlValues.V)]) {
      case (accu, TAT.Declaration(name, wdlType, exprOpt, _)) =>
        val value = exprOpt match {
          case None => WdlValues.V_Null
          case Some(expr) =>
            evaluateWdlExpression(expr, wdlType, accu ++ env)
        }
        accu + (name -> (wdlType, value))

      // scatter
      // scatter (K in collection) {
      // }
      case (accu, TAT.Scatter(id, expr, body, _)) =>
        val collectionRaw: WdlValues.V =
          evaluateWdlExpression(expr, expr.wdlType, accu ++ env)
        val collection: Vector[WdlValues.V] = collectionRaw match {
          case x: WdlValues.V_Array => x.value
          case other =>
            throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
        }

        // iterate on the collection, evaluate the body N times
        val vm: Vector[Map[String, (WdlTypes.T, WdlValues.V)]] =
          collection.map { v =>
            val envInner = accu ++ env + (id -> (expr.wdlType, v))
            evalExpressions(body, envInner)
          }

        // build a mapping from from result-key to its type
        val resultTypes: Map[String, WdlTypes.T] = Block.allOutputs(body)

        val initResults: Map[String, (WdlTypes.T, Vector[WdlValues.V])] = resultTypes.map {
          case (key, t) => key -> (t, Vector.empty[WdlValues.V])
        }

        // merge the vector of results, each of which is a map
        val results: Map[String, (WdlTypes.T, Vector[WdlValues.V])] =
          vm.foldLeft(initResults) {
            case (accu, m) =>
              accu.map {
                case (key, (t, arValues)) =>
                  val (_, value: WdlValues.V) = m(key)
                  key -> (t, arValues :+ value)
              }
          }

        // Add the WDL array type to each vector
        val resultsFull = results.map {
          case (key, (t, vv)) =>
            key -> (WdlTypes.T_Array(t, nonEmpty = false), WdlValues.V_Array(vv))
        }
        accu ++ resultsFull

      case (accu, TAT.Conditional(expr, body, _)) =>
        // evaluate the condition
        val condValue: Boolean = evaluateWdlExpression(expr, expr.wdlType, accu ++ env) match {
          case b: WdlValues.V_Boolean => b.value
          case other =>
            throw new AppInternalException(s"Unexpected condition expression value ${other}")
        }

        val resultsFull: Map[String, (WdlTypes.T, WdlValues.V)] =
          if (!condValue) {
            // condition is false, return None for all the values
            val resultTypes = Block.allOutputs(body)
            resultTypes.map {
              case (key, wdlType) =>
                key -> (WdlTypes.T_Optional(wdlType), WdlValues.V_Null)
            }
          } else {
            // condition is true, evaluate the internal block.
            val results = evalExpressions(body, accu ++ env)
            results.map {
              case (key, (t, value)) =>
                key -> (WdlTypes.T_Optional(t), WdlValues.V_Optional(value))
            }
          }
        accu ++ resultsFull

      case (_, other) =>
        throw new Exception(s"type ${other.getClass} while evaluating expressions")
    }
  }

  private def processOutputs(env: Map[String, (WdlTypes.T, WdlValues.V)],
                             fragResults: Map[String, WdlVarLinks],
                             exportedVars: Set[String]): Map[String, JsValue] = {
    dxApi.logger.traceLimited(
        s"""|processOutputs
            |  env = ${env.keys}
            |  fragResults = ${fragResults.keys}
            |  exportedVars = ${exportedVars}
            |""".stripMargin,
        minLevel = TraceLevel.VVerbose
    )

    // convert the WDL values to WVLs
    val envWvl = env.map {
      case (name, (wdlType, value)) =>
        name -> wdlVarLinksConverter.importFromWDL(wdlType, value)
    }

    // filter anything that should not be exported.
    val exportedWvls = (envWvl ++ fragResults).filter {
      case (name, _) => exportedVars contains name
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
  }

  // Access a field in a WDL pair/struct/object
  private def accessField(obj: WdlValues.V, fieldName: String): WdlValues.V = {
    obj match {
      case WdlValues.V_Pair(lv, _) if fieldName == "left" =>
        lv
      case WdlValues.V_Pair(_, rv) if fieldName == "right" =>
        rv
      case WdlValues.V_Struct(_, members) if members contains fieldName =>
        members(fieldName)
      case WdlValues.V_Call(_, members) if members contains fieldName =>
        members(fieldName)
      case WdlValues.V_Object(members) if members contains fieldName =>
        members(fieldName)
      case _ =>
        throw new Exception(s"field ${fieldName} does not exist in ${obj}")
    }
  }

  private def lookupInEnv(fqn: String,
                          env: Map[String, (WdlTypes.T, WdlValues.V)]): Option[WdlValues.V] = {
    if (env contains fqn) {
      // exact match, bottom of recursion
      val (_, v) = env(fqn)
      Some(v)
    } else {
      // A.B.C --> A.B
      val pos = fqn.lastIndexOf(".")
      if (pos < 0) {
        None
      } else {
        val lhs = fqn.substring(0, pos) // A.B
        val rhs = fqn.substring(pos + 1) // C

        // Look for "A.B"
        lookupInEnv(lhs, env) match {
          case None    => None
          case Some(v) => Some(accessField(v, rhs))
        }
      }
    }
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
                              env: Map[String, (WdlTypes.T, WdlValues.V)]): JsValue = {
    dxApi.logger.traceLimited(
        s"""|buildCallInputs (${callName})
            |env:
            |${env.mkString("\n")}
            |
            |linkInfo = ${linkInfo}
            |""".stripMargin,
        minLevel = TraceLevel.VVerbose
    )

    val inputs: Map[String, WdlValues.V] = linkInfo.inputs.flatMap {
      case (varName, _) =>
        val retval = lookupInEnv(varName, env)
        dxApi.logger.traceLimited(s"lookupInEnv(${varName} = ${retval})",
                                  minLevel = TraceLevel.VVerbose)
        retval match {
          case None =>
            // No binding for this input. It might be optional,
            // it could have a default value. It could also actually be missing,
            // which will result in a platform error.
            None
          case Some(wdlValue) =>
            Some(varName -> wdlValue)
        }
    }

    val wvlInputs = inputs.map {
      case (name, wdlValue) =>
        val wdlType = linkInfo.inputs(name)
        name -> wdlVarLinksConverter.importFromWDL(wdlType, wdlValue)
    }
    dxApi.logger.traceLimited(s"wvlInputs = ${wvlInputs}", minLevel = TraceLevel.VVerbose)

    val m = wvlInputs.foldLeft(Map.empty[String, JsValue]) {
      case (accu, (varName, wvl)) =>
        val fields = wdlVarLinksConverter.genFields(wvl, varName)
        accu ++ fields.toMap
    }
    dxApi.logger.traceLimited(s"WfFragRunner: buildCallInputs(m) = ${JsObject(m).prettyPrint}",
                              minLevel = TraceLevel.VVerbose)
    JsObject(m)
  }

  // Figure out what instance type to launch at task in. Return None if the instance
  // type is a constant, and is already set.
  private def preCalcInstanceType(
      task: TAT.Task,
      taskInputs: Map[TAT.InputDefinition, WdlValues.V]
  ): Option[String] = {
    // Note: if none of these attributes are specified, the return value is None.
    val instanceAttrs = Set("memory", "disks", "cpu")
    val attributes: Map[String, TAT.Expr] = task.runtime match {
      case None                             => Map.empty
      case Some(TAT.RuntimeSection(kvs, _)) => kvs
    }
    val allConst = instanceAttrs.forall { attrName =>
      attributes.get(attrName) match {
        case None       => true
        case Some(expr) => evaluator.isConst(expr, WdlTypes.T_String)
      }
    }
    if (allConst)
      return None

    // There is runtime evaluation for the instance type
    val taskRunner = TaskRunner(
        task,
        document,
        typeAliases,
        instanceTypeDB,
        dxPathConfig,
        fileResolver,
        wdlVarLinksConverter,
        jobInputOutput,
        defaultRuntimeAttributes,
        delayWorkspaceDestruction,
        dxApi,
        evaluator
    )
    try {
      val iType = taskRunner.calcInstanceType(taskInputs)
      dxApi.logger.traceLimited(s"Precalculated instance type for ${task.name}: ${iType}")
      Some(iType)
    } catch {
      case e: Throwable =>
        dxApi.logger.traceLimited(
            s"""|Failed to precalculate the instance type for
                |task ${task.name}.
                |
                |${e}
                |""".stripMargin
        )
        None
    }
  }

  private def execDNAxExecutable(execLink: ExecLinkInfo,
                                 dbgName: String,
                                 callInputs: JsValue,
                                 instanceType: Option[String]): (Int, DxExecution) = {
    dxApi.logger.traceLimited(s"execDNAx ${callInputs.prettyPrint}", minLevel = TraceLevel.VVerbose)

    // Last check that we have all the compulsory arguments.
    //
    // Note that we don't have the information here to tell difference between optional and non
    // optionals. Right now, we are emitting warnings for optionals or arguments that have defaults
    if (callInputs.isInstanceOf[JsObject]) {
      val fields = callInputs.asJsObject.fields
      execLink.inputs.foreach {
        case (argName, _) =>
          fields.get(argName) match {
            case None =>
              dxApi.logger.warning(s"Missing argument ${argName} to call ${execLink.name}",
                                   force = true)
            case Some(_) => ()
          }
      }
    }

    // We may need to run a collect subjob. Add the the sequence
    // number to each invocation, so the collect subjob will be
    // able to put the results back together in the correct order.
    val seqNum: Int = launchSeqNum()

    // If this is a task that specifies the instance type
    // at runtime, launch it in the requested instance.
    val dxExecId = execLink.dxExec.getId
    val dxExec =
      if (dxExecId.startsWith("app-")) {
        val applet = dxApi.app(dxExecId)
        val dxJob = applet.newRun(
            dbgName,
            callInputs,
            instanceType = instanceType,
            properties = Map("seq_number" -> JsString(seqNum.toString)),
            delayWorkspaceDestruction = delayWorkspaceDestruction
        )
        dxJob
      } else if (dxExecId.startsWith("applet-")) {
        val applet = dxApi.applet(dxExecId)
        val dxJob = applet.newRun(
            dbgName,
            callInputs,
            instanceType = instanceType,
            properties = Map("seq_number" -> JsString(seqNum.toString)),
            delayWorkspaceDestruction = delayWorkspaceDestruction
        )
        dxJob
      } else if (dxExecId.startsWith("workflow-")) {
        val workflow = dxApi.workflow(dxExecId)
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
                       callInputs: Map[String, (WdlTypes.T, WdlValues.V)],
                       callNameHint: Option[String]): (Int, DxExecution) = {
    dxApi.logger.traceLimited(
        s"""|call = ${call}
            |callInputs = ${callInputs}
            |""".stripMargin,
        minLevel = TraceLevel.VVerbose
    )
    val wvlInputs = callInputs.map {
      case (name, (wdlType, wdlValue)) =>
        name -> wdlVarLinksConverter.importFromWDL(wdlType, wdlValue)
    }

    val callInputsJs = wvlInputs.foldLeft(Map.empty[String, JsValue]) {
      case (accu, (varName, wvl)) =>
        val fields = wdlVarLinksConverter.genFields(wvl, varName)
        accu ++ fields.toMap
    }
    val callInputsJSON = JsObject(callInputsJs)
    dxApi.logger
      .traceLimited(s"callInputsJSON = ${callInputsJSON.prettyPrint}",
                    minLevel = TraceLevel.VVerbose)

    // This is presented in the UI, to inform the user
    val dbgName = callNameHint match {
      case None       => call.actualName
      case Some(hint) => s"${call.actualName} ${hint}"
    }

    // If this is a call to a task that computes the required instance type at runtime,
    // do the calculation right now. This saves a job relaunch down the road.
    val calleeName = call.callee.name
    val instanceType: Option[String] = taskDir.get(calleeName) match {
      case None => None
      case Some(task) =>
        val taskInputs = jobInputOutput.loadInputs(callInputsJSON, task)
        preCalcInstanceType(task, taskInputs)
    }
    val linkInfo = getCallLinkInfo(call)
    execDNAxExecutable(linkInfo, dbgName, callInputsJSON, instanceType)
  }

  // create promises to this call. This allows returning
  // from the parent job immediately.
  private def genPromisesForCall(call: TAT.Call, dxExec: DxExecution): Map[String, WdlVarLinks] = {
    val linkInfo = getCallLinkInfo(call)
    val callName = call.actualName
    linkInfo.outputs.map {
      case (varName, wdlType) =>
        val oName = s"${callName}.${varName}"
        oName -> WdlVarLinks(wdlType, DxLinkExec(dxExec, varName))
    }
  }

  // Note that we may need to coerce the caller inputs to what the callee expects.
  //
  // For example:
  //
  // workflow foo {
  //   call EmptyArray { input: fooAr=[] }
  // }
  //
  // task EmptyArray {
  //   input {
  //      Array[Int] fooAr
  //   }
  //   command {
  //   }
  //   output {
  //     Array[Int] result=fooAr
  //   }
  // }
  //
  //
  // The fooAr should be coerced from Array[Any] to Array[Int]
  //
  def evalCallInputs(
      call: TAT.Call,
      env: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, (WdlTypes.T, WdlValues.V)] = {
    val calleeInputs = call.callee.input
    call.inputs.map {
      case (key, expr) =>
        val actualCalleeType: WdlTypes.T = calleeInputs.get(key) match {
          case None =>
            throw new Exception(s"Callee ${call.callee.name} doesn't have input ${key}")
          case Some((t, _)) => t
        }

        val value = evaluateWdlExpression(expr, actualCalleeType, env)
        key -> (actualCalleeType, value)
    }
  }

  // Evaluate the condition
  private def evalCondition(cnNode: TAT.Conditional,
                            env: Map[String, (WdlTypes.T, WdlValues.V)]): Boolean = {
    val condValueRaw: WdlValues.V =
      evaluateWdlExpression(cnNode.expr, WdlTypes.T_Boolean, env)
    condValueRaw match {
      case b: WdlValues.V_Boolean => b.value
      case other                  => throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
    }
  }

  // A subblock containing exactly one call.
  // For example:
  //
  // if (flag) {
  //   call zinc as inc3 { input: a = num}
  // }
  //
  private def execConditionalCall(
      cnNode: TAT.Conditional,
      call: TAT.Call,
      env: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, WdlVarLinks] = {
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
        case (key, WdlVarLinks(wdlType, dxl)) =>
          // be careful not to make double optionals
          val optionalType = wdlType match {
            case WdlTypes.T_Optional(_) => wdlType
            case _                      => WdlTypes.T_Optional(wdlType)
          }
          key -> WdlVarLinks(optionalType, dxl)
      }
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
  private def execConditionalSubblock(
      cnNode: TAT.Conditional,
      env: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, WdlVarLinks] = {
    if (!evalCondition(cnNode, env)) {
      // Condition is false, no need to execute the call
      Map.empty
    } else {
      // There must be exactly one sub-workflow
      assert(execLinkInfo.size == 1)
      val (_, linkInfo) = execLinkInfo.toVector.head

      // The subblock is complex, and requires a fragment, or a subworkflow
      val callInputs: JsValue = buildCallInputs(linkInfo.name, linkInfo, env)
      val (_, dxExec) = execDNAxExecutable(linkInfo, linkInfo.name, callInputs, None)

      // create promises for results
      linkInfo.outputs.map {
        case (varName, wdlType) =>
          // Add optional modifier to the return types.
          // be careful not to make double optionals
          val optionalType = wdlType match {
            case WdlTypes.T_Optional(_) => wdlType
            case _                      => WdlTypes.T_Optional(wdlType)
          }
          varName -> WdlVarLinks(optionalType, DxLinkExec(dxExec, varName))
      }
    }
  }

  // create a short, easy to read, description for a scatter element.
  private def readableNameForScatterItem(item: WdlValues.V): Option[String] = {
    item match {
      case WdlValues.V_Boolean(b) => Some(b.toString)
      case WdlValues.V_Int(i)     => Some(i.toString)
      case WdlValues.V_Float(x)   => Some(x.toString)
      case WdlValues.V_String(s) =>
        Some(s)
      case WdlValues.V_File(path) =>
        val p = Paths.get(path).getFileName
        Some(p.toString)
      case WdlValues.V_Pair(l, r) =>
        val ls = readableNameForScatterItem(l)
        val rs = readableNameForScatterItem(r)
        (ls, rs) match {
          case (Some(ls1), Some(rs1)) => Some(s"(${ls1}, ${rs1})")
          case _                      => None
        }
      case WdlValues.V_Optional(x) =>
        readableNameForScatterItem(x)
      case WdlValues.V_Array(arrValues) =>
        // Create a name by concatenating the initial elements of the array.
        // Limit the total size of the name.
        val arrBeginning = arrValues.slice(0, 3)
        val elements = arrBeginning.flatMap(readableNameForScatterItem)
        Some(WfFragRunner.buildLimitedSizeName(elements, MAX_JOB_NAME))
      case _ =>
        None
    }
  }

  // Evaluate the collection on which we are scattering
  private def evalScatterCollection(
      sct: TAT.Scatter,
      env: Map[String, (WdlTypes.T, WdlValues.V)]
  ): (String, WdlTypes.T, Vector[WdlValues.V]) = {
    val collectionRaw: WdlValues.V =
      evaluateWdlExpression(sct.expr, sct.expr.wdlType, env)
    val collection: Seq[WdlValues.V] = collectionRaw match {
      case x: WdlValues.V_Array => x.value
      case other                => throw new AppInternalException(s"Unexpected class ${other.getClass}, ${other}")
    }

    // Limit the number of elements in the collection. Each one spawns a job; this strains the platform
    // at large numbers.
    if (collection.size > exec.SCATTER_LIMIT) {
      throw new AppInternalException(
          s"""|The scatter iterates over ${collection.size} elements which
              |exeedes the maximum (${exec.SCATTER_LIMIT})""".stripMargin
            .replaceAll("\n", " ")
      )
    }

    val elemType = sct.expr.wdlType match {
      case WdlTypes.T_Array(t, _) => t
      case _                      => throw new Exception("Scatter collection is not an array")
    }
    (sct.identifier, elemType, collection.toVector)
  }

  // Launch a subjob to collect and marshal the call results.
  // Remove the declarations already calculated
  private def collectScatter(sct: TAT.Scatter,
                             childJobs: Vector[DxExecution]): Map[String, WdlVarLinks] = {
    val resultTypes: Map[String, WdlTypes.T] = Block.allOutputs(sct.body)
    val resultArrayTypes = resultTypes.map {
      case (k, t) =>
        k -> WdlTypes.T_Array(t, nonEmpty = false)
    }
    val promises = collectSubJobs.launch(childJobs, resultArrayTypes)
    val promisesStr = promises.mkString("\n")

    dxApi.logger.traceLimited(s"resultTypes=${resultArrayTypes}")
    dxApi.logger.traceLimited(s"promises=${promisesStr}")
    promises
  }

  private def execScatterCall(
      sctNode: TAT.Scatter,
      call: TAT.Call,
      env: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, WdlVarLinks] = {
    val (_, elemType, collection) = evalScatterCollection(sctNode, env)

    // loop on the collection, call the applet in the inner loop
    val childJobs: Vector[DxExecution] =
      collection.map { item =>
        val innerEnv = env + (sctNode.identifier -> (elemType, item))
        val callInputs = evalCallInputs(call, innerEnv)
        val callHint = readableNameForScatterItem(item)
        val (_, dxJob) = execCall(call, callInputs, callHint)
        dxJob
      }

    collectScatter(sctNode, childJobs)
  }

  private def execScatterSubblock(
      sctNode: TAT.Scatter,
      env: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, WdlVarLinks] = {
    val (_, elemType, collection) = evalScatterCollection(sctNode, env)

    // There must be exactly one sub-workflow
    assert(execLinkInfo.size == 1)
    val (_, linkInfo) = execLinkInfo.toVector.head

    // loop on the collection, call the applet in the inner loop
    val childJobs: Vector[DxExecution] =
      collection.map { item =>
        val innerEnv = env + (sctNode.identifier -> (elemType, item))
        val callHint = readableNameForScatterItem(item)
        val dbgName = callHint match {
          case None       => linkInfo.name
          case Some(hint) => s"${linkInfo.name} ${hint}"
        }

        // The subblock is complex, and requires a fragment, or a subworkflow
        val callInputs: JsValue = buildCallInputs(linkInfo.name, linkInfo, innerEnv)
        val (_, dxJob) = execDNAxExecutable(linkInfo, dbgName, callInputs, None)
        dxJob
      }

    collectScatter(sctNode, childJobs)
  }

  def apply(blockPath: Vector[Int],
            envInitial: Map[String, (WdlTypes.T, WdlValues.V)],
            runMode: RunnerWfFragmentMode.Value): Map[String, JsValue] = {
    dxApi.logger.traceLimited(s"dxWDL version: ${getVersion}")
    dxApi.logger.traceLimited(s"link info=${execLinkInfo}")
    dxApi.logger.traceLimited(s"Environment: ${envInitial}")

    // Find the fragment block to execute
    val block = Block.getSubBlock(blockPath, wf.body)
    dxApi.logger.traceLimited(
        s"""|Block ${blockPath} to execute:
            |${PrettyPrintApprox.block(block)}
            |
            |""".stripMargin
    )

    // Some of the inputs could be optional. If they are missing,
    // add in a None value.
    val envInitialFilled: Map[String, (WdlTypes.T, WdlValues.V)] =
      block.inputs.flatMap { inputDef: BlockInput =>
        envInitial.get(inputDef.name) match {
          case None =>
            None
          case Some((t, v)) =>
            Some(inputDef.name -> (t, v))
        }
      }.toMap

    val catg = Block.categorize(block)
    val env = evalExpressions(catg.nodes, envInitialFilled) ++ envInitialFilled

    val fragResults: Map[String, WdlVarLinks] = runMode match {
      case RunnerWfFragmentMode.Launch =>
        // The last node could be a call or a block. All the other nodes
        // are expressions.
        catg match {
          case Block.AllExpressions(_) =>
            Map.empty

          case Block.CallDirect(_, _) =>
            throw new Exception("Should not be able to reach this state")

          // A single call at the end of the block
          case Block.CallWithSubexpressions(_, call) =>
            val callInputs = evalCallInputs(call, env)
            val (_, dxExec) = execCall(call, callInputs, None)
            genPromisesForCall(call, dxExec)

          // The block contains a call and a bunch of expressions
          // that will be part of the output.
          case Block.CallFragment(_, call) =>
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
          case Block.ScatterOneCall(_, _, call) =>
            // scatter with a single call
            collectSubJobs.aggregateResults(call, childJobsComplete)

          case Block.ScatterFullBlock(_, _) =>
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
    val exportedVars: Set[String] = block.outputs.map(_.name).toSet

    val jsOutputs: Map[String, JsValue] = processOutputs(env, fragResults, exportedVars)
    val jsOutputsDbgStr = jsOutputs.mkString("\n")
    dxApi.logger.traceLimited(s"""|JSON outputs:
                                  |${jsOutputsDbgStr}""".stripMargin)
    jsOutputs
  }
}

object WfFragRunner {
  // Concatenate the elements, until hitting a size limit
  private[exec] def buildLimitedSizeName(elements: Seq[String], maxLen: Int): String = {
    if (elements.isEmpty)
      return "[]"
    val (_, concat) = elements.tail.foldLeft((false, elements.head)) {
      case ((true, accu), _) =>
        // stopping condition reached, we have reached the size limit
        (true, accu)

      case ((false, accu), _) if accu.length >= maxLen =>
        // move into stopping condition
        (true, accu)

      case ((false, accu), elem) =>
        val tentative = accu + ", " + elem
        if (tentative.length > maxLen) {
          // not enough space
          (true, accu)
        } else {
          // still have space
          (false, tentative)
        }
    }
    "[" + concat + "]"
  }
}
