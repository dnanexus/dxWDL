/**
An applet that gathers outputs from scatter jobs. This is necessary when
the output is a non-native DNAx type. For example, the math workflow
below calls a scatter where each job returns an array of files. The
`GenFiles.result` is a ragged array of files (`Array[Array[File]]`). The
conversion between these two types is difficult, and requires this applet.

`
task GenFiles {
  ...
  output {
      Array[File] result
  }
}

workflow math {
    scatter (k in [2,3,5]) {
        call GenFiles { input: len=k }
    }
    output {
        Array[Array[File] result = GenFiles.result
    }
}
```

Diagram

          scatter
         /   | .. \
   child-jobs      \
                    \
                     collect

Design
  The collect applet takes three inputs:
1) job-ids     (array of strings)
2) field names (array of strings)
3) WDL types   (array of strings)

  It waits for all the scatter child jobs to complete, using the dependsOn field.
For each field F:
  - Get the value of F from all the child jobs
  - Merge. This is complex but doable for non native dx types. For
    example, to merge the GenFiles output, we need to merge an array
    of array of files into a hash with a companion flat array of
    files.

outputs: the merged value for each field.

Larger context
  The parent scatter returns ebors to each of the collect output fields. This
allows it to return immediately, and not wait for the child jobs to complete.
Each scatter requires its own collect applet, because the output type is
the same as the scatter output type.

Note: the compiler ensures that the scatter will call exactly one call.
  */
package dx.exec

import dx.api.{DxApi, DxExecution, DxFindExecutions, DxJob, InstanceTypeDB}
import dx.core.io.ExecLinkInfo
import dx.core.languages.wdl.{DxlExec, WdlVarLinks, WdlVarLinksConverter}
import spray.json._
import wdlTools.eval.WdlValues
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}

case class ChildExecDesc(execName: String,
                         seqNum: Int,
                         outputs: Map[String, JsValue],
                         exec: DxExecution)

case class CollectSubJobs(jobInputOutput: JobInputOutput,
                          inputsRaw: JsValue,
                          instanceTypeDB: InstanceTypeDB,
                          delayWorkspaceDestruction: Option[Boolean],
                          dxApi: DxApi,
                          typeAliases: Map[String, WdlTypes.T]) {
  private val wdlVarLinksConverter = WdlVarLinksConverter(dxApi, Map.empty, typeAliases)

  // Launch a subjob to collect the outputs
  def launch(childJobs: Vector[DxExecution],
             exportTypes: Map[String, WdlTypes.T]): Map[String, WdlVarLinks] = {
    assert(childJobs.nonEmpty)

    // Run a sub-job with the "collect" entry point.
    // We need to provide the exact same inputs.
    val dxSubJob: DxJob = dxApi.runSubJob("collect",
                                          Some(instanceTypeDB.defaultInstanceType),
                                          inputsRaw,
                                          childJobs,
                                          delayWorkspaceDestruction)

    // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
    // is exactly the same as the parent, we can immediately exit the parent job.
    exportTypes.map {
      case (eVarName, wdlType) =>
        eVarName -> WdlVarLinks(wdlType, DxlExec(dxSubJob, eVarName))
    }
  }

  private def findChildExecs(): Vector[DxExecution] = {
    // get the parent job
    val dxJob = dxApi.currentJob
    val parentJob: DxJob = dxJob.describe().parentJob.get
    val childExecs: Vector[DxExecution] = DxFindExecutions(dxApi).apply(Some(parentJob))
    // make sure the collect subjob is not included. Theoretically,
    // it should not be returned as a search result, becase we did
    // not explicitly ask for subjobs. However, let's make
    // sure.
    childExecs.filter(_ != dxJob)
  }

  // Describe all the scatter child jobs. Use a bulk-describe
  // for efficiency.
  private def describeChildExecs(execs: Vector[DxExecution]): Vector[ChildExecDesc] = {
    val jobInfoReq: Vector[JsValue] = execs.map { job =>
      JsObject(
          "id" -> JsString(job.getId),
          "describe" -> JsObject("outputs" -> JsBoolean(true),
                                 "executableName" -> JsBoolean(true),
                                 "properties" -> JsBoolean(true))
      )
    }
    val request = Map("executions" -> JsArray(jobInfoReq))
    dxApi.logger.info(s"bulk-describe request=${request}")
    val response: JsObject = dxApi.executionsDescribe(request)
    val results: Vector[JsValue] = response.fields.get("results") match {
      case Some(JsArray(x)) => x
      case _                => throw new Exception(s"wrong type for executableName ${response}")
    }
    (execs zip results).map {
      case (dxExec, desc) =>
        val fields = desc.asJsObject.fields.get("describe") match {
          case Some(JsObject(fields)) => fields
          case _                      => throw new Exception(s"result does not contains a describe field ${desc}")
        }
        val execName = fields.get("executableName") match {
          case Some(JsString(name)) => name
          case _                    => throw new Exception(s"wrong type for executableName ${desc}")
        }
        val seqNum = fields.get("properties") match {
          case Some(obj) =>
            obj.asJsObject.getFields("seq_number") match {
              case Seq(JsString(seqNum)) => seqNum.toInt
              case _                     => throw new Exception(s"wrong value for properties ${desc}, ${obj}")
            }
          case _ => throw new Exception(s"wrong type for properties ${desc}")
        }
        val outputs = fields.get("output") match {
          case None    => throw new Exception(s"No output field for a child job ${desc}")
          case Some(o) => o
        }
        ChildExecDesc(execName, seqNum, outputs.asJsObject.fields, dxExec)
    }
  }

  def executableFromSeqNum(): Vector[ChildExecDesc] = {
    // We cannot change the input fields, because this is a sub-job with the same
    // input/output spec as the parent scatter. Therefore, we need to computationally
    // figure out:
    //   1) child job-ids
    //   2) field names
    //   3) WDL types
    val childExecs: Vector[DxExecution] = findChildExecs()
    System.err.println(s"childExecs=${childExecs}")

    // describe all the job outputs and which applet they were running
    val execDescs: Vector[ChildExecDesc] = describeChildExecs(childExecs)
    System.err.println(s"execDescs=${execDescs}")

    // sort from low to high sequence number
    execDescs.sortWith(_.seqNum < _.seqNum)
  }

  // collect field [name] from all child jobs, by looking at their
  // outputs. Coerce to the correct [wdlType].
  private def collectCallField(name: String,
                               wdlType: WdlTypes.T,
                               childJobsComplete: Vector[ChildExecDesc]): WdlValues.V = {
    val vec: Vector[WdlValues.V] =
      childJobsComplete.flatMap { childExec =>
        val dxName = WdlVarLinksConverter.transformVarName(name)
        val fieldValue = childExec.outputs.get(dxName)
        (wdlType, fieldValue) match {
          case (WdlTypes.T_Optional(_), None) =>
            // Optional field that has not been returned
            None
          case (WdlTypes.T_Optional(_), Some(jsv)) =>
            Some(jobInputOutput.unpackJobInput(name, wdlType, jsv))
          case (_, None) =>
            // Required output that is missing
            throw new Exception(s"Could not find compulsory field <${name}> in results")
          case (_, Some(jsv)) =>
            Some(jobInputOutput.unpackJobInput(name, wdlType, jsv))
        }
      }
    WdlValues.V_Array(vec)
  }

  // aggregate call results
  def aggregateResults(call: TAT.Call,
                       childJobsComplete: Vector[ChildExecDesc]): Map[String, WdlVarLinks] = {
    call.callee.output.map {
      case (oName, wdlType) =>
        val fullName = s"${call.actualName}.${oName}"
        val value: WdlValues.V = collectCallField(oName, wdlType, childJobsComplete)

        // We get an array from collecting the values of a particular field
        val arrayType = WdlTypes.T_Array(wdlType, nonEmpty = false)
        val wvl = wdlVarLinksConverter.importFromWDL(arrayType, value)
        fullName -> wvl
    }
  }

  // collect results from a sub-workflow generated for the sole purpose of calculating
  // a sub-block.
  def aggregateResultsFromGeneratedSubWorkflow(
      execLinkInfo: ExecLinkInfo,
      childJobsComplete: Vector[ChildExecDesc]
  ): Map[String, WdlVarLinks] = {
    execLinkInfo.outputs.map {
      case (name, wdlType) =>
        val value: WdlValues.V = collectCallField(name, wdlType, childJobsComplete)

        // We get an array from collecting the values of a particular field
        val arrayType = WdlTypes.T_Array(wdlType, nonEmpty = false)
        val wvl = wdlVarLinksConverter.importFromWDL(arrayType, value)
        name -> wvl
    }
  }
}
