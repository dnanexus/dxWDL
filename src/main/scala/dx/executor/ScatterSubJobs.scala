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
package dx.executor

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import dx.api.{
  DxAnalysis,
  DxAnalysisDescribe,
  DxApi,
  DxExecution,
  DxFindExecutions,
  DxJob,
  DxJobDescribe,
  DxObject,
  DxUtils,
  Field,
  InstanceTypeDB
}
import dx.core.ir
import dx.core.ir.ParameterLinkExec
import dx.core.languages.wdl.{ParameterLinkExec, ParameterLinkSerde, WdlDxLink}
import dx.executor.wdl.WdlExecutableLink
import spray.json._
import wdlTools.eval.WdlValues
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.Logger

case class ChildExecDesc(execName: String,
                         seqNum: Int,
                         outputs: Map[String, JsValue],
                         exec: DxExecution)

object CollectSubJobs {
  val SeqNumber = "seq_number"
}

case class CollectSubJobs(jobInputOutput: JobInputOutput,
                          inputsRaw: JsValue,
                          instanceTypeDB: InstanceTypeDB,
                          delayWorkspaceDestruction: Option[Boolean],
                          dxApi: DxApi,
                          logger: Logger = Logger.get,
                          wdlVarLinksConverter: ParameterLinkSerde) {
  // Launch a subjob to collect the outputs
  def launch(childJobs: Vector[DxExecution],
             exportTypes: Map[String, WdlTypes.T]): Map[String, WdlDxLink] = {
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
        eVarName -> WdlDxLink(wdlType, ir.ParameterLinkExec(dxSubJob, eVarName))
    }
  }

  private def parseOneResult(value: JsValue, excludeId: String): Option[ChildExecDesc] = {
    val fields = value.asJsObject.fields
    val (exec, desc) = fields.get("id") match {
      case Some(JsString(id)) if id == excludeId =>
        logger.trace(s"Ignoring result for job ${id}")
        return None
      case Some(JsString(id)) if id.startsWith("job-") =>
        val job = dxApi.job(id)
        val desc = fields("describe").asJsObject
        (job, desc)
      case Some(JsString(id)) if id.startsWith("analysis-") =>
        val analysis = dxApi.analysis(id)
        val desc = fields("describe").asJsObject
        (analysis, desc)
      case Some(other) =>
        throw new Exception(s"malformed id field ${other.prettyPrint}")
      case None =>
        throw new Exception(s"field id not found in ${value.prettyPrint}")
    }
    logger.trace(s"parsing desc ${desc} for ${exec}")
    val (execName, properties, output) =
      desc.getFields("executableName", "properties", "output") match {
        case Seq(JsString(execName), properties, JsObject(output)) =>
          (execName, DxObject.parseJsonProperties(properties), output)
      }
    val seqNum = properties(CollectSubJobs.SeqNumber).toInt
    Some(ChildExecDesc(execName, seqNum, output, exec))
  }

  private def submitRequest(
      parentJob: Option[DxJob],
      cursor: JsValue,
      excludeId: String,
      limit: Option[Int]
  ): (Vector[ChildExecDesc], JsValue) = {
    val parentField: Map[String, JsValue] = parentJob match {
      case None      => Map.empty
      case Some(job) => Map("parentJob" -> JsString(job.getId))
    }
    val cursorField: Map[String, JsValue] = cursor match {
      case JsNull      => Map.empty
      case cursorValue => Map("starting" -> cursorValue)
    }
    val limitField: Map[String, JsValue] = limit match {
      case None    => Map.empty
      case Some(i) => Map("limit" -> JsNumber(i))
    }
    val describeField: Map[String, JsValue] = Map(
        "describe" -> DxObject
          .requestFields(Set(Field.Output, Field.ExecutableName, Field.Properties))
    )
    val response = dxApi.findExecutions(parentField ++ cursorField ++ limitField ++ describeField)
    val results: Vector[ChildExecDesc] =
      response.fields.get("results") match {
        case Some(JsArray(results)) => results.flatMap(res => parseOneResult(res, excludeId))
        case Some(other)            => throw new Exception(s"malformed results field ${other.prettyPrint}")
        case None                   => throw new Exception(s"missing results field ${response}")
      }
    (results, response.fields("next"))
  }

  private def findChildExecutions(parentJob: Option[DxJob],
                                  excludeId: String,
                                  limit: Option[Int] = None): Vector[ChildExecDesc] = {

    Iterator
      .unfold[Vector[ChildExecDesc], Option[JsValue]](Some(JsNull)) {
        case None => None
        case Some(cursor: JsValue) =>
          submitRequest(parentJob, cursor, excludeId, limit) match {
            case (Vector(), _)     => None
            case (results, JsNull) => Some(results, None)
            case (results, next)   => Some(results, Some(next))
          }
      }
      .toVector
      .flatten
      .sortWith(_.seqNum < _.seqNum)
  }

  def executableFromSeqNum(): Vector[ChildExecDesc] = {
    // get the parent job
    val dxJob = dxApi.currentJob
    val parentJob: DxJob = dxJob.describe().parentJob.get
    val childExecs: Vector[ChildExecDesc] =
      findChildExecutions(Some(parentJob), dxJob.id)
    logger.trace(s"execExecs=${childExecs}")
    childExecs
  }

  // collect field [name] from all child jobs, by looking at their
  // outputs. Coerce to the correct [wdlType].
  private def collectCallField(name: String,
                               wdlType: WdlTypes.T,
                               childJobsComplete: Vector[ChildExecDesc]): WdlValues.V = {
    val vec: Vector[WdlValues.V] =
      childJobsComplete.flatMap { childExec =>
        val dxName = ParameterLinkSerde.encodeDots(name)
        val fieldValue = childExec.outputs.get(dxName)
        (wdlType, fieldValue) match {
          case (WdlTypes.T_Optional(_), None) =>
            // Optional field that has not been returned
            None
          case (WdlTypes.T_Optional(_), Some(jsv)) =>
            Some(wdlVarLinksConverter.unpackJobInput(name, wdlType, jsv))
          case (_, None) =>
            // Required output that is missing
            throw new Exception(s"Could not find compulsory field <${name}> in results")
          case (_, Some(jsv)) =>
            Some(wdlVarLinksConverter.unpackJobInput(name, wdlType, jsv))
        }
      }
    WdlValues.V_Array(vec)
  }

  // aggregate call results
  def aggregateResults(call: TAT.Call,
                       childJobsComplete: Vector[ChildExecDesc]): Map[String, WdlDxLink] = {
    call.callee.output.map {
      case (oName, wdlType) =>
        val fullName = s"${call.actualName}.${oName}"
        val value: WdlValues.V = collectCallField(oName, wdlType, childJobsComplete)

        // We get an array from collecting the values of a particular field
        val arrayType = WdlTypes.T_Array(wdlType, nonEmpty = false)
        val wvl = wdlVarLinksConverter.createLink(arrayType, value)
        fullName -> wvl
    }
  }

  // collect results from a sub-workflow generated for the sole purpose of calculating
  // a sub-block.
  def aggregateResultsFromGeneratedSubWorkflow(
      execLinkInfo: WdlExecutableLink,
      childJobsComplete: Vector[ChildExecDesc]
  ): Map[String, WdlDxLink] = {
    execLinkInfo.outputs.map {
      case (name, wdlType) =>
        val value: WdlValues.V = collectCallField(name, wdlType, childJobsComplete)

        // We get an array from collecting the values of a particular field
        val arrayType = WdlTypes.T_Array(wdlType, nonEmpty = false)
        val wvl = wdlVarLinksConverter.createLink(arrayType, value)
        name -> wvl
    }
  }
}
