/**
An applet that gathers outputs from scatter jobs. This is necessary when
the output is a non-native DNAx type. For example, the math workflow
below calls a scatter where each job returns an array of files. The
GenFiles.result is a ragged array of files (Array[Array[File]]). The
conversion between these two types is difficult, and requires this applet.

```
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
package dxWDL.exec

// DX bindings
import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._
import wom.callable.Callable._
import wom.graph._
import wom.types._
import wom.values._
import dxWDL.base._
import dxWDL.dx._
import dxWDL.util._

import scala.collection.AbstractIterator

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
                          runtimeDebugLevel: Int,
                          typeAliases: Map[String, WomType]) {
  //private val verbose = runtimeDebugLevel >= 1
  private val maxVerboseLevel = runtimeDebugLevel == 2
  private val verbose = Verbose(runtimeDebugLevel >= 1, quiet = false, Set.empty)
  private val wdlVarLinksConverter = WdlVarLinksConverter(verbose, Map.empty, typeAliases)

  private def siblingJobs: Vector[DxExecution] = {
    val currentJobId = DxUtils.dxEnv.getJob
    val parentJob = DxJob(currentJobId).describe().parentJob match {
      case Some(job) => job
      case None =>
        throw new Exception(s"Can't get parent job for $currentJobId")
    }
    findChildExecutions(Some(parentJob), currentJobId).map(_.exec)
  }

  // Launch a subjob to continue a large scatter
  def launchContinue(childJobs: Vector[DxExecution],
                     start: Int,
                     exportTypes: Map[String, WomType],
                     isContinuation: Boolean = false): Map[String, WdlVarLinks] = {
    assert(childJobs.nonEmpty)

    val inputsWithStart = JsObject(
        inputsRaw.asJsObject.fields + (Utils.CONTINUE_START -> JsNumber(start))
    )
    val allChildJobs = if (isContinuation) {
      siblingJobs ++ childJobs
    } else {
      childJobs
    }
    val name = s"continue($start)"

    // Run a sub-job with the "collect" entry point.
    // We need to provide the exact same inputs.
    val dxSubJob: DxJob = DxUtils.runSubJob("continue",
                                            Some(instanceTypeDB.defaultInstanceType),
                                            inputsWithStart,
                                            allChildJobs,
                                            delayWorkspaceDestruction,
                                            maxVerboseLevel,
                                            Some(name))

    // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
    // is exactly the same as the parent, we can immediately exit the parent job.
    exportTypes.map {
      case (eVarName, womType) =>
        eVarName -> WdlVarLinks(womType, DxlExec(dxSubJob, eVarName))
    }
  }

  // Launch a subjob to collect the outputs
  def launchCollect(childJobs: Vector[DxExecution],
                    exportTypes: Map[String, WomType],
                    isContinuation: Boolean = false): Map[String, WdlVarLinks] = {
    assert(childJobs.nonEmpty)

    val allChildJobs = if (isContinuation) {
      siblingJobs ++ childJobs
    } else {
      childJobs
    }

    // Run a sub-job with the "collect" entry point.
    // We need to provide the exact same inputs.
    val dxSubJob: DxJob = DxUtils.runSubJob("collect",
                                            Some(instanceTypeDB.defaultInstanceType),
                                            inputsRaw,
                                            allChildJobs,
                                            delayWorkspaceDestruction,
                                            maxVerboseLevel,
                                            Some("collect"))

    // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
    // is exactly the same as the parent, we can immediately exit the parent job.
    exportTypes.map {
      case (eVarName, womType) =>
        eVarName -> WdlVarLinks(womType, DxlExec(dxSubJob, eVarName))
    }
  }

  private def parseOneResult(value: JsValue, excludeId: String): Option[ChildExecDesc] = {
    val fields = value.asJsObject.fields
    val (exec, desc) = fields.get("id") match {
      case Some(JsString(id)) if id == excludeId =>
        Utils.trace(verbose.on, s"Ignoring result for job ${id}")
        return None
      case Some(JsString(id)) if id.startsWith("job-") =>
        val job = DxJob.getInstance(id)
        val desc = fields("describe").asJsObject
        (job, desc)
      case Some(JsString(id)) if id.startsWith("analysis-") =>
        val analysis = DxAnalysis.getInstance(id)
        val desc = fields("describe").asJsObject
        (analysis, desc)
      case Some(other) =>
        throw new Exception(s"malformed id field ${other.prettyPrint}")
      case None =>
        throw new Exception(s"field id not found in ${value.prettyPrint}")
    }
    Utils.trace(verbose.on, s"parsing desc ${desc} for ${exec}")
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
    val request = JsObject(parentField ++ cursorField ++ limitField ++ describeField)
    val response = DXAPI.systemFindExecutions(DxUtils.jsonNodeOfJsValue(request),
                                              classOf[JsonNode],
                                              DxUtils.dxEnv)
    val responseJs: JsObject = DxUtils.jsValueOfJsonNode(response).asJsObject
    val results: Vector[ChildExecDesc] =
      responseJs.fields.get("results") match {
        case Some(JsArray(results)) => results.flatMap(res => parseOneResult(res, excludeId))
        case Some(other)            => throw new Exception(s"malformed results field ${other.prettyPrint}")
        case None                   => throw new Exception(s"missing results field ${response}")
      }
    (results, responseJs.fields("next"))
  }

  private def findChildExecutions(parentJob: Option[DxJob],
                                  excludeId: String,
                                  limit: Option[Int] = None): Vector[ChildExecDesc] = {

    new UnfoldIterator[Vector[ChildExecDesc], Option[JsValue]](Some(JsNull))({
      case None => None
      case Some(cursor: JsValue) =>
        submitRequest(parentJob, cursor, excludeId, limit) match {
          case (Vector(), _)     => None
          case (results, JsNull) => Some(results, None)
          case (results, next)   => Some(results, Some(next))
        }
    }).toVector.flatten.sortWith(_.seqNum < _.seqNum)
  }

  def executableFromSeqNum(): Vector[ChildExecDesc] = {
    // get the parent job
    val dxJob = DxJob(DxUtils.dxEnv.getJob)
    val parentJob: DxJob = dxJob.describe().parentJob.get
    val childExecs: Vector[ChildExecDesc] =
      findChildExecutions(Some(parentJob), dxJob.id)
    Utils.trace(verbose.on, s"childExecs=${childExecs}")
    childExecs
  }

  // collect field [name] from all child jobs, by looking at their
  // outputs. Coerce to the correct [womType].
  private def collectCallField(name: String,
                               womType: WomType,
                               childJobsComplete: Vector[ChildExecDesc]): WomValue = {
    val vec: Vector[WomValue] =
      childJobsComplete.flatMap { childExec =>
        val dxName = Utils.transformVarName(name)
        val fieldValue = childExec.outputs.get(dxName)
        (womType, fieldValue) match {
          case (WomOptionalType(_), None) =>
            // Optional field that has not been returned
            None
          case (WomOptionalType(_), Some(jsv)) =>
            Some(jobInputOutput.unpackJobInput(name, womType, jsv))
          case (_, None) =>
            // Required output that is missing
            throw new Exception(s"Could not find compulsory field <${name}> in results")
          case (_, Some(jsv)) =>
            Some(jobInputOutput.unpackJobInput(name, womType, jsv))
        }
      }
    WomArray(WomArrayType(womType), vec)
  }

  // aggregate call results
  def aggregateResults(call: CallNode,
                       childJobsComplete: Vector[ChildExecDesc]): Map[String, WdlVarLinks] = {
    call.callable.outputs.map { cot: OutputDefinition =>
      val fullName = s"${call.identifier.workflowLocalName}.${cot.name}"
      val womType = cot.womType
      val value: WomValue = collectCallField(cot.name, womType, childJobsComplete)
      val wvl = wdlVarLinksConverter.importFromWDL(value.womType, value)
      fullName -> wvl
    }.toMap
  }

  // collect results from a sub-workflow generated for the sole purpose of calculating
  // a sub-block.
  def aggregateResultsFromGeneratedSubWorkflow(
      execLinkInfo: ExecLinkInfo,
      childJobsComplete: Vector[ChildExecDesc]
  ): Map[String, WdlVarLinks] = {
    execLinkInfo.outputs.map {
      case (name, womType) =>
        val value: WomValue = collectCallField(name, womType, childJobsComplete)
        val wvl = wdlVarLinksConverter.importFromWDL(value.womType, value)
        name -> wvl
    }
  }
}

// copy the UnfoldIterator from scala 2.13
private final class UnfoldIterator[A, S](init: S)(f: S => Option[(A, S)])
    extends AbstractIterator[A] {
  private[this] var state: S = init
  private[this] var nextResult: Option[(A, S)] = null

  override def hasNext: Boolean = {
    if (nextResult eq null) {
      nextResult = {
        val res = f(state)
        if (res eq null) throw new NullPointerException("null during unfold")
        res
      }
      state = null.asInstanceOf[S] // allow GC
    }
    nextResult.isDefined
  }

  override def next(): A = {
    if (hasNext) {
      val (value, newState) = nextResult.get
      state = newState
      nextResult = null
      value
    } else Iterator.empty.next()
  }
}
