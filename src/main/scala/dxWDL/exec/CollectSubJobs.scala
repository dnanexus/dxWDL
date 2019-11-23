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

case class ChildExecDesc(execName: String,
                         seqNum: Int,
                         outputs: Map[String, JsValue],
                         exec: DxExecution)

case class CollectSubJobs(jobInputOutput : JobInputOutput,
                          inputsRaw : JsValue,
                          instanceTypeDB : InstanceTypeDB,
                          runtimeDebugLevel: Int,
                          typeAliases: Map[String, WomType]) {
    //private val verbose = runtimeDebugLevel >= 1
    private val maxVerboseLevel = (runtimeDebugLevel == 2)
    private val verbose = Verbose(runtimeDebugLevel >= 1, false, Set.empty)
    private val wdlVarLinksConverter = WdlVarLinksConverter(verbose, Map.empty, typeAliases)

    // Launch a subjob to collect the outputs
    def launch(childJobs: Vector[DxExecution],
               exportTypes: Map[String, WomType]) : Map[String, WdlVarLinks] = {
        assert(!childJobs.isEmpty)

        // Run a sub-job with the "collect" entry point.
        // We need to provide the exact same inputs.
        val dxSubJob : DxJob = DxUtils.runSubJob("collect",
                                                 Some(instanceTypeDB.defaultInstanceType),
                                                 inputsRaw,
                                                 childJobs,
                                                 maxVerboseLevel)

        // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
        // is exactly the same as the parent, we can immediately exit the parent job.
        exportTypes.map{
            case (eVarName, womType) =>
                eVarName -> WdlVarLinks(womType, DxlExec(dxSubJob, eVarName))
        }.toMap
    }

    private def findChildExecs() : Vector[DxExecution] = {
         // get the parent job
        val dxJob = DxJob(DxUtils.dxEnv.getJob())
        val parentJob: DxJob = dxJob.getParentJob()

        val childExecs : Vector[DxExecution] = DxFindExecutions.apply(Some(parentJob))

        // make sure the collect subjob is not included. Theoretically,
        // it should not be returned as a search result, becase we did
        // not explicitly ask for subjobs. However, let's make
        // sure.
        childExecs.filter(_ != dxJob)
    }

    // Describe all the scatter child jobs. Use a bulk-describe
    // for efficiency.
    private def describeChildExecs(execs: Vector[DxExecution]) : Vector[ChildExecDesc] = {
        val jobInfoReq:Vector[JsValue] = execs.map{ job =>
            JsObject(
                "id" -> JsString(job.getId),
                "describe" -> JsObject("outputs" -> JsBoolean(true),
                                       "executableName" -> JsBoolean(true),
                                       "properties" -> JsBoolean(true))
            )
        }
        val req = JsObject("executions" -> JsArray(jobInfoReq))
        System.err.println(s"bulk-describe request=${req}")
        val retval: JsValue =
            DxUtils.jsValueOfJsonNode(
                DXAPI.systemDescribeExecutions(DxUtils.jsonNodeOfJsValue(req), classOf[JsonNode]))
        val results:Vector[JsValue] = retval.asJsObject.fields.get("results") match {
            case Some(JsArray(x)) => x.toVector
            case _ => throw new Exception(s"wrong type for executableName ${retval}")
        }
        (execs zip results).map { case (dxExec, desc) =>
            val fields = desc.asJsObject.fields.get("describe") match {
                case Some(JsObject(fields)) => fields
                case _ => throw new Exception(s"result does not contains a describe field ${desc}")
            }
            val execName = fields.get("executableName") match {
                case Some(JsString(name)) => name
                case _ => throw new Exception(s"wrong type for executableName ${desc}")
            }
            val seqNum = fields.get("properties") match {
                case Some(obj) =>
                    obj.asJsObject.getFields("seq_number") match {
                        case Seq(JsString(seqNum)) => seqNum.toInt
                        case _ => throw new Exception(s"wrong value for properties ${desc}, ${obj}")
                    }
                case _ => throw new Exception(s"wrong type for properties ${desc}")
            }
            val outputs = fields.get("output") match {
                case None => throw new Exception(s"No output field for a child job ${desc}")
                case Some(o) => o
            }
            ChildExecDesc(execName, seqNum, outputs.asJsObject.fields, dxExec)
        }.toVector
    }

    def executableFromSeqNum() : Vector[ChildExecDesc] = {
        // We cannot change the input fields, because this is a sub-job with the same
        // input/output spec as the parent scatter. Therefore, we need to computationally
        // figure out:
        //   1) child job-ids
        //   2) field names
        //   3) WDL types
        val childExecs:Vector[DxExecution] = findChildExecs()
        System.err.println(s"childExecs=${childExecs}")

        // describe all the job outputs and which applet they were running
        val execDescs:Vector[ChildExecDesc] = describeChildExecs(childExecs)
        System.err.println(s"execDescs=${execDescs}")

        // sort from low to high sequence number
        execDescs.sortWith(_.seqNum < _.seqNum)
    }

    // collect field [name] from all child jobs, by looking at their
    // outputs. Coerce to the correct [womType].
    private def collectCallField(name: String,
                                 womType: WomType,
                                 childJobsComplete: Vector[ChildExecDesc]) : WomValue = {
        val vec : Vector[WomValue] =
            childJobsComplete.flatMap{
                case childExec =>
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
            }.toVector
        WomArray(WomArrayType(womType), vec)
    }

    // aggregate call results
    def aggregateResults(call: CallNode,
                         childJobsComplete: Vector[ChildExecDesc]) : Map[String, WdlVarLinks] = {
        call.callable.outputs.map{ cot : OutputDefinition =>
            val fullName = s"${call.identifier.workflowLocalName}.${cot.name}"
            val womType = cot.womType
            val value : WomValue = collectCallField(cot.name,
                                                    womType,
                                                    childJobsComplete)
            val wvl = wdlVarLinksConverter.importFromWDL(value.womType, value)
            fullName -> wvl
        }.toMap
    }

    // collect results from a sub-workflow generated for the sole purpose of calculating
    // a sub-block.
    def aggregateResultsFromGeneratedSubWorkflow(execLinkInfo: ExecLinkInfo,
                                                 childJobsComplete: Vector[ChildExecDesc])
            : Map[String, WdlVarLinks] = {
        execLinkInfo.outputs.map{
            case (name, womType) =>
                val value : WomValue = collectCallField(name,
                                                        womType,
                                                        childJobsComplete)
                val wvl = wdlVarLinksConverter.importFromWDL(value.womType, value)
                name -> wvl
        }.toMap
    }
}
