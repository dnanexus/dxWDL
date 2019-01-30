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
package dxWDL.runner

// DX bindings
import com.dnanexus.{DXAPI, DXEnvironment, DXExecution, DXJob, DXSearch}
import com.fasterxml.jackson.databind.JsonNode
import scala.collection.JavaConverters._
import spray.json._
import wom.values._
import wom.types._

import dxWDL.util._

object Collect {

    case class ChildExecDesc(execName: String,
                             unqCallName: String,
                             seqNum: Int,
                             outputs: Map[String, JsValue],
                             exec: DXExecution)

    def findChildExecs() : Vector[DXExecution] = {
        // get the parent job
        val dxEnv = DXEnvironment.create()
        val dxJob = dxEnv.getJob()
        val parentJob: DXJob = dxJob.describe().getParentJob()

        val childExecs : Vector[DXExecution] = DXSearch.findExecutions()
            .withParentJob(parentJob)
            .execute().asList().asScala.toVector

        // make sure the collect subjob is not included. Theoretically,
        // it should not be returned as a search result, becase we did
        // not explicitly ask for subjobs. However, let's make
        // sure.
        childExecs.filter(_ != dxJob)
    }

    // Describe all the scatter child jobs. Use a bulk-describe
    // for efficiency.
    def describeChildExecs(execs: Vector[DXExecution]) : Vector[ChildExecDesc] = {
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
            Utils.jsValueOfJsonNode(
                DXAPI.systemDescribeExecutions(Utils.jsonNodeOfJsValue(req), classOf[JsonNode]))
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
            val (unqCallName, seqNum) = fields.get("properties") match {
                case Some(obj) =>
                    obj.asJsObject.getFields("call", "seq_number") match {
                        case Seq(JsString(unqCallName), JsString(seqNum)) =>
                            (unqCallName, seqNum.toInt)
                        case _ => throw new Exception(s"wrong value for properties ${desc}, ${obj}")
                    }
                case _ => throw new Exception(s"wrong type for properties ${desc}")
            }
            val outputs = fields.get("output") match {
                case None => throw new Exception(s"No output field for a child job ${desc}")
                case Some(o) => o
            }
            ChildExecDesc(execName, unqCallName, seqNum, outputs.asJsObject.fields, dxExec)
        }.toVector
    }

    def executableFromSeqNum() : Vector[ChildExecDesc] = {
        // We cannot change the input fields, because this is a sub-job with the same
        // input/output spec as the parent scatter. Therefore, we need to computationally
        // figure out:
        //   1) child job-ids
        //   2) field names
        //   3) WDL types
        val childExecs:Vector[DXExecution] = findChildExecs()
        System.err.println(s"childExecs=${childExecs}")

        // describe all the job outputs and which applet they were running
        val execDescs:Vector[ChildExecDesc] = describeChildExecs(childExecs)
        System.err.println(s"execDescs=${execDescs}")

        // sort from low to high sequence number
        execDescs.sortWith(_.seqNum < _.seqNum)
    }

    // collect
    private def collectCallField(name: String,
                                 womType: WomType,
                                 childJobsComplete: Vector[ChildExecDesc]) : WomValue = {
        val vec : Vector[WomValue] =
            childJobsComplete.map{ childExec =>
                childExec.outputs.get(name) match {
                    case None =>
                        throw new Exception(s"Need to handle optionals here womType=${womType}")
                    case Some(jsv) =>
                        val wvl = WdlVarLinks.importFromDxExec(womType, jsv)
                        wvl.
                }
            }.toVector
        WomArray(WomArrayType(womType), vec)
    }

    // aggregate call results
    def aggregateResults(call: CallNode,
                         childJobsComplete: Vector[ChildExecDesc]) : Map[String, WdlVarLinks] = {
        call.callable.outputs{ cot =>
            val fullName = s"${call.identifier.workflowLocalName}.${cot.localName}"
            val womType = cot.womType
            val value : WomValue = collectCallField(cot.name, womType, childJobsComplete)
            fullName -> WdlVarLinks.importFromWDL(value.womType, value)
        }.toMap
    }
}
