/**
An applets that gathers outputs from scatter jobs. This is necessary when
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
        GenFiles.result
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
  The parent scatter returns jbors to each of the collect output fields. This
allows it to return immediately, and not wait for the child jobs to complete.
Each scatter requires its own collect applet, because the output type is
the same as the scatter output type.

General case
  In the general case the scatter can have several calls. The collect job
has to wait for all child jobs, figure out which call generated them, and
gather outputs in appropriate groups.

workflow math {
    scatter (k in [2,3,5]) {
        call GenFiles  { input: len=k }
        call CalcSize  { input: ... }
        call MakeTable { input: ... }
    }
    output {
        GenFiles.result
        CalcSize.total
        MakeTable.volume
    }
}
```

  */
package dxWDL

// DX bindings
import com.dnanexus.{DXAPI, DXEnvironment, DXJob}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Path
import scala.collection.mutable.HashMap
import spray.json._
import Utils.{appletLog, jsonNodeOfJsValue, jsValueOfJsonNode}
import wdl4s.wdl.{WdlCall, WdlWorkflow}
import wdl4s.wdl.types._

object RunnerCollect {

    def findChildJobs() : Vector[DXJob] = {
        // get the parent job
        val dxEnv = DXEnvironment.create()
        val dxJob: DXJob = dxEnv.getJob()

        // find all the jobs we we are waiting for
        val req = JsObject("fields" -> JsObject("dependsOn" -> JsBoolean(true)))
        val retval: JsValue = jsValueOfJsonNode(
            DXAPI.jobDescribe(dxJob.getId,
                              jsonNodeOfJsValue(req),
                              classOf[JsonNode])
        )
        val childJobs:Vector[JsValue] = retval.asJsObject.fields.get("dependsOn") match {
            case Some(JsArray(a)) => a.toVector
            case _ => throw new Exception(s"Field dependsOn missing jobDescribe ${retval}")
        }
        childJobs.map{
            case JsString(id) => DXJob.getInstance(id)
            case _ => throw new Exception(s"Wrong type returned in dependsOn field ${retval}")
        }.toVector
    }

    // Describe all the scatter child jobs. Use a bulk-describe
    // for efficiency.
    def describeChildJobs(jobs: Vector[DXJob]) : Map[DXJob, (String, JsValue)] = {
        val jobInfoReq:Vector[JsValue] = jobs.map{ job =>
            JsObject(
                "id" -> JsString(job.getId),
                "describe" -> JsObject("outputs" -> JsBoolean(true),
                                       "executableName" -> JsBoolean(true))
            )
        }
        val req = JsObject("executions" -> JsArray(jobInfoReq))
        val retval: JsValue =
            jsValueOfJsonNode(
                DXAPI.systemDescribeExecutions(jsonNodeOfJsValue(req), classOf[JsonNode]))
        val infoVec = retval match {
            case JsArray(a) => a.map{ response =>
                val fields = response.asJsObject.fields
                val execName = fields.get("executableName") match {
                    case Some(JsString(name)) => name
                    case other => throw new Exception(s"wrong type for executableName ${other}")
                }
                val outputs = fields.get("outputs") match {
                    case None => throw new Exception(s"No outputs for a child job")
                    case Some(o) => o
                }
                (execName, outputs)
            }
            case _ => throw new Exception(s"Wrong type returned from bulk-describe ${retval}")
        }
        (jobs zip infoVec).toMap
    }

    // Collect all the values of a field from a group of jobs. Convert
    // into the correct WDL type.
    //
    // Assumptions: the jobs have already completed, and their files are closed.
    def collect(fieldName: String,
                wdlType: WdlType,
                jobDescs:Vector[(DXJob, JsValue)]) : (String, WdlVarLinks) = {
        // TODO:
        // The type could be optional, in which case the output field
        // will be generated only from a subset of the jobs.
        val jsVec = jobDescs.map{ case (dxJob, desc) =>
            desc.asJsObject.fields.get(fieldName) match {
                case None =>
                    throw new Exception(s"missing field ${fieldName} from child job ${dxJob}}")
                case Some(x) => x
            }
        }
        val wvl = WdlVarLinks(wdlType, DeclAttrs.empty, DxlValue(JsArray(jsVec)))
        (fieldName, wvl)
    }

    // Collect all outputs for a call
    def collectCallOutputs(call: WdlCall,
                           retvals: Vector[(DXJob, JsValue)]) : Map[String,JsValue] = {
        val wvlOutputs: Vector[(String, WdlVarLinks)] =
            call.outputs.map { caOut =>
                collect(caOut.unqualifiedName, caOut.wdlType, retvals)
            }.toVector
        val outputs:Map[String, JsValue] = wvlOutputs.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, wvl)) =>
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
        }

        // Add the call name as a prefix to all output names
        outputs.map{ case (varName, jsv) =>
            s"${call.unqualifiedName}_${varName}" -> jsv
        }.toMap
    }


    def apply(wf: WdlWorkflow,
              jobInputPath : Path,
              jobOutputPath : Path,
              jobInfoPath: Path) : Unit = {
        // Figure out input/output types
        val (inputSpec, outputSpec) = Utils.loadExecInfo

        // We cannot change the input fields, because this is a sub-job with the same
        // input/output spec as the parent scatter. Therefore, we need to computationally
        // figure out:
        //   1) child job-ids
        //   2) field names
        //   3) WDL types
        val childJobs:Vector[DXJob] = findChildJobs()

        // describe all the job outputs and which applet they were running
        val jobDescs:Map[DXJob, (String, JsValue)] = describeChildJobs(childJobs)

        // find the WDL tasks that were run
        val execNames: Set[String] = jobDescs.foldLeft(Set.empty[String]) {
            case (accu, (_, (execName,_))) =>
                accu + execName
        }

        // Map executable name to WDL task
        val execName2Call: Map[String, WdlCall] = wf.calls.map{ call =>
            val task = Utils.taskOfCall(call)
            val taskName = task.unqualifiedName
            if (!(execNames contains taskName))
                throw new Exception(s"Encountered unknown executable ${taskName}")
            taskName -> call
        }.toMap

        // group jobs by executable
        val jobGroups = HashMap.empty[WdlCall, Vector[(DXJob, JsValue)]]
        jobDescs.foreach { case (dxJob, (execName, retval)) =>
            val call = execName2Call(execName)
            val vec = jobGroups.get(call) match {
                case None => Vector((dxJob, retval))
                case Some(v) => v :+ (dxJob, retval)
            }
            jobGroups(call) = vec
        }

        // collect each call outputs
        val combinedOutputs = jobGroups.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (call, retvalVec)) =>
                val callOutputs = collectCallOutputs(call, retvalVec)
                accu ++ callOutputs
        }

        val json = JsObject(combinedOutputs)
        val ast_pp = json.prettyPrint
        appletLog(s"exported = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
