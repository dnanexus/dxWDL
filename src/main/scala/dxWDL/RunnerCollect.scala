/**
An applets that gathers outputs from scatter jobs. This is necessary when
the output is a non-native DNAx type. For example, the math workflow
below calls a scatter where each job returns an array of files. The
GenFiles.result is a ragged array of files (Array[Array[File]]). The
conversion between these two types is difficult, and requires this applet.

```
# Create an array of integers from an integer.
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
  */
package dxWDL

// DX bindings
import com.dnanexus.{DXAPI, DXJob}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Path
import spray.json._
import Utils.{appletLog, jsonNodeOfJsValue, jsValueOfJsonNode}
import wdl4s.wdl.{WdlWorkflow}
import wdl4s.wdl.types._
import wdl4s.wdl.values._

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
        val infoVec:Vector[JsValue] = retval match {
            case JsArray(a) => a.toVector
            case _ => throw new Exception(s"Wrong type returned from jobDescribe ${retval}")
        }
        infoVec.map{
            case JsString(id) => DXJob.getInstance(id)
            case _ => throw new Exception(s"Wrong type returned from jobDescribe ${retval}")
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
                val execName = response.get("executableName") match {
                    case JsString(id) => id
                    case other => throw new Exception(s"wrong type for executableName ${other}")
                }
                val outputs = response.get("outputs") match {
                    case None => throw new Exception(s"No outputs for a child job")
                    case Some(o) => o
                }
                (execName, outputs)
            }
            case _ => throw new Exception(s"Wrong type returned from bulk-describe ${retval}")
        }
        jobs zip infoVec
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

    // Safely extract an array of strings from the input.
    // Validate any possible input.
    def extractStringArray(inputs: Map[String, WdlVarLinks],
                           fieldName: String) : Vector[String] = {
        val wvl:WdlVarLinks = inputs.get(fieldName) match {
            case None => throw new Exception(s"${fieldName} not provided")
            case Some(x) => x
        }
        if (wvl.wdlType != WdlArrayType(WdlStringType))
            throw new Exception(s"${fieldName} has the wrong type ${wvl.wdlType.toWdlString}")

        val wValue = WdlVarLinks.eval(wvl, false)
        val strArr:Vector[String] = wValue match {
            case WdlArray(WdlArrayType(WdlStringType), arr) =>
                arr.map{
                    case WdlString(s) => s
                    case other => throw new Exception(s"invalid value ${other}")
                }.toVector
            case other =>
                throw new Exception(s"invalid value ${other}")
        }
        strArr
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

        val fieldNames = extractStringArray(inputs, "fieldNames")
        val wdlTypes:Vector[WdlType] = extractStringArray(inputs, "wdlTypes").map{
            s => WdlType.fromWdlString(s)
        }
        if (fieldNames.length != wdlTypes.length)
            throw new Exception(s"""|The number of fields must be the same as the number
                                    |of types ${fieldNames.length} != ${wdlTypes.length}"""
                                    .stripMargin.replaceAll("\n", " "))

        // collect each field
        val wvlOutputs: Vector[(String, WdlVarLinks)] =
            (fieldNames zip wdlTypes).map { case (fieldName, wdlType) =>
                collect(fieldName, wdlType, jobDescs)
            }
        val outputs:Map[String,JsValue] = wvlOutputs.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, wvl)) =>
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
        }

        val json = JsObject(outputs.toMap)
        val ast_pp = json.prettyPrint
        appletLog(s"exported = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
