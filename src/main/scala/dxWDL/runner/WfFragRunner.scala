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

package dxWDL.runner

import com.dnanexus._
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Path, Paths, Files}
import scala.util.{Failure, Success}
import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.types._
import wom.values._

import dxWDL.util._

case class WfFragRunner(wf: WorkflowDefinition,
                        instanceTypeDB: InstanceTypeDB,
                        dxPathConfig : DxPathConfig,
                        execLinkInfo: Map[String, ExecLinkInfo],
                        runtimeDebugLevel: Int) {
    private val verbose = runtimeDebugLevel >= 1
    //private val maxVerboseLevel = (runtimeDebugLevel == 2)

    val dxIoFunctions = DxIoFunctions(dxPathConfig)

    def apply(inputs: Map[String, JsValue]) : Map[String, JsValue] = {
        throw new NotImplementedError("TODO")

        // 1. Convert the inputs to WOM values

        // 2. setup an environment for running the block
        // 3. Extract the block from the workflow

        // convert from WVL to JSON
        /*
        val outputFields: Map[String, JsValue] =
            wvlVarOutputs.foldLeft(Map.empty[String, JsValue]) {
                case (accu, (varName, wvl)) =>
                    val fields = WdlVarLinks.genFields(wvl, varName)
                    accu ++ fields.toMap
            }*/
    }


}

object WfFragRunner {
    // Load from disk a mapping of applet name to id. We
    // need this in order to call the right version of other
    // applets.
    private def loadLinkInfo(dxProject: DXProject,
                             verbose: Boolean) : Map[String, ExecLinkInfo] = {
        Utils.appletLog(verbose, s"Loading link information")
        val linkSourceFile: Path = Paths.get("/" + Utils.LINK_INFO_FILENAME)
        if (!Files.exists(linkSourceFile)) {
            Map.empty
        } else {
            val info: String = Utils.readFileContent(linkSourceFile)
            try {
                info.parseJson.asJsObject.fields.map {
                    case (key:String, jso) =>
                        key -> ExecLinkInfo.readJson(jso, dxProject)
                    case _ =>
                        throw new AppInternalException(s"Bad JSON")
                }.toMap
            } catch {
                case e : Throwable =>
                    throw new AppInternalException(s"Link JSON information is badly formatted ${info}")
            }
        }
    }

    def make(wf: WorkflowDefinition,
              wfSourceCode : String,
              instanceTypeDB: InstanceTypeDB,
              dxPathConfig : DxPathConfig,
              runtimeDebugLevel: Int) : WfFragRunner = {
        val verbose = runtimeDebugLevel >= 1
        Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
        Utils.appletLog(verbose, s"Workflow source code:")
        Utils.appletLog(verbose, wfSourceCode, 10000)
        Utils.appletLog(verbose, s"Inputs: ${inputs}")

        // Get handles for the referenced dx:applets
        val dxEnv = DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()
        val execLinkInfo = loadLinkInfo(dxProject, verbose)
        Utils.appletLog(verbose, s"link info=${execLinkInfo}")

        // Run the workflow
        new WfFragRunner(wf, instanceTypeDB, dxPathConfig, execLinkInfo, runtimeDebugLevel)
    }
}
