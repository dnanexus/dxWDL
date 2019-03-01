// Execute a WDL workflow with no calls, and no expressions, only inputs and outputs.
// It could have expressions in the output section.

package dxWDL.exec

import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.values._

import dxWDL.util._

case class WfInputs(wf: WorkflowDefinition,
                    wfSourceCode: String,
                    instanceTypeDB: InstanceTypeDB,
                    execLinkInfo: Map[String, ExecLinkInfo],
                    dxPathConfig : DxPathConfig,
                    dxIoFunctions : DxIoFunctions,
                    inputsRaw : JsValue,
                    fragInputOutput : WfFragInputOutput,
                    runtimeDebugLevel: Int) {
    private val verbose = runtimeDebugLevel >= 1
    //private val maxVerboseLevel = (runtimeDebugLevel == 2)

    def apply(inputs: Map[String, WomValue]) : Map[String, JsValue] = {
        Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
        Utils.appletLog(verbose, s"Environment: ${inputs}")
        val dbgInputs = wf.inputs.map{
            WomPrettyPrintApproxWdl.apply(_)
        }.mkString("\n")
        Utils.appletLog(verbose, s"""|Artificial applet for unlocked workflow inputs
                                     |${dbgInputs}
                                     |""".stripMargin)

        // convert the WDL values to JSON
        val outputFields:Map[String, JsValue] = inputs.map {
            case (outputVarName, womValue) =>
                val wvl = WdlVarLinks.importFromWDL(womValue.womType, womValue)
                WdlVarLinks.genFields(wvl, outputVarName)
        }.toList.flatten.toMap
        outputFields
    }
}
