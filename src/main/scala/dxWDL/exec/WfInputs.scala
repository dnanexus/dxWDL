// Execute a WDL workflow with no calls, and no expressions, only inputs and outputs.
// It could have expressions in the output section.

package dxWDL.exec

import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.values._
import wom.types.WomType

import dxWDL.base.{Utils, Verbose}
import dxWDL.util._

case class WfInputs(
    wf: WorkflowDefinition,
    wfSourceCode: String,
    typeAliases: Map[String, WomType],
    dxPathConfig: DxPathConfig,
    dxIoFunctions: DxIoFunctions,
    runtimeDebugLevel: Int
) {
  private val verbose = runtimeDebugLevel >= 1
  //private val maxVerboseLevel = (runtimeDebugLevel == 2)
  private val utlVerbose = Verbose(runtimeDebugLevel >= 1, false, Set.empty)
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(utlVerbose, dxIoFunctions.fileInfoDir, typeAliases)

  def apply(inputs: Map[String, WomValue]): Map[String, JsValue] = {
    Utils.appletLog(verbose, s"dxWDL version: ${Utils.getVersion()}")
    Utils.appletLog(verbose, s"Environment: ${inputs}")
    Utils.appletLog(
      verbose,
      s"""|Artificial applet for unlocked workflow inputs
                                     |${WomPrettyPrintApproxWdl.graphInputs(
           wf.inputs.toSeq
         )}
                                     |""".stripMargin
    )

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = inputs
      .map {
        case (outputVarName, womValue) =>
          val wvl =
            wdlVarLinksConverter.importFromWDL(womValue.womType, womValue)
          wdlVarLinksConverter.genFields(wvl, outputVarName)
      }
      .toList
      .flatten
      .toMap
    outputFields
  }
}
