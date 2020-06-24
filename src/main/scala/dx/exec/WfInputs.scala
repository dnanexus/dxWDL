package dx.exec

import dx.core.io.DxPathConfig
import dx.core.languages.wdl.{DxFileAccessProtocol, PrettyPrintApprox, WdlVarLinksConverter}
import dx.util.{Logger, Verbose}
import spray.json.JsValue
import wdlTools.eval.WdlValues
import wdlTools.types.WdlTypes

case class WfInputs(wf: TAT.Workflow,
                    document: TAT.Document,
                    typeAliases: Map[String, WdlTypes.T],
                    dxPathConfig: DxPathConfig,
                    dxIoFunctions: DxFileAccessProtocol,
                    runtimeDebugLevel: Int) {
  private val verbose = runtimeDebugLevel >= 1
  //private val maxVerboseLevel = (runtimeDebugLevel == 2)
  private val utlVerbose = Verbose(runtimeDebugLevel >= 1, quiet = false, Set.empty)
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(utlVerbose, dxIoFunctions.fileInfoDir, typeAliases)

  def apply(inputs: Map[String, (WdlTypes.T, WdlValues.V)]): Map[String, JsValue] = {
    Logger.appletLog(verbose, s"dxWDL version: ${getVersion}")
    Logger.appletLog(verbose, s"Environment: ${inputs}")
    Logger.appletLog(
        verbose,
        s"""|Artificial applet for unlocked workflow inputs
            |${PrettyPrintApprox.graphInputs(wf.inputs)}
            |""".stripMargin
    )

    // convert the WDL values to JSON
    val outputFields: Map[String, JsValue] = inputs
      .map {
        case (outputVarName, (wdlType, wdlValue)) =>
          val wvl = wdlVarLinksConverter.importFromWDL(wdlType, wdlValue)
          wdlVarLinksConverter.genFields(wvl, outputVarName)
      }
      .toList
      .flatten
      .toMap
    outputFields
  }
}
