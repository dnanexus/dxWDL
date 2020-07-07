package dx.exec

import dx.api.DxApi
import dx.core.io.DxPathConfig
import dx.core.languages.wdl.{DxFileAccessProtocol, PrettyPrintApprox, WdlVarLinksConverter}
import dx.util.getVersion
import spray.json.JsValue
import wdlTools.eval.WdlValues
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}

case class WfInputs(wf: TAT.Workflow,
                    document: TAT.Document,
                    typeAliases: Map[String, WdlTypes.T],
                    dxPathConfig: DxPathConfig,
                    dxIoFunctions: DxFileAccessProtocol,
                    dxApi: DxApi) {
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(dxApi, dxIoFunctions.fileInfoDir, typeAliases)

  def apply(inputs: Map[String, (WdlTypes.T, WdlValues.V)]): Map[String, JsValue] = {
    dxApi.logger.traceLimited(s"dxWDL version: ${getVersion}")
    dxApi.logger.traceLimited(s"Environment: ${inputs}")
    dxApi.logger.traceLimited(
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
