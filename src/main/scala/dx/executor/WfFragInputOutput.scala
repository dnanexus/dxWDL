package dx.executor

import dx.api.{DxApi, DxFile}
import dx.core.languages.wdl.{ParameterLinkSerde, TypeSerialization}
import dx.executor.wdl.WdlExecutableLink
import spray.json._
import wdlTools.eval.WdlValues
import wdlTools.types.WdlTypes

case class WfFragInput(blockPath: Vector[Int],
                       env: Map[String, (WdlTypes.T, WdlValues.V)],
                       execLinkInfo: Map[String, WdlExecutableLink])

case class WfFragInputOutput(typeAliases: Map[String, WdlTypes.T],
                             wdlVarLinksConverter: ParameterLinkSerde,
                             dxApi: DxApi) {

  // 1. Convert the inputs to WDL values
  // 2. Setup an environment to evaluate the sub-block. This should
  //    look to the WDL code as if all previous code had been evaluated.
  def loadInputs(inputs: JsValue, metaInfo: Map[String, JsValue]): WfFragInput = {
    val regularFields: Map[String, JsValue] = inputs.asJsObject.fields
      .filter { case (fieldName, _) => !fieldName.endsWith(Parameter.FlatFilesSuffix) }

    // Extract the meta information needed to setup the closure for the subblock
    val (execLinkInfo, blockPath, fqnDictTypes) = loadWorkflowMetaInfo(metaInfo.asJsObject.fields)

    // What remains are inputs from other stages. Convert from JSON to WDL values
    val env: Map[String, (WdlTypes.T, WdlValues.V)] = regularFields.map {
      case (name, jsValue) =>
        val fqn = ParameterLinkSerde.decodeDots(name)
        val wdlType = fqnDictTypes.get(fqn) match {
          case None =>
            throw new Exception(s"Did not find variable ${fqn} (${name}) in the block environment")
          case Some(x) => x
        }
        val value = wdlVarLinksConverter.unpackJobInput(fqn, wdlType, jsValue)
        fqn -> (wdlType, value)
    }

    WfFragInput(blockPath, env, execLinkInfo)
  }

  // find all the dx:files that are referenced from the inputs
  def findRefDxFiles(inputs: JsValue, metaInfo: JsValue): Vector[DxFile] = {
    val regularFields: Map[String, JsValue] = inputs.asJsObject.fields
      .filter { case (fieldName, _) => !fieldName.endsWith(ParameterLinkSerde.FlatFilesSuffix) }

    val (_, _, fqnDictTypes) = loadWorkflowMetaInfo(metaInfo.asJsObject.fields)

    // Convert from JSON to WDL values
    regularFields
      .map {
        case (name, jsValue) =>
          val fqn = ParameterLinkSerde.decodeDots(name)
          if (!fqnDictTypes.contains(fqn)) {
            throw new Exception(
                s"Did not find variable ${fqn} (${name}) in the block environment"
            )
          }
          dxApi.findFiles(jsValue)
      }
      .toVector
      .flatten
  }
}
