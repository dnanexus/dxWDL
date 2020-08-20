package dx.compiler.wdl

import dx.api.DxApi
import dx.compiler.ir.ExtrasParser
import spray.json.JsValue
import wdlTools.eval.{JsonSerde, WdlValues}
import wdlTools.util.Logger

case class WdlExtrasParser(dxApi: DxApi = DxApi.get, logger: Logger = Logger.get)
    extends ExtrasParser[WdlValues.V](dxApi, logger) {
  override protected def deserialize(jsValue: JsValue): WdlValues.V = JsonSerde.deserialize(jsValue)
}
