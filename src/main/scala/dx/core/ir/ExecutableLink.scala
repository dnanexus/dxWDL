package dx.core.ir

import dx.api.{DxApi, DxExecutable}
import dx.core.ir.Type.TSchema
import spray.json.{JsObject, JsString, JsValue}

/**
  * Information used to link applets that call other applets. For example, a scatter
  * applet calls applets that implement tasks.
  * @param name executable name
  * @param inputs executable inputs
  * @param outputs exectuable outputs
  * @param dxExec API Object
  */
case class ExecutableLink(name: String,
                          inputs: Map[String, Type],
                          outputs: Map[String, Type],
                          dxExec: DxExecutable)

object ExecutableLink {
  def serialize(link: ExecutableLink): JsObject = {
    val (inputTypes, inputSchemas) = TypeSerde.serializeMap(link.inputs)
    val (outputTypes, inputAndOutputSchemas) = TypeSerde.serializeMap(link.outputs, inputSchemas)
    JsObject(
        "name" -> JsString(link.name),
        "id" -> JsString(link.dxExec.id),
        "inputs" -> JsObject(inputTypes),
        "outputs" -> JsObject(outputTypes),
        "schemas" -> JsObject(inputAndOutputSchemas)
    )
  }

  def deserialize(jsValue: JsValue,
                  typeAliases: Map[String, TSchema],
                  dxApi: DxApi = DxApi.get): ExecutableLink = {
    jsValue match {
      case JsObject(fields) =>
        val JsString(name) = fields("name")
        val JsString(id) = fields("id")
        val JsObject(jsSchemas) = fields("schemas")
        val JsObject(jsInputs) = fields("inputs")
        val JsObject(jsOutputs) = fields("outputs")
        val (inputTypes, inputSchemas) = TypeSerde.deserializeMap(jsInputs, jsSchemas, typeAliases)
        val (outputTypes, _) = TypeSerde.deserializeMap(jsOutputs, jsSchemas, inputSchemas)
        ExecutableLink(name, inputTypes, outputTypes, dxApi.executable(id))
      case _ =>
        throw new Exception(s"Invalid ExecutableLink ${jsValue}")
    }
  }
}
