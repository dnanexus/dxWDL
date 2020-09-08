package dx.core.ir

import dx.api.{DxApi, DxExecutable}
import spray.json.{JsObject, JsString, JsValue}

import scala.collection.immutable.TreeMap

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
    val inputs: Map[String, JsValue] = link.inputs.map {
      case (name, wdlType) =>
        name -> TypeSerde.serialize(wdlType)
    }
    val outputs: Map[String, JsValue] = link.outputs.map {
      case (name, wdlType) => name -> TypeSerde.serialize(wdlType)
    }
    JsObject(
        "name" -> JsString(link.name),
        "inputs" -> JsObject(inputs.to(TreeMap)),
        "outputs" -> JsObject(outputs.to(TreeMap)),
        "id" -> JsString(link.dxExec.getId)
    )
  }

  def deserialize(jsValue: JsValue,
                  typeAliases: Map[String, Type],
                  dxApi: DxApi = DxApi.get): ExecutableLink = {
    jsValue match {
      case JsObject(fields) =>
        val JsString(name) = fields("name")
        val JsObject(inputs) = fields("inputs")
        val inputTypes = inputs.map {
          case (name, jsValue) =>
            name -> TypeSerde.deserialize(jsValue, typeAliases)
        }
        val JsObject(outputs) = fields("outputs")
        val outputTypes = outputs.map {
          case (name, jsValue) =>
            name -> TypeSerde.deserialize(jsValue, typeAliases)
        }
        val JsString(id) = fields("id")
        ExecutableLink(name, inputTypes, outputTypes, dxApi.executable(id))
      case _ =>
        throw new Exception(s"Invalid ExecutableLink ${jsValue}")
    }
  }
}
