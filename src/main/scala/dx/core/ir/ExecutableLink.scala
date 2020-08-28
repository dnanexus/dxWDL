package dx.core.ir

import dx.api.DxExecutable
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
}
