package dx.executor.wdl

import dx.api._
import dx.core.ir.Type
import spray.json._
import wdlTools.types.WdlTypes

import scala.collection.immutable.TreeMap

// Information used to link applets that call other applets. For example, a scatter
// applet calls applets that implement tasks.
case class WdlExecutableLink(name: String,
                             inputs: Map[String, Type],
                             outputs: Map[String, Type],
                             dxExec: DxExecutable)

object WdlExecutableLink {
  // Serialize applet input definitions, so they could be used
  // at runtime.
  def writeJson(link: WdlExecutableLink, typeAliases: Map[String, Type]): JsValue = {
    val wdlTypeConverter = TypeSerialization(typeAliases)

    val appInputDefs: Map[String, JsString] = link.inputs.map {
      case (name, wdlType) => name -> JsString(wdlTypeConverter.toString(wdlType))
    }
    val appOutputDefs: Map[String, JsString] = link.outputs.map {
      case (name, wdlType) => name -> JsString(wdlTypeConverter.toString(wdlType))
    }
    JsObject(
        "name" -> JsString(link.name),
        "inputs" -> JsObject(appInputDefs.to(TreeMap)),
        "outputs" -> JsObject(appOutputDefs.to(TreeMap)),
        "id" -> JsString(link.dxExec.getId)
    )
  }

  def readJson(dxApi: DxApi,
               aplInfo: JsValue,
               typeAliases: Map[String, WdlTypes.T]): WdlExecutableLink = {
    val wdlTypeConverter = TypeSerialization(typeAliases)

    val name = aplInfo.asJsObject.fields("name") match {
      case JsString(x) => x
      case _           => throw new Exception("Bad JSON")
    }
    val inputDefs = aplInfo.asJsObject
      .fields("inputs")
      .asJsObject
      .fields
      .map {
        case (key, JsString(wdlTypeStr)) => key -> wdlTypeConverter.fromString(wdlTypeStr)
        case _                           => throw new Exception("Bad JSON")
      }
    val outputDefs = aplInfo.asJsObject
      .fields("outputs")
      .asJsObject
      .fields
      .map {
        case (key, JsString(wdlTypeStr)) => key -> wdlTypeConverter.fromString(wdlTypeStr)
        case _                           => throw new Exception("Bad JSON")
      }
    val JsString(id) = aplInfo.asJsObject.fields("id")
    val dxExec = dxApi.getObject(id) match {
      case a: DxApp      => a
      case a: DxApplet   => a
      case a: DxWorkflow => a
      case _             => throw new Exception(s"${id} is not an app/applet/workflow")
    }
    WdlExecutableLink(name, inputDefs, outputDefs, dxExec)
  }
}
