package dx.core.languages.wdl

import dx.api._
import spray.json._
import wdlTools.types.WdlTypes

import scala.collection.immutable.TreeMap

// Information used to link applets that call other applets. For example, a scatter
// applet calls applets that implement tasks.
case class ExecLinkInfo(name: String,
                        inputs: Map[String, WdlTypes.T],
                        outputs: Map[String, WdlTypes.T],
                        dxExec: DxExecutable)

object ExecLinkInfo {
  // Serialize applet input definitions, so they could be used
  // at runtime.
  def writeJson(ali: ExecLinkInfo, typeAliases: Map[String, WdlTypes.T]): JsValue = {
    val wdlTypeConverter = TypeSerialization(typeAliases)

    val appInputDefs: Map[String, JsString] = ali.inputs.map {
      case (name, wdlType) => name -> JsString(wdlTypeConverter.toString(wdlType))
    }
    val appOutputDefs: Map[String, JsString] = ali.outputs.map {
      case (name, wdlType) => name -> JsString(wdlTypeConverter.toString(wdlType))
    }
    JsObject(
        "name" -> JsString(ali.name),
        "inputs" -> JsObject(appInputDefs.to(TreeMap)),
        "outputs" -> JsObject(appOutputDefs.to(TreeMap)),
        "id" -> JsString(ali.dxExec.getId)
    )
  }

  def readJson(dxApi: DxApi,
               aplInfo: JsValue,
               typeAliases: Map[String, WdlTypes.T]): ExecLinkInfo = {
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
    ExecLinkInfo(name, inputDefs, outputDefs, dxExec)
  }
}
