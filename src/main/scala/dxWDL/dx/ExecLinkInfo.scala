package dxWDL.dx

import spray.json._
import scala.collection.immutable.TreeMap
import wdlTools.types.WdlTypes

import dxWDL.base._

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
    val wdlTypeConverter = WdlTypeSerialization(typeAliases)

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

  def readJson(aplInfo: JsValue, typeAliases: Map[String, WdlTypes.T]): ExecLinkInfo = {
    val wdlTypeConverter = WdlTypeSerialization(typeAliases)

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
    val dxExec = aplInfo.asJsObject.fields("id") match {
      case JsString(id) if id.startsWith("app-")      => DxApp(id)
      case JsString(id) if id.startsWith("applet-")   => DxApplet(id, None)
      case JsString(id) if id.startsWith("workflow-") => DxWorkflow(id, None)
      case JsString(id)                               => throw new Exception(s"${id} is not an app/applet/workflow")
      case _                                          => throw new Exception("Bad JSON")
    }
    ExecLinkInfo(name, inputDefs, outputDefs, dxExec)
  }
}
