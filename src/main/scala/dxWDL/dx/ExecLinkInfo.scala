package dxWDL.dx

import spray.json._
import scala.collection.immutable.TreeMap
import wom.types._
import dxWDL.base._

// Information used to link applets that call other applets. For example, a scatter
// applet calls applets that implement tasks.
case class ExecLinkInfo(name: String,
                        inputs: Map[String, WomType],
                        outputs: Map[String, WomType],
                        dxExec: DxExecutable)

object ExecLinkInfo {
  // Serialize applet input definitions, so they could be used
  // at runtime.
  def writeJson(ali: ExecLinkInfo, typeAliases: Map[String, WomType]): JsValue = {
    val womTypeConverter = WomTypeSerialization(typeAliases)

    val appInputDefs: Map[String, JsString] = ali.inputs.map {
      case (name, womType) => name -> JsString(womTypeConverter.toString(womType))
    }.toMap
    val appOutputDefs: Map[String, JsString] = ali.outputs.map {
      case (name, womType) => name -> JsString(womTypeConverter.toString(womType))
    }.toMap
    JsObject(
        "name" -> JsString(ali.name),
        "inputs" -> JsObject(TreeMap(appInputDefs.toArray: _*)),
        "outputs" -> JsObject(TreeMap(appOutputDefs.toArray: _*)),
        "id" -> JsString(ali.dxExec.getId)
    )
  }

  def readJson(aplInfo: JsValue, typeAliases: Map[String, WomType]): ExecLinkInfo = {
    val womTypeConverter = WomTypeSerialization(typeAliases)

    val name = aplInfo.asJsObject.fields("name") match {
      case JsString(x) => x
      case _           => throw new Exception("Bad JSON")
    }
    val inputDefs = aplInfo.asJsObject
      .fields("inputs")
      .asJsObject
      .fields
      .map {
        case (key, JsString(womTypeStr)) => key -> womTypeConverter.fromString(womTypeStr)
        case _                           => throw new Exception("Bad JSON")
      }
      .toMap
    val outputDefs = aplInfo.asJsObject
      .fields("outputs")
      .asJsObject
      .fields
      .map {
        case (key, JsString(womTypeStr)) => key -> womTypeConverter.fromString(womTypeStr)
        case _                           => throw new Exception("Bad JSON")
      }
      .toMap
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
