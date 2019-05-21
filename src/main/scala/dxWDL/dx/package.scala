package dxWDL.dx

import com.dnanexus._
import spray.json._
import wom.types._

import dxWDL.base.WomTypeSerialization

case class IOParameter(name: String,
                       ioClass: IOClass,
                       optional : Boolean)

// This is similar to DXDataObject.Describe
case class DxDescribe(name : String,
                      folder: String,
                      size : Option[Long],
                      project: Option[DxProject],
                      dxobj : DXDataObject,
                      creationDate : java.util.Date,
                      properties: Map[String, String],
                      inputSpec : Option[Vector[IOParameter]],
                      outputSpec : Option[Vector[IOParameter]])

// A DNAx executable. An app, applet, or workflow.
case class DxExec(id: String) {
    def getId : String = id
}

case class DxProject(id: String) {
    def getId : String = id
}

// An equivalent for the InputParmater/OutputParameter types
case class DXIOParam(ioClass: IOClass,
                     optional: Boolean)

// A stand in for the DXWorkflow.Stage inner class (we don't have a constructor for it)
case class DXWorkflowStage(id: String) {
    def getId() = id

    def getInputReference(inputName:String) : JsValue = {
        JsObject("$dnanexus_link" -> JsObject(
                     "stage" -> JsString(id),
                     "inputField" -> JsString(inputName)))
    }
    def getOutputReference(outputName:String) : JsValue = {
        JsObject("$dnanexus_link" -> JsObject(
                     "stage" -> JsString(id),
                     "outputField" -> JsString(outputName)))
    }
}


// Information used to link applets that call other applets. For example, a scatter
// applet calls applets that implement tasks.
case class ExecLinkInfo(name: String,
                        inputs: Map[String, WomType],
                        outputs: Map[String, WomType],
                        dxExec: DxExec)



object ExecLinkInfo {
    // Serialize applet input definitions, so they could be used
    // at runtime.
    def writeJson(ali: ExecLinkInfo,
                  typeAliases: Map[String, WomType]) : JsValue = {
        val womTypeConverter = WomTypeSerialization(typeAliases)

        val appInputDefs: Map[String, JsString] = ali.inputs.map{
            case (name, womType) => name -> JsString(womTypeConverter.toString(womType))
        }.toMap
        val appOutputDefs: Map[String, JsString] = ali.outputs.map{
            case (name, womType) => name -> JsString(womTypeConverter.toString(womType))
        }.toMap
        JsObject(
            "name" -> JsString(ali.name),
            "inputs" -> JsObject(appInputDefs),
            "outputs" -> JsObject(appOutputDefs),
            "id" -> JsString(ali.dxExec.getId)
        )
    }

    def readJson(aplInfo: JsValue,
                 typeAliases: Map[String, WomType]) : ExecLinkInfo = {
        val womTypeConverter = WomTypeSerialization(typeAliases)

        val name = aplInfo.asJsObject.fields("name") match {
            case JsString(x) => x
            case _ => throw new Exception("Bad JSON")
        }
        val inputDefs = aplInfo.asJsObject.fields("inputs").asJsObject.fields.map{
            case (key, JsString(womTypeStr)) => key -> womTypeConverter.fromString(womTypeStr)
            case _ => throw new Exception("Bad JSON")
        }.toMap
        val outputDefs = aplInfo.asJsObject.fields("outputs").asJsObject.fields.map{
            case (key, JsString(womTypeStr)) => key -> womTypeConverter.fromString(womTypeStr)
            case _ => throw new Exception("Bad JSON")
        }.toMap
        val dxExec = aplInfo.asJsObject.fields("id") match {
            case JsString(execId) =>
                if (execId.startsWith("app-") ||
                        execId.startsWith("applet-") ||
                        execId.startsWith("workflow-"))
                    DxExec(execId)
                else
                    throw new Exception(s"${execId} is not an app/applet/workflow")
            case _ => throw new Exception("Bad JSON")
        }
        ExecLinkInfo(name, inputDefs, outputDefs, dxExec)
    }
}
