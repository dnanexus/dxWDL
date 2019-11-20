package dxWDL.dx

import com.dnanexus._
import scala.collection.immutable.TreeMap
import spray.json._
import wom.types._

import dxWDL.base.WomTypeSerialization

object DxIOClass extends Enumeration {
    val INT, FLOAT, STRING, BOOLEAN, FILE,
        ARRAY_OF_INTS, ARRAY_OF_FLOATS, ARRAY_OF_STRINGS, ARRAY_OF_BOOLEANS, ARRAY_OF_FILES,
        HASH, OTHER = Value

    def fromString(s : String) : DxIOClass.Value = {
        s match {
            // primitives
            case "int" => INT
            case "float" => FLOAT
            case "string" => STRING
            case "boolean" => BOOLEAN
            case "file" => FILE

            // arrays of primitives
            case "array:int" => ARRAY_OF_INTS
            case "array:float" => ARRAY_OF_FLOATS
            case "array:string" => ARRAY_OF_STRINGS
            case "array:boolean"=> ARRAY_OF_BOOLEANS
            case "array:file" => ARRAY_OF_FILES

            // hash
            case "hash" => HASH

            // we don't deal with anything else
            case other => OTHER
        }
    }
}

case class IOParameter(name: String,
                       ioClass: DxIOClass.Value,
                       optional : Boolean)

case class DxFilePart(state: String,
                      size: Long,
                      md5: String)

// This is similar to DXDataObject.Describe
case class DxDescribe(name : String,
                      folder: String,
                      size : Option[Long],
                      container: DxContainer, // a project or a container
                      dxobj : DXDataObject,
                      created : Long,
                      modified : Long,
                      properties: Map[String, String],
                      inputSpec : Option[Vector[IOParameter]],
                      outputSpec : Option[Vector[IOParameter]],
                      parts : Option[Map[Int, DxFilePart]],
                      details : Option[JsValue])

// A DNAx executable. An app, applet, or workflow.
case class DxExec(id: String) {
    def getId : String = id
}


class DxDataObject {
    def getId : String
}

case class DxContainer(id: String) {
    def getId : String = id
}

case class DxRecord(id : string) {
    def getId : String = id

    def describe()
}

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
            "inputs" -> JsObject(TreeMap(appInputDefs.toArray:_*)),
            "outputs" -> JsObject(TreeMap(appOutputDefs.toArray:_*)),
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
