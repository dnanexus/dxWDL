package dxWDL.util

import com.dnanexus._
import java.nio.file.Path
import spray.json._
import wom.types.WomType

import dxWDL.base._

// Exception used for AppInternError
class AppInternalException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(message:String) = this(new RuntimeException(message))
}

// Exception used for AppError
class AppException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(message:String) = this(new RuntimeException(message))
}

object IORef extends Enumeration {
    val Input, Output = Value
}

object CompilerFlag extends Enumeration {
    val All, IR, NativeWithoutRuntimeAsset = Value
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

// Request for an instance type
case class InstanceTypeReq(dxInstanceType: Option[String],
                           memoryMB: Option[Int],
                           diskGB: Option[Int],
                           cpu: Option[Int])


// Encapsulation of verbosity flags.
//  on --       is the overall setting true/false
//  keywords -- specific words to trace
//  quiet:      if true, do not print warnings and informational messages
case class Verbose(on: Boolean,
                   quiet: Boolean,
                   keywords: Set[String]) {
    lazy val keywordsLo = keywords.map(_.toLowerCase).toSet

    // check in a case insensitive fashion
    def containsKey(word: String) : Boolean = {
        keywordsLo contains word.toLowerCase
    }
}


// Packing of all compiler flags in an easy to digest
// format
case class CompilerOptions(archive: Boolean,
                           compileMode: CompilerFlag.Value,
                           defaults: Option[Path],
                           extras: Option[Extras],
                           fatalValidationWarnings: Boolean,
                           force: Boolean,
                           importDirs: List[Path],
                           inputs: List[Path],
                           leaveWorkflowsOpen: Boolean,
                           locked: Boolean,
                           projectWideReuse: Boolean,
                           reorg: Boolean,
                           runtimeDebugLevel: Option[Int],
                           verbose: Verbose)

// A DNAx executable. An app, applet, or workflow.
case class DxExec(id: String) {
    def getId : String = id
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

// The end result of the compiler
case class CompilationResults(primaryCallable: Option[DxExec],
                              execDict: Map[String, DxExec])

// Different ways of using the mini-workflow runner.
//   Launch:     there are WDL calls, lanuch the dx:executables.
//   Collect:    the dx:exucutables are done, collect the results.
object RunnerWfFragmentMode extends Enumeration {
    val Launch, Collect = Value
}

object Language extends Enumeration {
    val WDLvDraft2, WDLv1_0, CWLv1_0 = Value
}

case class IOParameter(name: String,
                       ioClass: IOClass,
                       optional : Boolean)

// This is similar to DXDataObject.Describe
case class DxDescribe(name : String,
                      folder: String,
                      size : Option[Long],
                      project: DXProject,
                      dxid : DXDataObject,
                      properties: Map[String, String],
                      inputSpec : Option[Vector[IOParameter]],
                      outputSpec : Option[Vector[IOParameter]])

object DxDescribe {
    def convertToDxObject(objName : String) : Option[DXDataObject] = {
        // If the object is a file-id (or something like it), then
        // shortcut the expensive findDataObjects call.
        if (objName.startsWith("applet-")) {
            return Some(DXApplet.getInstance(objName))
        }
        if (objName.startsWith("file-")) {
            return Some(DXFile.getInstance(objName))
        }
        if (objName.startsWith("record-")) {
            return Some(DXRecord.getInstance(objName))
        }
        if (objName.startsWith("workflow-")) {
            return Some(DXWorkflow.getInstance(objName))
        }
        return None
    }
}
