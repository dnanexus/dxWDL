package dxWDL.util

import com.dnanexus._
import java.nio.file.Path
import spray.json._
import wdl.draft2.model.types._
import wom.types._

// Exception used for AppInternError
class AppInternalException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(message:String) = this(new RuntimeException(message))
}

// Exception used for AppError
class AppException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(message:String) = this(new RuntimeException(message))
}

class UnboundVariableException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(varName: String) = this(new RuntimeException(s"Variable ${varName} is unbound"))
}

class NamespaceValidationException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(msg: String) = this(new RuntimeException(msg))
}

class NullValueException private(ex: Exception) extends RuntimeException(ex) {
    def this(msg: String) = this(new RuntimeException(msg))
}


// Mode of file data transfer
//   Data: download of upload the entire file
//   Remote: leave the file on the platform
//   Stream: stream download/upload the file
object IOMode extends Enumeration {
    val Data, Remote, Stream = Value
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
                   keywords: Set[String])


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
case class ExecLinkInfo(inputs: Map[String, WomType],
                        outputs: Map[String, WomType],
                        dxExec: DxExec)

object ExecLinkInfo {
    def writeJson(ali: ExecLinkInfo) : JsValue = {
            // Serialize applet input definitions, so they could be used
            // at runtime.
        val appInputDefs: Map[String, JsString] = ali.inputs.map{
            case (name, womType) => name -> JsString(womType.toDisplayString)
        }.toMap
        val appOutputDefs: Map[String, JsString] = ali.outputs.map{
            case (name, womType) => name -> JsString(womType.toDisplayString)
        }.toMap
        JsObject(
            "id" -> JsString(ali.dxExec.getId),
            "inputs" -> JsObject(appInputDefs),
            "outputs" -> JsObject(appOutputDefs)
        )
    }

    def readJson(aplInfo: JsValue, dxProject: DXProject) = {
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
        val inputDefs = aplInfo.asJsObject.fields("inputs").asJsObject.fields.map{
            case (key, JsString(womTypeStr)) => key -> WdlFlavoredWomType.fromDisplayString(womTypeStr)
            case _ => throw new Exception("Bad JSON")
        }.toMap
        val outputDefs = aplInfo.asJsObject.fields("outputs").asJsObject.fields.map{
            case (key, JsString(womTypeStr)) => key -> WdlFlavoredWomType.fromDisplayString(womTypeStr)
            case _ => throw new Exception("Bad JSON")
        }.toMap
        ExecLinkInfo(inputDefs, outputDefs, dxExec)
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
