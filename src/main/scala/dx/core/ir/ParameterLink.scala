package dx.core.ir

import dx.api.{DxExecution, DxWorkflowStage}
import dx.core.languages.IORef
import spray.json.JsValue

/**
  * A union of all the different ways of building a value from JSON passed
  * by the platform. A complex value is a WDL values that does not map to
  * a native dx:type. Such values may also have files embedded in them.
  * For example:
  * - Ragged file array:  Array\[Array\[File\]\]
  * - Object with file elements
  * - Map of files:     Map[String, File]
  * A complex value is implemented as a json structure, and an array of
  * all the files it references.
  */
sealed trait ParameterLink {
  val dxType: Type
}
case class ParameterLinkValue(jsn: JsValue, dxType: Type) extends ParameterLink
case class ParameterLinkStage(dxStage: DxWorkflowStage,
                              ioRef: IORef.Value,
                              varName: String,
                              dxType: Type)
    extends ParameterLink
case class ParameterLinkWorkflowInput(varName: String, dxType: Type) extends ParameterLink
case class ParameterLinkExec(dxExecution: DxExecution, varName: String, dxType: Type)
    extends ParameterLink
