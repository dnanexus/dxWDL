/** Describe the workflow in a tree representation
  */
package dxWDL.compiler

import spray.json._
import Native.ExecRecord
import IR._
import dxWDL.base.Utils

case class Tree(execDict: Map[String, ExecRecord]) {

  private def kindToString(kind: AppletKind): String = {
    kind match {
      case _: AppletKindNative               => "Native"
      case _: AppletKindTask                 => "Task"
      case _: AppletKindWfFragment           => "Fragment"
      case AppletKindWfInputs                => "Inputs"
      case AppletKindWfOutputs               => "Outputs"
      case AppletKindWfCustomReorgOutputs    => "Reorg outputs"
      case AppletKindWorkflowOutputReorg     => "Output Reorg"
      case AppletKindWorkflowCustomReorg(id) => s"Custom reorg ${id}"
    }
  }

  def apply(primary: Native.ExecRecord): JsValue = {
    primary.callable match {
      case apl: IR.Applet if primary.links.size == 0 =>
        JsObject("name" -> JsString(apl.name),
                 "id" -> JsString(primary.dxExec.id),
                 "kind" -> JsString(kindToString(apl.kind)))

      case apl: IR.Applet =>
        // applet that calls other applets/workflows at runtime.
        // recursively describe all called elements.
        val links: Vector[JsValue] = primary.links.map {
          case eli =>
            val calleeRecord = execDict(eli.name)
            apply(calleeRecord)
        }.toVector
        JsObject("name" -> JsString(apl.name),
                 "id" -> JsString(primary.dxExec.id),
                 "kind" -> JsString(kindToString(apl.kind)),
                 "executables" -> JsArray(links))

      case wf: IR.Workflow =>
        val vec = wf.stages.map {
          case stage =>
            val calleeRecord = execDict(stage.calleeName)
            val jsv: JsValue = apply(calleeRecord)
            JsObject("stage_name" -> JsString(stage.description), "callee" -> jsv)
        }.toVector
        val stages = JsArray(vec)
        JsObject("name" -> JsString(wf.name),
                 "id" -> JsString(primary.dxExec.id),
                 "kind" -> JsString("workflow"),
                 "stages" -> stages)
    }
  }

  def prettyPrint(primary: Native.ExecRecord, indent: Int = 0): String = {
    primary.callable match {
      case apl: IR.Applet if apl.kind.isInstanceOf[AppletKindTask] =>
        Utils.genNSpaces(indent) + Console.GREEN + apl.name + Console.RESET

      case apl: IR.Applet if apl.kind.isInstanceOf[AppletKindWfFragment] =>
        // applet that calls other applets/workflows at runtime.
        // recursively describe all called elements.
        val links: Vector[String] = primary.links.map {
          case eli =>
            val calleeRecord = execDict(eli.name)
            prettyPrint(calleeRecord, indent + 4)
        }.toVector
        val topLine = Utils.genNSpaces(indent) + Console.CYAN + apl.name + Console.RESET
        val lines = topLine +: links
        lines.mkString("\n")

      case wf: IR.Workflow =>
        val stageLines: Vector[String] = wf.stages.map {
          case stage =>
            val calleeRecord = execDict(stage.calleeName)
            val calleeDesc: String = prettyPrint(calleeRecord, indent + 4)
            val topLine = Utils
              .genNSpaces(indent) + Console.MAGENTA + stage.description + Console.RESET
            topLine + calleeDesc
        }.toVector
        val wfTopLine = Utils.genNSpaces(indent) + Console.BLACK + wf.name + Console.RESET
        val lines = wfTopLine +: stageLines
        lines.mkString("\n")
    }
  }
}
