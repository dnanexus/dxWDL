/** Describe the workflow in a tree representation
  */
package dxWDL.compiler

import spray.json._
import Native.ExecRecord
import IR._

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

  // Traverse the exec tree and generate an appropriate name + color based on the node type
  // pass back the prefix for the next node build on.
  def prettyPrint(primary: Native.ExecRecord,
                  stageDesc: Option[String] = None,
                  indent: Int = 3,
                  prefix: String = ""): String = {

    val lastElem = s"└${"─" * indent}"
    val midElem = s"├${"─" * indent}"

    primary.callable match {
      case wf: IR.Workflow => {
        val stageLines = wf.stages.zipWithIndex.map {
          case (stage, index) => {
            val isLast = index == wf.stages.size - 1
            val postPrefix = if (isLast) lastElem else midElem
            val wholePrefix = if (isLast) {
              prefix.replace("├", "│").replace("└", " ").replace("─", " ") + postPrefix
            } else {
              prefix.replace("├", " ").replace("└", " ").replace("─", " ") + postPrefix
            }
            prettyPrint(execDict(stage.calleeName), Some(stage.description), indent, wholePrefix)
          }
        }.toVector

        val stages = stageLines.mkString("\n")
        prefix + Console.CYAN + "Workflow: " + Console.YELLOW + wf.name + Console.RESET + "\n" + stages
      }
      case apl: IR.Applet => {
        apl.kind match {
          case AppletKindWfFragment(calls, blockPath, fqnDictTypes) => {
            // applet that calls other applets/workflows at runtime.
            // recursively describe all called elements.
            val links = calls.zipWithIndex.map {
              case (link, index) => {
                val isLast = index == (calls.size - 1)
                val postPrefix = if (isLast) lastElem else midElem
                val wholePrefix = if (isLast) {
                  prefix.replace("├", "│").replace("└", "│").replace("─", " ") + postPrefix
                } else {
                  prefix.replace("├", " ").replace("└", " ").replace("─", " ") + postPrefix
                }
                prettyPrint(execDict(link), None, indent, wholePrefix)
              }
            }.toVector

            val name = stageDesc match {
              case Some(name) => name
              case None       => s"${apl.name}"
            }

            prefix + Console.CYAN + s"App ${kindToString(apl.kind)}: " + Console.WHITE + name + Console.RESET + "\n" + links
              .mkString("\n")
          }
          case _ =>
            val name = stageDesc match {
              case Some(name) => name
              case None       => s"${apl.name}"
            }
            prefix + Console.CYAN + s"App ${kindToString(apl.kind)}: " + Console.WHITE + name + Console.RESET
        }
      }
    }
  }
}
