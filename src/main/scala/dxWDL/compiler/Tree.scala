/** Describe the workflow in a tree representation
  */
package dxWDL.compiler

import spray.json._
import Native.ExecRecord
import IR._
// import org.python.bouncycastle.math.raw.Nat
import dxWDL.base.Utils

case class Displayable(root: Native.ExecRecord, name: String, children: Vector[Displayable])

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
  def prettyPrintRec(primary: Native.ExecRecord): Displayable = {
    primary.callable match {
      case wf: IR.Workflow => {
        val stageLines = wf.stages.map {
          case stage => {
            prettyPrintRec(execDict(stage.calleeName))
          }
        }.toVector

        Displayable(primary, wf.name + " (Workflow)", stageLines)

      }
      case apl: IR.Applet => {
        apl.kind match {
          case AppletKindWfFragment(calls, blockPath, fqnDictTypes) => {
            // applet that calls other applets/workflows at runtime.
            // recursively describe all called elements.
            val links = calls.map {
              case link => {
                val calleeRecord = execDict(link)
                prettyPrintRec(calleeRecord)
              }
            }.toVector
            Displayable(primary, s"${apl.name} (${kindToString(apl.kind)})", links)
          }
          case _ =>
            Displayable(primary, s"${apl.name} (${kindToString(apl.kind)})", Vector())
        }
      }
    }
  }

  def prettyPrint(primary: Native.ExecRecord): String = {
    val steps = prettyPrintRec(primary)
    formatter(steps)
  }
  def formatter(items: Displayable, level: Int = 0): String = {
    Utils.genNSpaces(level * 2) + items.name + "\n" + items.children
      .map {
        case Displayable(primary, name, children) => {
          primary.callable match {
            case wf: IR.Workflow => {
              Utils.genNSpaces((level + 1) * 2) + name + "\n" + children
                .map(formatter(_, level + 2))
                .toVector
                .mkString("")
            }
            case apl: IR.Applet => {
              Utils.genNSpaces((level + 1) * 2) + name + "\n" + children
                .map(formatter(_, level + 2))
                .toVector
                .mkString("")
            }
          }
        }
      }
      .toVector
      .mkString("")
  }
}
