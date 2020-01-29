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
  // TODO: add tree style formatting
  // Traverse the exec tree by creating displayble nodes. Children contian more nodes to visit.
  // Converting the exec tree to this format allows for formatting it more easily than trying to do the
  // formatting and traversing at the same time.
  def prettyPrintRec(primary: Native.ExecRecord, stageDesc: Option[String] = None): Displayable = {
    primary.callable match {
      case wf: IR.Workflow => {
        val stageLines = wf.stages.map {
          case stage => {
            prettyPrintRec(execDict(stage.calleeName), Some(stage.description))
          }
        }.toVector

        Displayable(primary,
                    Console.CYAN + "Workflow: " + Console.YELLOW + wf.name + Console.RESET,
                    stageLines)

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
            val result = Displayable(primary, _, links)
            val name = stageDesc match {
              case Some(name) => name
              case None       => s"${apl.name}"
            }
            result(
                Console.CYAN + s"App ${kindToString(apl.kind)}: " + Console.WHITE + name + Console.RESET
            )
          }
          case _ =>
            val result = Displayable(primary, _, Vector())
            val name = stageDesc match {
              case Some(name) => name
              case None       => s"${apl.name}"
            }
            result(
                Console.CYAN + s"App ${kindToString(apl.kind)}: " + Console.WHITE + name + Console.RESET
            )
        }
      }
    }
  }

  // Entry point into the prettyPrintRec recursive function, calls the formatter
  def prettyPrint(primary: Native.ExecRecord): String = {
    val steps = prettyPrintRec(primary)
    formatter(steps)
  }

  // Traverse the Displayable nodes and buildup a single string to print
  def formatter(items: Displayable,
                level: Int = 0,
                last: Boolean = false,
                indent: Int = 3): String = {
    println(s"${items.name}, last? $last")
    Utils.genTree(level, indent, last) + items.name + "\n" + items.children.zipWithIndex
      .map {
        case (element, index) => {
          val last = if (index == items.children.size - 1) true else false
          element match {
            case Displayable(primary, name, children) => {
              primary.callable match {
                case wf: IR.Workflow => {
                  Utils
                    .genTree(level + 1, indent, last) + name + "\n" + children.zipWithIndex
                    .map {
                      // Mark the last element
                      case (e, i) => {
                        println(s">> WF child: $e.name $i / ${children.size}")
                        val last = if (i == children.size - 1) true else false
                        formatter(e, level + 2, last)
                      }
                    }
                    .toVector
                    .mkString("")
                }
                case apl: IR.Applet => {
                  Utils
                    .genTree(level + 1, indent, last) + name + "\n" + children.zipWithIndex
                    .map {
                      // Mark the last element
                      case (e, i) => {
                        println(s">> App child: ${e.name} $i / ${children.size}")
                        val last = if (i == children.size - 1) true else false
                        formatter(e, level + 2, last)
                      }
                    }
                    .toVector
                    .mkString("")
                }
              }
            }
          }
        }
      }
      .toVector
      .mkString("")
  }
}
