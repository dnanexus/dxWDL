/** Describe the workflow in a tree representation
  */
package dxWDL.compiler

import spray.json._

import Native.ExecRecord
import IR._
import dxWDL.base.Utils
import dxWDL.dx.{DxWorkflow, Field}

object KindString {
  val NATIVE = "Native"
  val TASK = "Task"
  val FRAGMENT = "Fragment"
  val INPUTS = "Inputs"
  val OUTPUTS = "Outputs"
  val REORG_OUTPUT = "Reorg outputs"
  val OUTPUT_REORG = "Output Reorg"
  val CUSTOM_REORG = "Custom reorg"
  val WORKFLOW = "workflow"
}

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

  def fromWorkflowIR(wf: IR.Workflow): JsValue = {
    val vec = wf.stages.map {
      case stage =>
        val calleeRecord = execDict(stage.calleeName)
        val jsv: JsValue = apply(calleeRecord)
        JsObject("stage_name" -> JsString(stage.description), "callee" -> jsv)
    }.toVector
    val stages = JsArray(vec)
    JsObject("name" -> JsString(wf.name), "kind" -> JsString("workflow"), "stages" -> stages)
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

  /** Recursivly traverse the exec tree and generate an appropriate name + color based on the node type.
    * The prefix is built up as recursive calls happen. This allows for mainaining the locations of branches
    * in the tree. When a prefix made for a current node, it undergoes a transformation to strip out any
    * extra characters from previous calls. This maintains the indenation level and tree branches.
    *
    * Color scheme:
    *   'types' are in CYAN, types being Workflow, App, Task etc
    *   'names' are in WHITE for all app types, and YELLOW for workflow types
    *
    * Step by step:
    *
    *       TREE                                            LEVEL               prettyPrint calls (approx)                                                         NOTES
    * Workflow: four_levels                                 0. Workflow         0a. prettyPrint(IR.Workflow, None)
    * ├───App Inputs: common                                1. App              1a. prettyPrint(IR.App, Some("common"), 3, "├───")
    * ├───App Fragment: if ((username == "a"))              1. App              1b. prettyPrint(IR.AppFrag, Some("if ((username == "a"))", 3, "├───")              The starting prefix for 6 would be ├───└───, that combo gets fixed to be what you actually see by the replace rules
    * │   └───Workflow: four_levels_block_0                 2. Workflow         2a. prettyPrint(IR.Workflow, None, 3, "│   └───")
    * │       ├───App Task: c1                              3. App              3a. prettyPrint(IR.App, None, 3, "│       ├───")                                   The prefix in the step before this would have looked like "│   └───├───"
    * │       └───App Task: c2                              3. App              3b. pretyyPrint(IR.App, None, 3, "│       └───")
    * ├───App Fragment: scatter (i in [1, 4, 9])            1. App              1c. prettyPrint(IR.AppFrag, Some("scatter (i in [1, 4, 9])", 3, "├───")
    * │   └───App Fragment: four_levels_frag_4              2. App              2b. prettyPrint(IR.AppFrag, Some("four_levels_frag_4"), 3, "├───├───")
    * │       └───Workflow: four_levels_block_1_0           3. Workflow          3c. prettyPrint(IR.Workflow, None, 3, "│       └───")
    * │           ├───App Fragment: if ((j == "john"))      4. App              4a. prettyPrint(IR.AppFrag, Some("if ((j == "john"))"), 3, "│           ├───")
    * │           │   └───App Task: concat                  5. App              5a. prettyPrint(IR.App, None, 3, "│           │   └───")                           The prefix that would be 'fixed', into this was "│           ├───└───"
    * │           └───App Fragment: if ((j == "clease"))    4. App              4b. prettyPrint(IR.AppFrag, Some("if ((j == "clease"))"), 3, "│           └───")
    * └───App Outputs: outputs                              1. App              1d. prettyPrint(IR.AppFrag, Some("outputs"), 3, "└───")
    * */
  def prettyPrint(primary: Native.ExecRecord,
                  stageDesc: Option[String] = None,
                  indent: Int = 3,
                  prefix: String = ""): String = {

    val lastElem = s"└${"─" * indent}"
    val midElem = s"├${"─" * indent}"

    // Determine the type of the current node
    primary.callable match {
      case wf: IR.Workflow => {
        // If it's a workflow, we know it could have stages, follow each stage node as well
        val stageLines = wf.stages.zipWithIndex.map {
          case (stage, index) => {
            val isLast = index == wf.stages.size - 1
            val postPrefix = if (isLast) lastElem else midElem
            val wholePrefix = if (isLast) {
              // This is the last node at this level, remove any ─ or └ characters fromt he
              // prefix thus far, then append a prefix with a └.
              prefix.replace("├", "│").replace("└", " ").replace("─", " ") + postPrefix
            } else {
              // Not last, strop out previous characters from the prefix and add a ├.
              prefix.replace("├", " ").replace("└", " ").replace("─", " ") + postPrefix
            }
            // For Stages, the stage description field is more useful than the stage name, but it is only
            // available on the workflow node, so pass it to the prettyPrint call that will generate the
            // name for the stage
            prettyPrint(execDict(stage.calleeName), Some(stage.description), indent, wholePrefix)
          }
        }.toVector

        // Append the stages to the workflow node that they originated from, if there are any
        if (stageLines.size > 0) {
          prefix + Console.CYAN + "Workflow: " + Console.YELLOW + wf.name + Console.RESET + "\n" + stageLines
            .mkString("\n")
        } else {
          prefix + Console.CYAN + "Workflow: " + Console.YELLOW + wf.name + Console.RESET
        }
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
                  prefix.replace("├", "│").replace("└", " ").replace("─", " ") + postPrefix
                } else {
                  prefix.replace("├", " ").replace("└", " ").replace("─", " ") + postPrefix
                }
                prettyPrint(execDict(link), None, indent, wholePrefix)
              }
            }.toVector

            // If our applet came from a stage as described by the workflow, use the passed back name
            val name = stageDesc match {
              case Some(name) => name
              case None       => s"${apl.name}"
            }

            if (links.size > 0) {
              prefix + Console.CYAN + s"App ${kindToString(apl.kind)}: " + Console.WHITE + name + Console.RESET + "\n" + links
                .mkString("\n")
            } else {
              prefix + Console.CYAN + s"App ${kindToString(apl.kind)}: " + Console.WHITE + name + Console.RESET
            }
          }
          case _ =>
            // If our applet came from a stage as described by the workflow, use the passed back name
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

object Tree {

  val CANNOT_FIND_EXEC_TREE = "Unable to find exec tree from"

  def formDXworkflow(workflow: DxWorkflow): String = {
    val execTree = workflow.describe(Set(Field.Details)).details match {
      case Some(x: JsValue) =>
        x.asJsObject.fields.get("execTree") match {
          case Some(JsString(execString)) => execString
          case _                          => throw new Exception(s"${CANNOT_FIND_EXEC_TREE} for ${workflow.id}")
        }
      case None => throw new Exception(s"${CANNOT_FIND_EXEC_TREE} for ${workflow.id}")
    }

    val TreeJS = Utils.base64DecodeAndGunzip(execTree).parseJson.asJsObject

    JsObject(
        TreeJS.fields + ("id" -> JsString(workflow.id))
    ).toString

  }

  val NATIVE = "Native"
  val TASK = "Task"
  val FRAGMENT = "Fragment"
  val INPUTS = "Inputs"
  val OUTPUTS = "Outputs"
  val REORG_OUTPUT = "Reorg outputs"
  val OUTPUT_REORG = "Output Reorg"
  val CUSTOM_REORG = "Custom reorg"

  def tranverseTree(TreeJS: JsObject, stageDesc: Option[String] = None, indent: Int = 3, prefix: String = ""): String = {

    val lastElem = s"└${"─" * indent}"
    val midElem = s"├${"─" * indent}"

    TreeJS.fields.get("kind") match {
      case Some(JsString(KindString.NATIVE))
        | Some(JsString(KindString.TASK))
        | Some(JsString(KindString.FRAGMENT))
        | Some(JsString(KindString.INPUTS))
        | Some(JsString(KindString.REORG_OUTPUT))
        | Some(JsString(KindString.CUSTOM_REORG))
      => {
        TreeJS.getFields("name", "id", "kind", "executables") match {

          case Seq(JsString(aplName), JsString(id), JsString(kind)) =>  {
            val name = stageDesc match {
              case Some(name) => name
              case None       => s"${aplName}"
            }
            prefix + Console.CYAN + s"App ${kind}: " + Console.WHITE + name + Console.RESET
          }

          case Seq(JsString(aplName), JsString(id), JsString(aplKind), JsArray(executables)) => {

            val links = executables.zipWithIndex.map {
              case (link, index) => {
                val isLast = index == (executables.size - 1)
                val postPrefix = if (isLast) lastElem else midElem
                val wholePrefix = if (isLast) {
                  prefix.replace("├", "│").replace("└", " ").replace("─", " ") + postPrefix
                } else {
                  prefix.replace("├", " ").replace("└", " ").replace("─", " ") + postPrefix
                }
                tranverseTree(link.asJsObject, None, indent, wholePrefix)
              }

            }.toVector

            val name = stageDesc match {
              case Some(name) => name
              case None       => s"${aplName}"
            }

            if (links.nonEmpty) {
              prefix + Console.CYAN + s"App ${aplKind}: " + Console.WHITE + name + Console.RESET + "\n" + links
                .mkString("\n")
            } else {
              prefix + Console.CYAN + s"App ${aplKind}: " + Console.WHITE + name + Console.RESET
            }
          }
          case _ => throw new Exception("Tree is fucked")
        }
      }

      case Some(JsString(KindString.WORKFLOW)) => {
        TreeJS.getFields("name", "stages") match {
          case Seq(JsString(wfName), JsArray(stages)) => {
            val stageLines = stages.zipWithIndex.map {
              case (stage, index) => {
                val isLast = index == stages.length - 1
                val postPrefix = if (isLast) lastElem else midElem
                val wholePrefix = if (isLast) {
                  // This is the last node at this level, remove any ─ or └ characters fromt he
                  // prefix thus far, then append a prefix with a └.
                  prefix.replace("├", "│").replace("└", " ").replace("─", " ") + postPrefix
                } else {
                  // Not last, strop out previous characters from the prefix and add a ├.
                  prefix.replace("├", " ").replace("└", " ").replace("─", " ") + postPrefix
                }
                // For Stages, the stage description field is more useful than the stage name, but it is only
                // available on the workflow node, so pass it to the prettyPrint call that will generate the
                // name for the stage

                val (stageName, callele) = stage.asJsObject.getFields("stage_name", "callele") match {
                  case Seq(JsString(stageName), callele: JsObject) =>
                    (stageName, callele)
                }
                tranverseTree(callele, Some(stageName), indent, wholePrefix)
              }
            }.toVector

            if (stageLines.nonEmpty) {
              prefix + Console.CYAN + "Workflow: " + Console.YELLOW + wfName + Console.RESET + "\n" + stageLines
                .mkString("\n")
            } else {
              prefix + Console.CYAN + "Workflow: " + Console.YELLOW + wfName + Console.RESET
            }
          }

        }
      }

      case _    => throw new Exception("Tree is fucked.")
    }
  }
}
