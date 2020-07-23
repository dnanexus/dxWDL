/** Describe the workflow in a tree representation
  */
package dx.compiler

import spray.json._
import Native.ExecRecord
import IR._

import dx.api.DxWorkflow
import dx.api.Field
import dx.core.util.CompressionUtils

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
    val vec = wf.stages.map { stage =>
      val calleeRecord = execDict(stage.calleeName)
      val jsv: JsValue = apply(calleeRecord)
      JsObject("stage_name" -> JsString(stage.description), "callee" -> jsv)
    }
    val stages = JsArray(vec)
    JsObject("name" -> JsString(wf.name), "kind" -> JsString("workflow"), "stages" -> stages)
  }

  def apply(primary: Native.ExecRecord): JsValue = {
    primary.callable match {
      case apl: IR.Applet if primary.links.isEmpty =>
        JsObject("name" -> JsString(apl.name),
                 "id" -> JsString(primary.dxExec.id),
                 "kind" -> JsString(kindToString(apl.kind)))

      case apl: IR.Applet =>
        // applet that calls other applets/workflows at runtime.
        // recursively describe all called elements.
        val links: Vector[JsValue] = primary.links.map { eli =>
          val calleeRecord = execDict(eli.name)
          apply(calleeRecord)
        }
        JsObject("name" -> JsString(apl.name),
                 "id" -> JsString(primary.dxExec.id),
                 "kind" -> JsString(kindToString(apl.kind)),
                 "executables" -> JsArray(links))

      case wf: IR.Workflow =>
        val vec = wf.stages.map { stage =>
          val calleeRecord = execDict(stage.calleeName)
          val jsv: JsValue = apply(calleeRecord)
          JsObject("stage_name" -> JsString(stage.description), "callee" -> jsv)
        }
        val stages = JsArray(vec)
        JsObject("name" -> JsString(wf.name),
                 "id" -> JsString(primary.dxExec.id),
                 "kind" -> JsString("workflow"),
                 "stages" -> stages)
    }
  }
}

object Tree {

  val CANNOT_FIND_EXEC_TREE = "Unable to find exec tree from"

  val INDENT = 3
  val LAST_ELEM = s"└${"─" * INDENT}"
  val MID_ELEM = s"├${"─" * INDENT}"

  def formDXworkflow(workflow: DxWorkflow): JsValue = {
    val execTree = workflow.describe(Set(Field.Details)).details match {
      case Some(x: JsValue) =>
        x.asJsObject.fields.get("execTree") match {
          case Some(JsString(execString)) => execString
          case _                          => throw new Exception(s"${CANNOT_FIND_EXEC_TREE} for ${workflow.id}")
        }
      case None => throw new Exception(s"${CANNOT_FIND_EXEC_TREE} for ${workflow.id}")
    }

    val TreeJS = CompressionUtils.base64DecodeAndGunzip(execTree).parseJson.asJsObject

    JsObject(
        TreeJS.fields + ("id" -> JsString(workflow.id))
    )
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
  def generateTreeFromJson(TreeJS: JsObject,
                           stageDesc: Option[String] = None,
                           prefix: String = ""): String = {

    TreeJS.fields.get("kind") match {
      case Some(JsString("workflow")) => processWorkflow(prefix, TreeJS)
      case Some(JsString(_))          => processApplets(prefix, stageDesc, TreeJS)
      case _                          => throw new Exception(s"Missing 'kind' field to be in execTree's entry ${TreeJS}.")
    }
  }

  private def determineDisplayName(stageDesc: Option[String], name: String): String = {
    stageDesc match {
      case Some(name) => name
      case None       => s"${name}"
    }
  }

  private def generateWholePrefix(prefix: String, isLast: Boolean): String = {
    val commonPrefix = prefix.replace("└", " ").replace("─", " ")
    if (isLast) {
      commonPrefix.replace("├", "│") + LAST_ELEM
    } else {
      commonPrefix.replace("├", " ") + MID_ELEM
    }
  }

  private def generateTreeBlock(prefix: String,
                                links: Vector[String],
                                title: String,
                                name: String): String = {
    if (links.nonEmpty) {
      prefix + Console.CYAN + title + name + Console.RESET + "\n" + links
        .mkString("\n")
    } else {
      prefix + Console.CYAN + title + name + Console.RESET
    }
  }

  private def processWorkflow(prefix: String, TreeJS: JsObject): String = {
    TreeJS.getFields("name", "stages") match {
      case Seq(JsString(wfName), JsArray(stages)) => {
        val stageLines = stages.zipWithIndex.map {
          case (stage, index) => {
            val isLast = index == stages.length - 1
            val wholePrefix = generateWholePrefix(prefix, isLast)
            val (stageName, callee) = stage.asJsObject.getFields("stage_name", "callee") match {
              case Seq(JsString(stageName), callee: JsObject) => (stageName, callee)
              case x                                          => throw new Exception(s"something is wrong ${x}")
            }
            generateTreeFromJson(callee, Some(stageName), wholePrefix)
          }
        }
        generateTreeBlock(prefix, stageLines, "Workflow: ", Console.YELLOW + wfName)
      }
    }
  }

  private def processApplets(prefix: String,
                             stageDesc: Option[String],
                             TreeJS: JsObject): String = {
    TreeJS.getFields("name", "id", "kind", "executables") match {
      case Seq(JsString(stageName), JsString(_), JsString(kind)) => {
        val name = determineDisplayName(stageDesc, stageName)
        generateTreeBlock(prefix, Vector.empty, s"App ${kind}: ", Console.WHITE + name)

      }
      case Seq(JsString(stageName), JsString(_), JsString(kind), JsArray(executables)) => {
        val links = executables.zipWithIndex.map {
          case (link, index) => {
            val isLast = index == (executables.size - 1)
            val wholePrefix = generateWholePrefix(prefix, isLast)
            generateTreeFromJson(link.asJsObject, None, wholePrefix)
          }
        }
        val name = determineDisplayName(stageDesc, stageName)
        generateTreeBlock(prefix, links, s"App ${kind}: ", Console.WHITE + name)
      }
      case _ => throw new Exception(s"Missing id, name or kind in ${TreeJS}.")
    }
  }
}
