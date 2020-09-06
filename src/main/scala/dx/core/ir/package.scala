package dx.core.ir

import dx.api.DxWorkflowStage
import dx.translator.RunSpec.{ContainerImage, InstanceType}
import dx.core.ir.ExecutableType.ExecutableType
import dx.core.ir.Level.Level
import wdlTools.util.Enum

trait ParameterAttribute

/**
  * Compile time representation of a variable. Used also as an applet argument.
  *
  * The fullyQualifiedName could contains dots. However dx does not allow dots
  * in applet/workflow arugment names, this requires some kind of transform.
  *
  * The attributes are used to encode DNAx applet input/output specification
  * fields, such as {help, suggestions, patterns}.
  *
  * @param name parameter name
  * @param dxType parameter data type
  * @param defaultValue default value
  * @param attributes metadata
  */
case class Parameter(
    name: String,
    dxType: Type,
    defaultValue: Option[Value] = None,
    attributes: Vector[ParameterAttribute] = Vector.empty
) {
  // dx does not allow dots in variable names, so we
  // convert them to underscores.
  //
  // TODO: check for collisions that are created this way.
  def dxName: String = {
    val nameNoDots = Parameter.encodeDots(name)
    assert(!nameNoDots.contains("."))
    nameNoDots
  }
}

object Parameter {
  val ComplexValueKey = "___"

  /**
    * Converts dots in parameter names to underscores.
    * DNAnexus does not allow dots in variable names
    * @param name parameter name
    * @return
    */
  def encodeDots(name: String): String = {
    name.replaceAll("\\.", ComplexValueKey)
  }

  def decodeDots(varName: String): String = {
    varName.replaceAll(ComplexValueKey, "\\.")
  }
}

trait CallableAttribute

/**
  * A unified type representing a workflow, app, or applet.
  * This is useful when compiling WDL workflows, because they can
  * call other WDL workflows and applets. This is done using the
  * same syntax.
  */
trait Callable {
  def name: String
  def inputVars: Vector[Parameter]
  def outputVars: Vector[Parameter]
  def attributes: Vector[CallableAttribute]
}

/**
  * @param dependencies: the order in which to compile the workflows and tasks.
  *                      The first element in the vector depends on nothing else.
  *                      Each other element (may) depend on any of the previous
  *                      elements.
  */
case class Bundle(primaryCallable: Option[Callable],
                  allCallables: Map[String, Callable],
                  dependencies: Vector[String],
                  typeAliases: Map[String, Type]) {

  override def toString: String = {
    primaryCallable match {
      case Some(c) => s"Bundle[${c.name}]"
      case None if allCallables.size == 1 =>
        s"Bundle[${allCallables.values.head.name}]"
      case None =>
        s"Bundle[${allCallables.values.head.name} and ${allCallables.size - 1} others]"
    }
  }
}

object ExecutableType extends Enum {
  type ExecutableType = Value
  val App, Applet, Workflow = Value
}

/**
  * Kinds of executables that might be generated:
  *  Native: a native platform app or applet
  *  Task: call a task, execute a shell command (usually)
  *  WfFragment: WDL workflow fragment, can included nested if/scatter blocks
  *  WfInputs handle workflow inputs for unlocked workflows
  *  WfOutputs: evaluate workflow outputs
  *  WorkflowOutputReorg: move intermediate result files to a subdirectory.
  */
sealed trait ExecutableKind
case class ExecutableKindNative(executableType: ExecutableType,
                                id: Option[String] = None,
                                name: Option[String] = None,
                                project: Option[String] = None,
                                path: Option[String] = None)
    extends ExecutableKind
case object ExecutableKindApplet extends ExecutableKind

/**
  * An applet that executes a workflow fragment.
  * @param calls names of calls made in the fragment
  * @param blockPath path to the block represented by this fragment
  * @param inputs mapping of input name to type, where names are encoded
  *               such that any dots are replaced with '\_\_\_'
  */
case class ExecutableKindWfFragment(calls: Vector[String],
                                    blockPath: Vector[Int],
                                    inputs: Map[String, Type])
    extends ExecutableKind
case object ExecutableKindWfInputs extends ExecutableKind
// Output - default and custom reorg
case object ExecutableKindWfOutputs extends ExecutableKind
case object ExecutableKindWfCustomReorgOutputs extends ExecutableKind
// Reorg - default and custom reorg
case object ExecutableKindWorkflowOutputReorg extends ExecutableKind
case class ExecutableKindWorkflowCustomReorg(id: String) extends ExecutableKind

object ExecutableKind {
  def getCommand(kind: ExecutableKind): Option[String] = {
    kind match {
      case ExecutableKindWfInputs             => Some("Inputs")
      case ExecutableKindWfOutputs            => Some("Outputs")
      case ExecutableKindWfCustomReorgOutputs => Some("CustomReorgOutputs")
      case ExecutableKindWorkflowOutputReorg  => Some("OutputReorg")
      case _                                  => None
    }
  }

  def toString(kind: ExecutableKind): String = {
    kind match {
      case _: ExecutableKindNative               => "Native"
      case _: ExecutableKindWfFragment           => "Fragment"
      case ExecutableKindApplet                  => "Task"
      case ExecutableKindWfInputs                => "Inputs"
      case ExecutableKindWfOutputs               => "Outputs"
      case ExecutableKindWfCustomReorgOutputs    => "Reorg outputs"
      case ExecutableKindWorkflowOutputReorg     => "Output Reorg"
      case ExecutableKindWorkflowCustomReorg(id) => s"Custom reorg ${id}"
    }
  }
}

/**
  * Marker trait for runtime requirements.
  */
trait RuntimeRequirement

/**
  * An app or applet.
  * @param name name of application
  * @param inputs input arguments
  * @param outputs output arguments
  * @param instanceType a platform instance name
  * @param container is docker used? if so, what image
  * @param kind kind of application: task, scatter, ...
  * @param document task definition
  * @param attributes additional applet metadata
  * @param requirements runtime resource requirements
  */
case class Application(name: String,
                       inputs: Vector[Parameter],
                       outputs: Vector[Parameter],
                       instanceType: InstanceType,
                       container: ContainerImage,
                       kind: ExecutableKind,
                       document: DocumentSource,
                       attributes: Vector[CallableAttribute] = Vector.empty,
                       requirements: Vector[RuntimeRequirement] = Vector.empty)
    extends Callable {
  def inputVars: Vector[Parameter] = inputs
  def outputVars: Vector[Parameter] = outputs
}

/**
  * An input to a stage. Could be empty, a wdl constant,
  * a link to an output variable from another stage,
  * or a workflow input.
  */
sealed trait StageInput
case object EmptyInput extends StageInput
case class StaticInput(value: Value) extends StageInput
case class LinkInput(stageId: DxWorkflowStage, paramName: String) extends StageInput
case class WorkflowInput(param: Parameter) extends StageInput

// A stage can call an application or a workflow.
//
// Note: the description may contain dots, parentheses, and other special
// symbols. It is shown to the user on the UI. The [id] is unique
// across the workflow.
case class Stage(description: String,
                 id: DxWorkflowStage,
                 calleeName: String,
                 inputs: Vector[StageInput],
                 outputs: Vector[Parameter])

/**
  * A workflow output is linked to the stage that generated it.
  *
  * If [level] is SubWorkflow, then a workflow matches part of a
  * WDL workflow, it is not a first class citizen. It is compiled
  * into a hidden dx:workflow.
  */
object Level extends Enum {
  type Level = Value
  val Top, Sub = Value
}

case class Workflow(name: String,
                    inputs: Vector[(Parameter, StageInput)],
                    outputs: Vector[(Parameter, StageInput)],
                    stages: Vector[Stage],
                    document: WorkflowSource,
                    locked: Boolean,
                    level: Level,
                    attributes: Vector[CallableAttribute] = Vector.empty)
    extends Callable {
  def inputVars: Vector[Parameter] = inputs.map { case (cVar, _)   => cVar }
  def outputVars: Vector[Parameter] = outputs.map { case (cVar, _) => cVar }
}
