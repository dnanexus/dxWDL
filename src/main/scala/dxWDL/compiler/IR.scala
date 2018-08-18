// Intermediate Representation (IR)
//
// Representation the compiler front end generates from a WDL
// workflow. The compiler back end uses it to generate a
// dx:workflow. A more detailed description can be found at
// ToplevelDir/[IntermediateForm.md].
//
// We use YAML as a human readable representation of the IR.
package dxWDL.compiler

import com.dnanexus.DXRecord
import dxWDL.{DeclAttrs, Utils, WdlPrettyPrinter}
import net.jcazevedo.moultingyaml._
import spray.json._
import wdl.draft2.model.WdlNamespace
import wdl.draft2.model.types._
import wom.types.WomType
import wom.values._

object IR {
    private val INVALID_AST =  wdl.draft2.model.AstTools.getAst("", "")

    // Compile time representation of a variable. Used also as
    // an applet argument. We keep track of the syntax-tree, for error
    // reporting purposes.
    //
    // The attributes are used to encode DNAx applet input/output
    // specification fields, such as {help, suggestions, patterns}.
    //
    case class CVar(name: String,
                    womType: WomType,
                    attrs: DeclAttrs,
                    ast: wdl.draft2.parser.WdlParser.Ast) {
        // dx does not allow dots in variable names, so we
        // convert them to underscores.
        //
        // TODO: check for collisions that are created this way.
        def dxVarName : String = Utils.transformVarName(name)
    }

    /** Specification of instance type.
      *
      *  An instance could be:
      *  Default: the platform default, useful for auxiliary calculations.
      *  Const:   instance type is known at compile time. We can start the
      *           job directly on the correct instance type.
      *  Runtime: WDL specifies a calculation for the instance type, based
      *           on information known only at runtime. The generated applet
      *           will need to evalulate the expressions at runtime, and then
      *           start another job on the correct instance type.
      */
    sealed trait InstanceType
    case object InstanceTypeDefault extends InstanceType
    case class InstanceTypeConst(
        dxInstanceType: Option[String],
        memoryMB: Option[Int], diskGB: Option[Int], cpu: Option[Int]
    ) extends InstanceType
    case object InstanceTypeRuntime extends InstanceType

    // A task may specify a docker image to run under. There are three
    // supported options:
    //  None:    no image
    //  Network: the image resides on a network site and requires download
    //  DxAsset: the image is a platform asset
    //
    sealed trait DockerImage
    case object DockerImageNone extends DockerImage
    case object DockerImageNetwork extends DockerImage
    case class DockerImageDxAsset(asset: DXRecord) extends DockerImage

    // A unified type representing a WDL workflow or a WDL applet.
    // This is useful when compiling WDL workflows, because they can
    // call other WDL workflows and applets. This is done using the
    // same syntax.
    sealed trait Callable {
        def name: String
        def inputVars: Vector[CVar]
        def outputVars: Vector[CVar]
    }

    // There are several kinds of applets
    //   Native:     a native platform applet
    //   WfFragment: WDL workflow fragment, can included nested if/scatter blocks
    //   Task:       call a task, execute a shell command (usually)
    //   WorkflowOutputReorg: move intermediate result files to a subdirectory.
    sealed trait AppletKind
    case class  AppletKindNative(id: String) extends AppletKind
    case class  AppletKindWfFragment(calls: Map[String, String]) extends AppletKind
    case object AppletKindTask extends AppletKind
    case object AppletKindWorkflowOutputReorg extends AppletKind

    /** @param name          Name of applet
      * @param input         WDL input arguments
      * @param output        WDL output arguments
      * @param instaceType   a platform instance name
      * @param docker        is docker used? if so, what image
      * @param kind          Kind of applet: task, scatter, ...
      * @param ns            WDL namespace
      */
    case class Applet(name: String,
                      inputs: Vector[CVar],
                      outputs: Vector[CVar],
                      instanceType: InstanceType,
                      docker: DockerImage,
                      kind: AppletKind,
                      ns: WdlNamespace) extends Callable {
        def inputVars = inputs
        def outputVars = outputs
    }

    /** An input to a stage. Could be empty, a wdl constant,
      * a link to an output variable from another stage,
      * or a workflow input.
      */
    sealed trait SArg
    case object SArgEmpty extends SArg
    case class SArgConst(wdlValue: WomValue) extends SArg
    case class SArgLink(stageName: String, argName: CVar) extends SArg
    case class SArgWorkflowInput(argName: CVar) extends SArg

    // Linking between a variable, and which stage we got
    // it from.
    case class LinkedVar(cVar: CVar, sArg: SArg) {
        def yaml : YamlObject = {
            YamlObject(
                YamlString("cVar") -> IR.yaml(cVar),
                YamlString("sArg") -> IR.yaml(sArg)
            )
        }
    }

    // A stage can call an applet or a workflow.
    //
    // Note: the description may concatin dots, parentheses, and other special
    // symbols. It is shown to the user on the UI.
    case class Stage(stageName: String,
                     description: Option[String],
                     id: Utils.DXWorkflowStage,
                     calleeName: String,
                     inputs: Vector[SArg],
                     outputs: Vector[CVar])


    // A toplevel fragment is the initial workflow we started with, minus
    // whatever was replaced or rewritten.
    object WorkflowKind extends Enumeration {
        val TopLevel, Sub  = Value
    }

    /** A workflow output is linked to the stage that
      * generated it.
      */
    case class Workflow(name: String,
                        inputs: Vector[(CVar,SArg)],
                        outputs: Vector[(CVar,SArg)],
                        stages: Vector[Stage],
                        locked: Boolean,
                        kind: WorkflowKind.Value) extends Callable {
        def inputVars = inputs.map{ case (cVar,_) => cVar }.toVector
        def outputVars = outputs.map{ case (cVar,_) => cVar }.toVector
    }

    /** The simplest form of namespace contains a library of tasks.
      * Tasks do not call other tasks, and so they are standalone. A more complex
      * namespace also contains a workflow.
      *
      * The most advanced namespace is one where the top level
      * workflow imports other namespaces, and calls subworkflows and tasks.
      */
    case class Namespace(name: String,
                         entrypoint: Option[Workflow],
                         subWorkflows: Map[String, Workflow],
                         applets: Map[String, Applet])
}
