// Intermediate Representation (IR)
//
// Representation the compiler front end generates from a WDL
// workflow. The compiler back end uses it to generate a
// dx:workflow. A more detailed description can be found at
// ToplevelDir/[IntermediateForm.md].
package dxWDL

import net.jcazevedo.moultingyaml._
import wdl4s.wdl.WdlNamespace
import wdl4s.wdl.types._
import wdl4s.wdl.values._

object IR {

    // Compile time representation of a variable. Used also as
    // an applet argument. We keep track of the syntax-tree, for error
    // reporting purposes.
    //
    // The attributes are used to encode DNAx applet input/output
    // specification fields, such as {help, suggestions, patterns}.
    case class CVar(name: String,
                    wdlType: WdlType,
                    attrs: DeclAttrs,
                    ast: wdl4s.parser.WdlParser.Ast) {
        // dx does not allow dots in variable names, so we
        // convert them to underscores.
        //
        // TODO: check for collisions that are created this way.
        def dxVarName : String = Utils.transformVarName(name)
    }

    // There are several kinds of applets
    //   Eval:      evaluate WDL expressions, pure calculation
    //   If:        block for a conditional
    //   Scatter:   utility block for scatter/gather
    //   Task:      call a task, execute a shell command (usually)
    //   WorkflowOutputs: evaluate workflow outputs, and clean up
    //              intermediate results if needed.
    sealed trait AppletKind
    case object AppletKindEval extends AppletKind
    case class AppletKindIf(sourceCalls: Vector[String]) extends AppletKind
    case class AppletKindScatter(sourceCalls: Vector[String]) extends AppletKind
    case object AppletKindTask extends AppletKind
    case object AppletKindWorkflowOutputs extends AppletKind
    case object AppletKindWorkflowOutputsAndReorg extends AppletKind

    /** Secification of instance type.
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
    case class InstanceTypeConst(name: String) extends InstanceType
    case object InstanceTypeRuntime extends InstanceType

    /** @param name          Name of applet
      * @param input         WDL input arguments
      * @param output        WDL output arguments
      * @param instaceType   a platform instance name
      * @param docker        is docker used?
      * @param destination   folder path on the platform
      * @param kind          Kind of applet: task, scatter, ...
      * @param ns            WDL namespace
      */
    case class Applet(name: String,
                      inputs: Vector[CVar],
                      outputs: Vector[CVar],
                      instanceType: InstanceType,
                      docker: Boolean,
                      destination : String,
                      kind: AppletKind,
                      ns: WdlNamespace)

    /** An input to a stage. Could be empty, a wdl constant, or
      * a link to an output variable from another stage.
      */
    sealed trait SArg
    case object SArgEmpty extends SArg
    case class SArgConst(wdlValue: WdlValue) extends SArg
    case class SArgLink(stageName: String, argName: CVar) extends SArg

    case class Stage(name: String,
                     appletName: String,
                     inputs: Vector[SArg],
                     outputs: Vector[CVar])

    case class Workflow(name: String,
                        stages: Vector[Stage],
                        applets: Vector[Applet])

    case class Namespace(workflow: Option[Workflow],
                         applets: Vector[Applet])

    // Human readable representation of the IR, with YAML
    def yaml(cVar: CVar) : YamlObject = {
        val m : Map[YamlValue, YamlValue] = Map(
            YamlString("type") -> YamlString(cVar.wdlType.toWdlString),
            YamlString("name") -> YamlString(cVar.name),
            YamlString("dxName") -> YamlString(cVar.dxVarName)
        )
        val attrs: Map[YamlValue, YamlValue] = cVar.attrs.m.map{
            case (k,v) => YamlString(k) -> YamlString(v.toString)
        }.toMap
        YamlObject(m ++ attrs)
    }

    def yaml(applet: Applet) : YamlObject = {
        val inputs = applet.inputs.map(yaml)
        val outputs = applet.outputs.map(yaml)
        val docker: Map[YamlValue, YamlValue] = applet.docker match {
            case false => Map()
            case true => Map(YamlString("docker") -> YamlBoolean(true))
        }
        val instanceType: Map[YamlValue, YamlValue] = applet.instanceType match {
            case InstanceTypeDefault => Map()
            case InstanceTypeConst(x) => Map(YamlString("instanceType") -> YamlString(x))
            case InstanceTypeRuntime  => Map(YamlString("instanceType") -> YamlString("calculated at runtime"))
        }
        val wdlCode:String = WdlPrettyPrinter(false, None)
            .apply(applet.ns, 0)
            .mkString("\n")
        val m: Map[YamlValue, YamlValue] = Map(
            YamlString("name") -> YamlString(applet.name),
            YamlString("inputs") -> YamlArray(inputs.toVector),
            YamlString("outputs") -> YamlArray(outputs.toVector),
            YamlString("destination") -> YamlString(applet.destination),
            YamlString("kind") -> YamlString(applet.kind.toString),
            YamlString("wdlCode") -> YamlString(wdlCode)
        )
        YamlObject(m ++ docker ++ instanceType)
    }

    def yaml(sArg: SArg) : YamlValue = {
        sArg match {
            case SArgEmpty => YamlString("empty")
            case SArgConst(wVal) => YamlString(wVal.toWdlString)
            case SArgLink(stageName, cVar) => YamlString(stageName + "->" + cVar.name)
        }
    }

    def yaml(stage: Stage) : YamlObject = {
        val inputs = stage.inputs.map(yaml)
        val outputs = stage.outputs.map(yaml)
        YamlObject(
            YamlString("name") -> YamlString(stage.name),
            YamlString("appletName") -> YamlString(stage.appletName),
            YamlString("inputs") -> YamlArray(inputs.toVector),
            YamlString("outputs") -> YamlArray(outputs.toVector)
        )
    }

    def yaml(wf: Workflow) : YamlObject = {
        val stages = wf.stages.map(yaml)
        val applets = wf.applets.map(yaml)
        YamlObject(
            YamlString("name") -> YamlString(wf.name),
            YamlString("stages") -> YamlArray(stages.toVector),
            YamlString("applets") -> YamlArray(applets.toVector)
        )
    }

    def yaml(ns: Namespace) : YamlObject = {
        ns.workflow match {
            case None =>
                YamlObject(
                    YamlString("applets") -> YamlArray(ns.applets.map(yaml))
                )
            case Some(wf) =>
                YamlObject(
                    YamlString("workflow") -> yaml(wf),
                    YamlString("applets") -> YamlArray(ns.applets.map(yaml))
                )
        }
    }
}
