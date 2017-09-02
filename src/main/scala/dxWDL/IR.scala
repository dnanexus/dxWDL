// Intermediate Representation (IR)
//
// Representation the compiler front end generates from a WDL
// workflow. The compiler back end uses it to generate a
// dx:workflow. A more detailed description can be found at
// ToplevelDir/[IntermediateForm.md].
//
// We use YAML as a human readable representation of the IR.
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
                      destination: String,
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


    // Automatic conversion to/from Yaml
    object IrInternalYamlProtocol extends DefaultYamlProtocol {
        implicit object AppletKindYamlFormat  extends YamlFormat[AppletKind] {
            def write(aKind: AppletKind) =
                aKind match {
                    case AppletKindEval =>
                        YamlArray(YamlString("Eval"))
                    case AppletKindIf(sourceCalls) =>
                        YamlArray(YamlString("If"), sourceCalls.toYaml)
                    case AppletKindScatter(sourceCalls) =>
                        YamlArray(YamlString("Scatter"), sourceCalls.toYaml)
                    case AppletKindTask =>
                        YamlArray(YamlString("Task"))
                    case AppletKindWorkflowOutputs =>
                        YamlArray(YamlString("WorkflowOutputs"))
                    case AppletKindWorkflowOutputsAndReorg =>
                        YamlArray(YamlString("WorkflowOutputsAndReorg"))
                }

            def read(value: YamlValue) = value match {
                case YamlArray(Vector(YamlString("Eval"))) =>
                    AppletKindEval
                case YamlArray(Vector(YamlString("If"), sourceCalls)) =>
                    AppletKindIf(sourceCalls.convertTo[Vector[String]])
                case YamlArray(Vector(YamlString("Scatter"), sourceCalls)) =>
                    AppletKindScatter(sourceCalls.convertTo[Vector[String]])
                case YamlArray(Vector(YamlString("Task"))) =>
                    AppletKindTask
                case YamlArray(Vector(YamlString("WorkflowOutputs"))) =>
                    AppletKindWorkflowOutputs
                case YamlArray(Vector(YamlString("WorkflowOutputsAndReorg"))) =>
                    AppletKindWorkflowOutputsAndReorg
                case unrecognized => deserializationError(s"AppletKind expected ${unrecognized}")
            }
        }

        implicit object CVarYamlFormat extends YamlFormat[CVar] {
            def write(cVar: CVar) = {
                val m : Map[YamlValue, YamlValue] = Map(
                    YamlString("type") -> YamlString(cVar.wdlType.toWdlString),
                    YamlString("name") -> YamlString(cVar.name)
                )
                YamlObject(m)
            }

            def read(value: YamlValue) = {
                value.asYamlObject.getFields(YamlString("type"), YamlString("name"))
                    match {
                    case Seq(YamlString(wdlType), YamlString(name)) =>
                        new CVar(name,
                                 WdlType.fromWdlString(wdlType),
                                 DeclAttrs.empty,
                                 WdlRewrite.INVALID_AST)
                    case unrecognized =>
                        deserializationError(s"CVar expected ${unrecognized}")
                }
            }
        }

        implicit object SArgYamlFormat extends YamlFormat[SArg] {
            def write(sArg: SArg) = {
                sArg match {
                    case SArgEmpty => YamlString("empty")
                    case SArgConst(wVal) =>
                        YamlArray(Vector(YamlString(wVal.wdlType.toWdlString),
                                         YamlString(wVal.toWdlString)))
                    case SArgLink(stageName, cVar) =>
                        YamlObject(YamlString("stageName") -> YamlString(stageName),
                                   YamlString("cVar") -> cVar.toYaml)
                }
            }

            def read(value: YamlValue) = {
                value match {
                    case YamlString("empty") =>
                        SArgEmpty
                    case YamlArray(Vector(YamlString(wdlType), YamlString(wdlValue))) =>
                        val t:WdlType = WdlType.fromWdlString(wdlType)
                        val v = t.fromWdlString(wdlValue)
                        SArgConst(v)
                    case YamlObject(_) =>
                        value.asYamlObject.getFields(YamlString("stage"),
                                                     YamlString("cVar")) match {
                            case Seq(YamlString(stageName),
                                     cVar) =>
                                SArgLink(stageName, cVar.convertTo[CVar])
                        }
                    case unrecognized =>
                        deserializationError(s"CVar expected ${unrecognized}")
                }
            }
        }

//        implicit val appletFormat = yamlFormat7(Applet)
    }
    import IrInternalYamlProtocol._


    def yaml(cVar: CVar) = cVar.toYaml

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
            YamlString("kind") -> applet.kind.toYaml,
            YamlString("wdlCode") -> YamlString(wdlCode)
        )
        YamlObject(m ++ docker ++ instanceType)
    }

    def yaml(sArg: SArg) : YamlValue = sArg.toYaml

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
