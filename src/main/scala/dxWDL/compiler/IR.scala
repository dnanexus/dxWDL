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
import wdl.WdlNamespace
import wdl.types._
import wom.types.WomType
import wom.values._

object IR {
    private val INVALID_AST = wdl.AstTools.getAst("", "")

    // Compile time representation of a variable. Used also as
    // an applet argument. We keep track of the syntax-tree, for error
    // reporting purposes.
    //
    // The attributes are used to encode DNAx applet input/output
    // specification fields, such as {help, suggestions, patterns}.
    //
    // The [originalFqn] is for a special case where a required call input
    // was unspecified in the workflow. It can still be provided
    // at the command line, or from an input file.
    case class CVar(name: String,
                    womType: WomType,
                    attrs: DeclAttrs,
                    ast: wdl4s.parser.WdlParser.Ast,
                    originalFqn: Option[String] = None) {
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
    //   Eval:      evaluate WDL expressions, pure calculation
    //   If:        block for a conditional
    //   Native:    a native platform applet
    //   Scatter:   utility block for scatter/gather
    //   ScatterCollect: utility block for scatter, with a gather subjob.
    //   Task:      call a task, execute a shell command (usually)
    //   WorkflowOutputReorg: move intermediate result files to a subdirectory.
    sealed trait AppletKind
    case object AppletKindEval extends AppletKind
    case class  AppletKindIf(calls: Map[String, String]) extends AppletKind
    case class  AppletKindNative(id: String) extends AppletKind
    case class  AppletKindScatter(calls: Map[String, String]) extends AppletKind
    case class  AppletKindScatterCollect(calls: Map[String, String]) extends AppletKind
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

    // A stage can call an applet or a workflow
    case class Stage(name: String,
                     id: Utils.DXWorkflowStage,
                     calleeName: String,
                     inputs: Vector[SArg],
                     outputs: Vector[CVar])

    /** A workflow output is linked to the stage that
      * generated it.
      */
    case class Workflow(name: String,
                        inputs: Vector[(CVar,SArg)],
                        outputs: Vector[(CVar,SArg)],
                        stages: Vector[Stage],
                        locked: Boolean) extends Callable {
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
                         importedAs: Option[String],
                         entrypoint: Option[Workflow],
                         subWorkflows: Map[String, Workflow],
                         applets: Map[String, Applet]) {
        // Make a list of all workflows
        def listWorkflows : Vector[Workflow] = {
            val subWf : Vector[Workflow] = subWorkflows.map{ case (_, wf) => wf}.toVector
            entrypoint.toVector ++ subWf
        }

        // Make a list of all the workflows and applets that can be
        // called, if we import this namespace. Index them by their
        // unqualified name.
        def buildCallables: Map[String, IR.Callable] = {
            applets ++ subWorkflows ++ entrypoint.map(wf => wf.name -> wf)
        }
    }


    // Automatic conversion to/from Yaml
    object IrInternalYamlProtocol extends DefaultYamlProtocol {
        implicit val InstanceTypeConstFormat = yamlFormat4(InstanceTypeConst)

        implicit object InstanceTypeYamlFormat extends YamlFormat[InstanceType] {
            def write(it: InstanceType) =
                it match {
                    case InstanceTypeDefault =>
                        YamlString("Default")
                    case itc : InstanceTypeConst =>
                        itc.toYaml
                    case InstanceTypeRuntime =>
                        YamlString("Runtime")
                }
            def read(value: YamlValue) = value match {
                case YamlString("Default") => InstanceTypeDefault
                case YamlString("Runtime") => InstanceTypeRuntime
                case YamlObject(_) => value.convertTo[InstanceTypeConst]
                case unrecognized => throw new Exception(s"InstanceType expected ${unrecognized}")
            }
        }

        implicit object DockerImageYamlFormat extends YamlFormat[DockerImage] {
            def write(it: DockerImage) : YamlValue =
                it match {
                    case DockerImageNone =>
                        YamlString("None")
                    case DockerImageNetwork =>
                        YamlString("Network")
                    case DockerImageDxAsset(dxAsset) =>
                        val assetJs = Utils.jsValueOfJsonNode(dxAsset.getLinkAsJson)
                        YamlObject(YamlString("asset") -> YamlString(assetJs.prettyPrint))
                }
            def read(value: YamlValue) : DockerImage = value match {
                case YamlString("None") => DockerImageNone
                case YamlString("Network") => DockerImageNetwork
                case YamlObject(_) =>
                    value.asYamlObject.getFields(YamlString("asset")) match {
                        case Seq(YamlString(buf)) =>
                            val assetJs = buf.parseJson
                            val dxRecord = assetJs.asJsObject.getFields("id") match {
                                case Seq(JsString(rid)) =>
                                    DXRecord.getInstance(rid)
                                case _ => throw new Exception(s"Unexpected dx:asset ${assetJs.prettyPrint}")
                            }
                            DockerImageDxAsset(dxRecord)
                    }
                case unrecognized =>
                    throw new Exception(s"DocketImage expected ${unrecognized}")
            }
        }


        implicit object AppletKindYamlFormat  extends YamlFormat[AppletKind] {
            def write(aKind: AppletKind) =
                aKind match {
                    case AppletKindEval =>
                        YamlObject(YamlString("aKind") -> YamlString("Eval"))
                    case AppletKindIf(calls) =>
                        YamlObject(
                            YamlString("aKind") -> YamlString("If"),
                            YamlString("calls") -> calls.toYaml)
                    case AppletKindNative(id) =>
                        YamlObject(
                            YamlString("aKind") -> YamlString("Native"),
                            YamlString("id") -> YamlString(id))
                    case AppletKindScatter(calls) =>
                        YamlObject(
                            YamlString("aKind") -> YamlString("Scatter"),
                            YamlString("calls") -> calls.toYaml)
                    case AppletKindScatterCollect(calls) =>
                        YamlObject(
                            YamlString("aKind") -> YamlString("ScatterCollect"),
                            YamlString("calls") -> calls.toYaml)
                    case AppletKindTask =>
                        YamlObject(YamlString("aKind") -> YamlString("Task"))
                    case AppletKindWorkflowOutputReorg =>
                        YamlObject(YamlString("aKind") -> YamlString("WorkflowOutputReorg"))
                }
            def read(value: YamlValue) = value match {
                case YamlObject(_) =>
                    val yo = value.asYamlObject
                    yo.getFields(YamlString("aKind")) match {
                        case Seq(YamlString("Eval")) =>
                            AppletKindEval
                        case Seq(YamlString("Native")) =>
                            yo.getFields(YamlString("id")) match {
                                case Seq(YamlString(id)) =>
                                    AppletKindNative(id)
                            }
                        case Seq(YamlString("Task")) =>
                            AppletKindTask
                        case Seq(YamlString("WorkflowOutputReorg")) =>
                            AppletKindWorkflowOutputReorg
                        case Seq(YamlString("If")) =>
                            yo.getFields(YamlString("calls")) match {
                                case Seq(calls) =>
                                    AppletKindIf(calls.convertTo[Map[String, String]])
                            }
                        case Seq(YamlString("Scatter")) =>
                            yo.getFields(YamlString("calls")) match {
                                case Seq(calls) =>
                                    AppletKindScatter(calls.convertTo[Map[String, String]])
                            }
                        case Seq(YamlString("ScatterCollect")) =>
                            yo.getFields(YamlString("calls")) match {
                                case Seq(calls) =>
                                    AppletKindScatterCollect(calls.convertTo[Map[String, String]])
                            }
                    }
                case unrecognized => throw new Exception(s"AppletKind expected ${unrecognized}")
            }
        }

        implicit object DeclAttrsYamlFormat extends YamlFormat[DeclAttrs] {
            def write(dAttrs: DeclAttrs) : YamlValue = {
                val yAttrs:Map[YamlValue, YamlValue] = dAttrs.m.map{
                    case (key,wVal) =>
                        YamlString(key) -> YamlObject(
                            YamlString("womType") -> YamlString(wVal.womType.toDisplayString),
                            YamlString("value") ->  YamlString(wVal.toWomString))

                }.toMap
                YamlObject(yAttrs)
            }
            def read(value: YamlValue) : DeclAttrs = {
                val m:Map[String, WomValue] = value match {
                    case YamlObject(fields) =>
                        fields.map{
                            case (YamlString(key), obj) =>
                                val yo:YamlObject = obj.asYamlObject
                                yo.getFields(YamlString("womType"),
                                             YamlString("value")) match {
                                    case Seq(YamlString(womType), YamlString(value)) =>
                                        val t:WomType = WdlFlavoredWomType.fromDisplayString(womType)
                                        val v:WomValue =
                                            WdlFlavoredWomType.FromString(t).fromWorkflowSource(value)
                                        key -> v
                                    case _ =>
                                        throw new Exception(s"Malformed attributes ${key} ${yo.prettyPrint}")
                                }
                            case _ =>
                                throw new Exception(s"Malformed attributes ${value}")
                        }
                    case _ =>
                        throw new Exception(s"Malformed attributes ${value}")
                }
                DeclAttrs(m)
            }
        }

        implicit object CVarYamlFormat extends YamlFormat[CVar] {
            def write(cVar: CVar) = {
                val m : Map[YamlValue, YamlValue] = Map(
                    YamlString("type") -> YamlString(cVar.womType.toDisplayString),
                    YamlString("name") -> YamlString(cVar.name),
                    YamlString("attributes") -> cVar.attrs.toYaml,
                    YamlString("originalFqn") -> cVar.originalFqn.toYaml
                )
                YamlObject(m)
            }

            def read(value: YamlValue) = {
                value.asYamlObject.getFields(YamlString("type"),
                                             YamlString("name"),
                                             YamlString("attributes"),
                                             YamlString("originalFqn")) match {
                    case Seq(YamlString(womType), YamlString(name), attrs, originalFqn) =>
                        new CVar(name,
                                 WdlFlavoredWomType.fromDisplayString(womType),
                                 attrs.convertTo[DeclAttrs],
                                 INVALID_AST,
                                 originalFqn.convertTo[Option[String]])
                    case unrecognized =>
                        throw new Exception(s"CVar expected ${unrecognized}")
                }
            }
        }

        implicit object SArgYamlFormat extends YamlFormat[SArg] {
            def write(sArg: SArg) = {
                sArg match {
                    case SArgEmpty =>
                        YamlObject(YamlString("kind") -> YamlString("empty"))
                    case SArgConst(wVal) =>
                        YamlObject(YamlString("kind") -> YamlString("const"),
                                   YamlString("womType") -> YamlString(wVal.womType.toDisplayString),
                                   YamlString("value") ->  YamlString(wVal.toWomString))
                    case SArgLink(stageName, cVar) =>
                        YamlObject(YamlString("kind") -> YamlString("link"),
                                   YamlString("stageName") -> YamlString(stageName),
                                   YamlString("cVar") -> cVar.toYaml)
                    case SArgWorkflowInput(cVar) =>
                        YamlObject(YamlString("kind") -> YamlString("workflow_input"),
                                   YamlString("cVar") -> cVar.toYaml)
                }
            }

            def read(value: YamlValue) = {
                value match {
                    case YamlObject(_) =>
                        val yo = value.asYamlObject
                        yo.getFields(YamlString("kind")) match {
                            case Seq(YamlString("empty")) =>
                                SArgEmpty
                            case Seq(YamlString("const")) =>
                                yo.getFields(YamlString("womType"),
                                             YamlString("value")) match {
                                    case Seq(YamlString(womType), YamlString(value)) =>
                                        val t:WomType = WdlFlavoredWomType.fromDisplayString(womType)
                                        val v = WdlFlavoredWomType.FromString(t).fromWorkflowSource(value)
                                        SArgConst(v)
                                    case _ => throw new Exception("SArg malformed const")
                                }
                            case Seq(YamlString("link")) =>
                                yo.getFields(YamlString("stage"),
                                             YamlString("cVar")) match {
                                    case Seq(YamlString(stageName),
                                             cVar) =>
                                        SArgLink(stageName, cVar.convertTo[CVar])
                                    case _ => throw new Exception("SArg malformed link")
                                }
                            case Seq(YamlString("workflow_input")) =>
                                yo.getFields(YamlString("cVar")) match {
                                    case Seq(cVar) =>
                                        SArgWorkflowInput(cVar.convertTo[CVar])
                                    case _ => throw new Exception("SArg malformed link")
                                }
                            case unrecognized =>
                                throw new Exception(s"CVar expected ${unrecognized}")
                        }
                    case unrecognized =>
                        throw new Exception(s"CVar expected ${unrecognized}")
                }
            }
        }

        implicit object AppletFormat extends YamlFormat[Applet] {
            def write(applet: Applet) = {
                // discard empty lines
                val lines = WdlPrettyPrinter(false, None).apply(applet.ns, 0)
                val wdlCode = lines.map{ x =>
                    if (!x.trim.isEmpty) Some(x)
                    else None
                }.flatten.mkString("\n")
                YamlObject(
                    YamlString("name") -> YamlString(applet.name),
                    YamlString("inputs") -> applet.inputs.toYaml,
                    YamlString("outputs") -> applet.outputs.toYaml,
                    YamlString("instanceType") -> applet.instanceType.toYaml,
                    YamlString("docker") -> applet.docker.toYaml,
                    YamlString("kind") -> applet.kind.toYaml,
                    YamlString("ns") -> YamlString(wdlCode))
            }

            def read(value: YamlValue) = {
                value match {
                    case YamlObject(_) =>
                        value.asYamlObject.getFields(
                            YamlString("name"),
                            YamlString("inputs"),
                            YamlString("outputs"),
                            YamlString("instanceType"),
                            YamlString("docker"),
                            YamlString("kind"),
                            YamlString("ns")) match {
                            case Seq(
                                YamlString(name),
                                inputs,
                                outputs,
                                instanceType,
                                docker,
                                kind,
                                YamlString(wdlCode)) =>
                                Applet(name,
                                       inputs.convertTo[Vector[CVar]],
                                       outputs.convertTo[Vector[CVar]],
                                       instanceType.convertTo[InstanceType],
                                       docker.convertTo[DockerImage],
                                       kind.convertTo[AppletKind],
                                       WdlNamespace.loadUsingSource(wdlCode, None, None).get)
                        }
                    case unrecognized =>
                        throw new Exception(s"Applet expected ${unrecognized}")
                }
            }
        }

        implicit val dxWorkflowStageFormat = yamlFormat1(Utils.DXWorkflowStage)
        implicit val stageFormat = yamlFormat5(Stage)
        implicit val workflowFormat = yamlFormat5(Workflow)
        implicit val namespaceFormat = yamlFormat5(Namespace)
    }
    import IrInternalYamlProtocol._

    // convenience methods, so we don't need to export the InternalYamlProtocol
    def yaml(cVar: CVar) = cVar.toYaml
    def yaml(sArg: SArg) : YamlValue = sArg.toYaml
    def yaml(wf: Workflow) : YamlValue = wf.toYaml
    def yaml(ns: Namespace) : YamlValue = ns.toYaml

    def prettyPrint(y: YamlValue) : String = {
        y.prettyPrint
/*        y.print(Flow,
                ScalarStyle.DEFAULT,
                LineBreak.DEFAULT)*/
    }
}
