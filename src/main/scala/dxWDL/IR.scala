// Intermediate Representation (IR)
//
// Representation the compiler front end generates from a WDL
// workflow. The compiler back end uses it to generate a
// dx:workflow. A more detailed description can be found at
// ToplevelDir/[IntermediateForm.md].
package dxWDL

import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.WdlExpression
import wdl4s.types._
import wdl4s.values._

object IR {

    // Compile time representation of a variable. Used also as
    // an applet argument.
    // At compile time, we need to keep track of the syntax-tree, for error
    // reporting purposes.
    case class CVar(name: String, wdlType: WdlType, ast: Ast) {
        // dx does not allow dots in variable names, so we
        // convert them to underscores.
        //
        // TODO: check for collisions that are created this way.
        def dxVarName : String = Utils.transformVarName(name)
    }

    // There are several kinds of applets
    //   Eval:      evaluate WDL expressions, pure calculation
    //   Scatter:   utility block for scatter/gather
    //   Task:      call a task, execute a shell command (usually)
    sealed trait AppletKind
    case object Eval extends AppletKind
    case class Scatter(sourceCalls: Vector[String]) extends AppletKind
    case object Task extends AppletKind


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
    sealed trait InstanceTypeSpec
    case object InstTypeDefault extends InstanceTypeSpec
    case class InstTypeConst(name: String) extends InstanceTypeSpec
    case object InstTypeRuntime extends InstanceTypeSpec

    /** @param name          Name of applet
      * @param input         WDL input arguments
      * @param output        WDL output arguments
      * @param instaceType   a platform instance name
      * @param docker        is docker used?
      * @param destination   folder path on the platform
      * @param kind          Kind of applet: task, scatter, ...
      * @param sourceCalls   Calls in source WDL that are handled by this applet.
      *                      In scatters, these are all the calls in the block. For a task,
      *                      it is an empty list.
      * @param wdlCode       WDL source code to run
      */
    case class Applet(name: String,
                      inputs: Vector[CVar],
                      outputs: Vector[CVar],
                      instanceType: InstanceTypeSpec,
                      docker: Boolean,
                      destination : String,
                      kind: AppletKind,
                      wdlCode: String)

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

    // Human readable representation of the IR, with YAML
    def yaml(cVar: CVar) : YamlObject = {
        YamlObject(
            YamlString("type") -> YamlString(cVar.wdlType.toWdlString),
            YamlString("name") -> YamlString(cVar.name),
            YamlString("dxName") -> YamlString(cVar.dxVarName)
        )
    }

    def yaml(applet: Applet) : YamlObject = {
        val inputs = applet.inputs.map(yaml)
        val outputs = applet.outputs.map(yaml)
        val docker: Map[YamlValue, YamlValue] = applet.docker match {
            case false => Map()
            case true => Map(YamlString("docker") -> YamlBoolean(true))
        }
        val instanceType: Map[YamlValue, YamlValue] = applet.instanceType match {
            case InstTypeDefault => Map()
            case InstTypeConst(x) => Map(YamlString("instanceType") -> YamlString(x))
            case InstTypeRuntime  => Map(YamlString("instanceType") -> YamlString("calculated at runtime"))
        }
        val m: Map[YamlValue, YamlValue] = Map(
            YamlString("name") -> YamlString(applet.name),
            YamlString("inputs") -> YamlArray(inputs.toVector),
            YamlString("outputs") -> YamlArray(outputs.toVector),
            YamlString("destination") -> YamlString(applet.destination),
            YamlString("kind") -> YamlString(applet.kind.toString),
            YamlString("wdlCode") -> YamlString(applet.wdlCode)
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

    // Rename member accesses inside an expression, from
    // the form A.x to A_x. This is used inside an applet WDL generated code.
    //
    // Here, we take a shortcut, and just replace strings, instead of
    // doing a recursive syntax analysis (see ValueEvaluator wdl4s
    // module).
    def exprRenameVars(expr: WdlExpression,
                       allVars: Vector[IR.CVar]) : WdlExpression = {
        var sExpr: String = expr.toWdlString
        for (cVar <- allVars) {
            // A.x => A_x
            sExpr = sExpr.replaceAll(cVar.name, cVar.dxVarName)
        }
        WdlExpression.fromString(sExpr)
    }
}
