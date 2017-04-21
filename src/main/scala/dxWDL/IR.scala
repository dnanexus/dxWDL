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
    object AppletKind extends Enumeration {
        val Eval, Scatter, Task = Value
    }

    /** @param name          Name of applet
      * @param input         WDL input arguments
      * @param output        WDL output arguments
      * @param instaceType   a platform instance name
      * @param docker        docker image name
      * @param destination   folder path on the platform
      * @param kind          Kind of applet: task, scatter, ...
      * @param wdlCode       WDL source code to run
      */
    case class Applet(name: String,
                      inputs: Vector[CVar],
                      outputs: Vector[CVar],
                      instanceType: String,
                      docker: Option[String],
                      destination : String,
                      kind: AppletKind.Value,
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

    // remove empty lines from a long string
    private def stripEmptyLines(s: String) : String = {
        val lines = s.split("\n")
        val nonEmptyLines = lines.filter(l => !l.trim().isEmpty)
        nonEmptyLines.mkString("\n")
    }

    def yaml(applet: Applet) : YamlObject = {
        val inputs = applet.inputs.map(yaml)
        val outputs = applet.outputs.map(yaml)
        val docker = applet.docker match {
            case None => Map()
            case Some(x) => Map(YamlString("docker") -> YamlString(x))
        }
        val m: Map[YamlValue, YamlValue] = Map(
            YamlString("name") -> YamlString(applet.name),
            YamlString("inputs") -> YamlArray(inputs.toVector),
            YamlString("outputs") -> YamlArray(outputs.toVector),
            YamlString("instanceType") -> YamlString(applet.instanceType),
            YamlString("destination") -> YamlString(applet.destination),
            YamlString("kind") -> YamlString(applet.kind.toString),
            YamlString("wdlCode") -> YamlString(stripEmptyLines(applet.wdlCode))
        )
        YamlObject(m ++ docker)
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

    def prettyPrint(ir: Workflow) : String = {
        val yo: YamlObject = yaml(ir)
        //yaml.print(Block)
        yo.print()
    }
}
