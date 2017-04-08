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
    case class CVar(name: String, wdlType: WdlType, ast: Ast)

    // There are several kinds of applets
    //   Eval:      evaluate WDL expressions, pure calculation
    //   Scatter:   utility block for scatter/gather
    //   Task:      call a task, execute a shell command (usually)
    object AppletKind extends Enumeration {
        val Eval, Scatter, Task = Value
    }

    /** @param name          Name of applet
      * @param input         list of WDL input arguments
      * @param output        list of WDL output arguments
      * @param instaceType   a platform instance name
      * @param docker        docker image name
      * @param destination   folder path on the platform
      * @param kind          Kind of applet: task, scatter, ...
      * @param wdlCode       WDL source code to run
      */
    case class Applet(name: String,
                      inputs: List[CVar],
                      outputs: List[CVar],
                      instanceType: String,
                      docker: Option[String],
                      destination : String,
                      kind: AppletKind,
                      wdlCode: String,
                      ast: Ast)

    /** An input to a stage. Could be empty, a wdl constant, or
      * a link to an output variable from another stage.
      */
    sealed trait SArg
    case object SArgEmpty extends SArg
    case class SArgLink(stageName: String, argName: String) extends SArg

    // Note: we figure out the outputs from a stage by looking up the
    // applet outputs.
    case class Stage(name: String,
                     appletName: String,
                     inputs: List[SArg])

    case class Workflow(name: String,
                        stages: List[Stage],
                        applets: List[Applet])

    // Human readable representation of the IR, with YAML
    def yaml(cVar: CVar) : YamlString = {
        YamlString(cVar.wdlType.toWdlString + " " + cVar.name)
    }

    def yaml(applet: Applet) : YamlObject = {
        val inputs = applet.inputs.map(yaml)
        val outputs = applet.outputs.map(yaml)
        val docker = applet.docker match {
            case None => "none"
            case Some(x) => x
        }
        YamlObject(
            YamlString("name") -> YamlString(applet.name),
            YamlString("inputs") -> YamlArray(inputs.toVector),
            YamlString("outputs") -> YamlArray(outputs.toVector),
            YamlString("instanceType") -> YamlString(applet.instanceType),
            YamlString("docker") -> YamlString(docker),
            YamlString("destination") -> YamlString(applet.destination),
            YamlString("kind") -> YamlString(applet.kind.toString),
            YamlString("wdlCode") -> YamlString(applet.wdlCode)
        )
    }

    def yaml(sArg: SArg) : YamlValue = {
        sArg match {
            case SArgEmpty => YamlString("empty")
            case SArgLink(stageName, argName) => YamlString(stageName + ":" + argName)
        }
    }

    def yaml(stage: Stage) : YamlObject = {
        val inputs = stage.inputs.map(yaml)
        YamlObject(
            YamlString("name") -> YamlString(stage.name),
            YamlString("appletName") -> YamlString(stage.appletName),
            YamlString("inputs") -> YamlArray(inputs.toVector)
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
}
