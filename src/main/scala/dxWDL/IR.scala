// Intermediate Representation (IR)
//
// Representation the compiler front end generates from a WDL
// workflow. The compiler back end uses it to generate a
// dx:workflow. A more detailed description can be found at
// ToplevelDir/[IntermediateForm.md].
package dxWDL

import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._

object IR {

    // Compile time representation of a variable. Used also as
    // an applet argument.
    // At compile time, we need to keep track of the syntax-tree, for error
    // reporting purposes.
    case class CVar(name: String, wdlType: WdlType, ast: Ast)

    /** @param name          Name of applet
      * @param input         list of platform input arguments
      * @param output        list of platform output arguments
      * @param instaceType   a platform instance name
      * @param docker        docker image name
      * @param destination   folder path on the platform
      * @param language      language in which the script is written, could be bash or WDL
      * @entrypoint          starting point of execution in the code. For WDL, this could be a scatter.
      *                      For bash, normally `main`.
      * @param code          bash or WDL snippet to exeute
      */
    case class Applet(name: String,
                      input: List[CVar],
                      output: List[CVar],
                      instanceType: String,
                      docker: Option[String],
                      destination : String,
                      code: String,
                      ast: Ast)

    /** An input to a stage. Could be empty, a wdl constant, or
      * a link to an output variable from another stage.
      */
    sealed trait SArg
    case object SArgEmpty extends SArg
    case class SArgConst(cVal: WdlValue) extends SArg
    case class SArgLink(stageName: String, argName: String) extends SArg

    // Linking between a variable, and which stage we got
    // it from.
    case class LinkedVar(cVar: CVar, sArg: SArg)

    // Note: we figure out the outputs from a stage by looking up the
    // applet outputs.
    case class Stage(name: String,
                     appletName: String,
                     inputs: List[LinkedVar])

    case class Workflow(name: String,
                        stages: List[Stage],
                        applets: List[Applet])
}
