package dxWDL.base

import wdlTools.syntax.{AbstractSyntax => AST}
import wdlTools.types.WdlTypes

// Tools that help use the abstract syntax
object AstTools {

  // A callable is a generalization of tasks and workflows. Objects
  // that can be called.
  final case class CallableInfo(name: String,
                                input: Vector[AST.Declaration],
                                output: Vector[AST.Declaration],
                                original : AST.Callable)

  def callableInfo(callable: Callable) = {
    callable match {
      case wf: Workflow =>
        val input: Vector[AST.Declaration] = wf.input.map(_.declarations).getOrElse(Vector.empty)
        val output: Vector[AST.Declaration] = wf.output.map(_.declarations).getOrElse(Vector.empty)
        CallableInfo(wf.name, input, output, wf)

      case task: Task =>
        val input: Vector[AST.Declaration] = task.input.map(_.declarations).getOrElse(Vector.empty)
        val output: Vector[AST.Declaration] = task.output.map(_.declarations).getOrElse(Vector.empty)
        CallableInfo(task.name, input, output, task)
    }
  }

  final case class WdlBundle(primaryCallable: Option[AST.Callable],
                             allCallables: Map[String, AST.Callable],
                             structDefs: Map[String, WdlTypes.WT_Struct])
}
