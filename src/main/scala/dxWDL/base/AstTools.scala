package dxWDL.base

import wdlTools.syntax.{AbstractSyntax => AST}

// Tools that help use the abstract syntax
object AstTools {

  // A callable is a generalization of tasks and workflows. Objects
  // that can be called.
  final case class CallableInfo(name: String,
                                input: Vector[Declaration],
                                output: Vector[Declaration],
                                original : AST.Callable)

  def callableInfo(callable: Callable) = {
    callable match {
      case wf: Workflow =>
        val input: Vector[Declaration] = wf.input.map(_.declarations).getOrElse(Vector.empty)
        val output: Vector[Declaration] = wf.output.map(_.declarations).getOrElse(Vector.empty)
        CallableInfo(wf.name, input, output, wf)

      case task: Task =>
        val input: Vector[Declaration] = task.input.map(_.declarations).getOrElse(Vector.empty)
        val output: Vector[Declaration] = task.output.map(_.declarations).getOrElse(Vector.empty)
        CallableInfo(task.name, input, output, task)
    }
  }

  final case class WdlBundle(primaryCallable: Option[Callable],
                             allCallables: Map[String, Callable],
                             typeAliases: Map[String, WdlTypes.WT])
}
