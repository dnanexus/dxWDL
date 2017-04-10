package dxWDL

import wdl4s.WdlSource
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wdl4s.{Call, Declaration, Scatter, Scope, Task, TaskOutput, WdlExpression,
    WdlNamespace, WdlNamespaceWithWorkflow, WdlSource, Workflow}

case class CompilerErrorFormatter(terminalMap: Map[Terminal, WdlSource]) {
    private def pointToSource(t: Terminal): String = s"${line(t)}\n${" " * (t.getColumn - 1)}^"
    private def line(t:Terminal): String = terminalMap.get(t).get.split("\n")(t.getLine - 1)

    def missingVarRefException(t: Terminal) : String = {
        s"""|Reference to missing variable
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def missingVarRefException(ast: Ast) : String = {
        val name: Terminal = ast.getAttribute("name").asInstanceOf[Terminal]
        s"""|Reference to missing variable
            |
            |${pointToSource(name)}
            |""".stripMargin
    }

    def notCurrentlySupported(ast: Ast, featureName: String) : String = {
        val name: Terminal = ast.getAttribute("name").asInstanceOf[Terminal]
        s"""|Not currently supported: ${featureName}
            |
            |${pointToSource(name)}
            |""".stripMargin
    }

    def illegalCallName(call: Call) : String = {
        val name: Terminal = call.ast.getAttribute("name").asInstanceOf[Terminal]
        s"""|Illegal call name
            |
            |${pointToSource(name)}
            |""".stripMargin
    }

    def rightSideMustBeIdentifer(ast: Ast) : String = {
        val t: Terminal = ast.getAttribute("name").asInstanceOf[Terminal]
        s"""|Right-hand side of expression must be an identifier
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def expressionMustBeConstOrVar(ast: Ast) : String = {
        val t: Terminal = ast.getAttribute("name").asInstanceOf[Terminal]
        s"""|Expression must be const or variable
            |
            |${pointToSource(t)}
            |""".stripMargin
    }
}
