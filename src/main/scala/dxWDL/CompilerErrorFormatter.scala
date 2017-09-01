package dxWDL

import wdl4s.wdl.AstTools
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wdl4s.wdl._

case class CompilerErrorFormatter(terminalMap: Map[Terminal, WorkflowSource]) {
    private def pointToSource(t: Terminal): String = s"${line(t)}\n${" " * (t.getColumn - 1)}^"
    private def line(t:Terminal): String = terminalMap.get(t).get.split("\n")(t.getLine - 1)

    def cannotParseMemberAccess(ast: Ast) = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|Error parsing expression, which is supposed to be a member access
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def couldNotEvaluateType(ast: Ast) : String = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|Could not evaluate the WDL type for this expression
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def evaluatingTerminal(t: Terminal, x: String) = {
        s"""|Looking up string ${x}, while evaluating terminal
            |
            |${pointToSource(t)}
            |""".stripMargin

    }

    def expressionMustBeConstOrVar(expr: WdlExpression) : String = {
        val t: Terminal = AstTools.findTerminals(expr.ast).head
        s"""|Expression ${expr.toWdlString} must be const or variable
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def illegalCallName(call: WdlCall) : String = {
        val name: Terminal = call.ast.getAttribute("name").asInstanceOf[Terminal]
        s"""|Illegal call name
            |
            |${pointToSource(name)}
            |""".stripMargin
    }

    def illegalVariableName(ast: Ast) : String = {
        val name: Terminal = ast.getAttribute("name").asInstanceOf[Terminal]
        s"""|Illegal variable name
            |
            |${pointToSource(name)}
            |""".stripMargin
    }

    def missingVarRefException(t: Terminal) : String = {
        s"""|Reference to missing variable
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def missingScatterCollectionException(t: Terminal) : String = {
        s"""|Scatter collection variable missing
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def notCurrentlySupported(ast: Ast, featureName: String) : String = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|Not currently supported: ${featureName}
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def onlyFilesCanBeStreamed(ast: Ast) : String = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|Only files can be streamed
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def rightSideMustBeIdentifer(ast: Ast) : String = {
        val t: Terminal = ast.getAttribute("name").asInstanceOf[Terminal]
        s"""|Right-hand side of expression must be an identifier
            |
            |${pointToSource(t)}
            |""".stripMargin
    }

    def undefinedMemberAccess(ast: Ast): String = {
        val lhsAst = ast.getAttribute("lhs").asInstanceOf[Terminal]
        val fqn = WdlExpression.toString(ast)
        s"""|Undefined member access (${fqn})
            |
            |${pointToSource(lhsAst)}
            |""".stripMargin
    }

}
