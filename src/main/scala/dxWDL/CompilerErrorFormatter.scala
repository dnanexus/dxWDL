package dxWDL

import wdl4s.parser.WdlParser._
import wdl.AstTools
import wdl.{WdlExpression, WdlCall, WorkflowOutput}
import wom.core._
import wom.types.WomType

case class CompilerErrorFormatter(resource: String,
                                  terminalMap: Map[Terminal, WorkflowSource]) {
    private def line(t:Terminal): String = {
        terminalMap.get(t) match {
            case None => throw new Exception(s"Could not find terminal ${t} in source file ${resource}")
            case Some(x) => x.split("\n")(t.getLine - 1)
        }
    }

    private def pointToSource(t: Terminal): String = {
        s"${line(t)}\n${" " * (t.getColumn - 1)}^"
    }

    private def textualSource(t: Terminal) : String = {
        val lineNum = t.getLine
        s"${resource}, line ${lineNum}"
    }

    def compilerInternalError(ast: Ast, featureName: String) : String = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|Should not reach this point in the code: ${featureName}
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    def couldNotEvaluateType(expr: WdlExpression) : String = {
        val t: Terminal = AstTools.findTerminals(expr.ast).head
                s"""|Could not evaluate the WDL type for expression
            |
            |${textualSource(t)}
            |${pointToSource(t)}
                    |""".stripMargin
    }

    def illegalCallName(call: WdlCall) : String = {
        val t: Terminal = AstTools.findTerminals(call.ast).head
        s"""|Illegal call name ${call.unqualifiedName}
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    def illegalVariableName(ast: Ast) : String = {
        val name: Terminal = ast.getAttribute("name").asInstanceOf[Terminal]
        s"""|Illegal variable name
            |
            |${textualSource(name)}
            |${pointToSource(name)}
            |""".stripMargin
    }

    def missingCallArgument(ast: Ast, msg:String) : String = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|Call is missing a compulsory argument.
            |${msg}
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    def missingVarRef(t: Terminal) : String = {
        s"""|Reference to missing variable
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    def missingVarRef(ast: Ast) : String = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|Reference to missing variable
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    def notCurrentlySupported(ast: Ast, featureName: String) : String = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|Not currently supported: ${featureName}
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    def onlyFilesCanBeStreamed(ast: Ast) : String = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|Only files can be streamed
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    def taskInputDefaultMustBeConst(expr: WdlExpression) = {
        val t: Terminal = AstTools.findTerminals(expr.ast).head
        s"""|Task input expression ${expr.toWomString} must be const or variable
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    // debugging
    def traceExpression(ast: Ast) : String = {
        val t: Terminal = AstTools.findTerminals(ast).head
        s"""|
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    def typeConversionRequired(expr: WdlExpression,
                               call: WdlCall,
                               srcType: WomType,
                               trgType: WomType) : String = {
        val termList: Seq[Terminal] = AstTools.findTerminals(expr.ast)
        val t:Terminal = termList match {
            case Nil => AstTools.findTerminals(call.ast).head
            case _ => AstTools.findTerminals(expr.ast).head
        }
        s"""|Warning: expression <${expr.toWomString}> is coerced from type ${srcType.toDisplayString}
            |to ${trgType.toDisplayString}.
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }


    def workflowInputDefaultMustBeConst(expr: WdlExpression) = {
        val t: Terminal = AstTools.findTerminals(expr.ast).head
        s"""|Workflow input expression ${expr.toWomString} must be const or variable
            |
            |${textualSource(t)}
            |${pointToSource(t)}
            |""".stripMargin
    }

    def workflowOutputIsPartial(wot: WorkflowOutput) = {
        s"""|Workflow output must have a name, type, and value
            |
            | ${resource}
            | ${wot.toWdlString}
            |""".stripMargin
    }
}
