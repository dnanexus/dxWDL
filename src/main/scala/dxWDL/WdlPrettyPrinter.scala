/**
  *  Print a WDL structure in internal representation, to a valid
  *  textual WDL string.
  */
package dxWDL

import wdl4s._
import wdl4s.command.{CommandPart, ParameterCommandPart, StringCommandPart}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}

object WdlPrettyPrinter {
    val I_STEP = 4


    // Create an indentation of [n] spaces
    private def genNSpaces(n: Int) = {
        s"${" " * n}"
    }

    // indent a line by [level] steps
    def indentLine(line: String, indentLevel: Int) = {
        val spaces = genNSpaces(indentLevel * I_STEP)
        spaces + line
    }

    // All blocks except for task command.
    //
    // The top and bottom lines are indented, the middle lines must
    // already have the correct indentation.
    def buildBlock(top: String,
                   middle: Vector[String],
                   level: Int) : Vector[String] = {
        if (middle.isEmpty) {
            Vector.empty
        } else {
            val firstLine = indentLine(s"${top} {", level)
            val endLine = indentLine("}", level)
            firstLine +: middle :+ endLine
        }
    }

    // The command block is special because spaces and tabs must be
    // faithfully preserved. There are shell commands that are
    // sensitive to white space and tabs.
    //
    def buildCommandBlock(commandTemplate: Seq[CommandPart], level: Int) : Vector[String] = {
        val commandLines: String = commandTemplate.map {part =>
            part match  {
                case x:ParameterCommandPart => x.toString()
                case x:StringCommandPart => x.toString()
            }
        }.mkString("")
        val firstLine = indentLine("command <<<", level)
        val endLine = indentLine(">>>", level)
        Vector(firstLine, commandLines, endLine)
    }

    def apply(call: Call, level: Int) : Vector[String] = {
        val name = call match {
            case x:TaskCall => x.task.name
            case x:WorkflowCall => x.calledWorkflow.unqualifiedName
        }
        val aliasStr = call.alias match {
            case None => ""
            case Some(nm) => " as " ++ nm
        }
        val inputs: Seq[String] = call.inputMappings.map { case (key, expr) =>
            val rhs = expr.ast match {
                case t: Terminal => t.getSourceString
                case a: Ast => WdlExpression.toString(a)
            }
            s"${key}=${rhs}"
        }.toList
        val inputsConcat = "input:  " + inputs.mkString(", ")

        buildBlock(s"call ${name} ${aliasStr}",
                   Vector(indentLine(inputsConcat, level+1)),
                   level)
    }

    def apply(decl: Declaration, level: Int) : Vector[String] = {
        val exprStr = decl.expression match {
            case None => ""
            case Some(x) => " = " ++ x.toWdlString
        }
        val ln = s"${decl.wdlType.toWdlString} ${decl.unqualifiedName} ${exprStr}"
        Vector(indentLine(ln, level))
    }

    def apply(ssc: Scatter, level: Int) : Vector[String] = {
        val top: String = s"scatter (${ssc.item} in ${ssc.collection.toWdlString})"
        val children = ssc.children.map{
            case x:Call => apply(x, level + 1)
            case x:Declaration => apply(x, level + 1)
            case x:Scatter => apply(x, level + 1)
            case _ => throw new Exception("Unimplemented scatter element")
        }.flatten.toVector
        buildBlock(top, children.toVector, level)
    }

    // transform the expressions in a scatter, and then pretty print
    def scatterRewrite(ssc: Scatter,
                       level: Int,
                       transform: WdlExpression => WdlExpression) : Vector[String] = {
        def transformChild(scope: Scope): Scope = {
            scope match {
                case x:TaskCall =>
                    val inputs = x.inputMappings.map{ case (k,expr) => (k, transform(expr)) }.toMap
                    TaskCall(x.alias, x.task, inputs, x.ast)
                case x:WorkflowCall =>
                    val inputs = x.inputMappings.map{ case (k,expr) => (k, transform(expr)) }.toMap
                    WorkflowCall(x.alias, x.calledWorkflow, inputs, x.ast)
                case x:Declaration =>
                    Declaration(x.wdlType, x.unqualifiedName,
                                x.expression.map(transform), x.parent, x.ast)
                case _ => throw new Exception("Unimplemented scatter element")
            }
        }
        val tChildren = ssc.children.map(x => transformChild(x))

        val top: String = s"scatter (${ssc.item} in ${ssc.collection.toWdlString})"
        val children = tChildren.map{
            case x:Call => apply(x, level + 1)
            case x:Declaration => apply(x, level + 1)
            case x:Scatter => apply(x, level + 1)
            case _ => throw new Exception("Unimplemented scatter element")
        }.flatten.toVector
        buildBlock(top, children.toVector, level)
    }

    def apply(tso: TaskOutput, level: Int): Vector[String] = {
        val ln = s"${tso.wdlType.toWdlString} ${tso.unqualifiedName} = ${tso.requiredExpression.toWdlString}"
        Vector(indentLine(ln, level))
    }

    // We need to be careful here to preserve white spaces in the command
    // section.
    //
    // TODO: support meta and parameterMeta
    def apply(task: Task, level:Int) : Vector[String] = {
        val decls = task.declarations.map(x => apply(x, level + 1)).flatten.toVector
        val runtime = task.runtimeAttributes.attrs.map{ case (key, expr) =>
            indentLine(s"${key}: ${expr.toWdlString}", level + 2)
        }.toVector
        val outputs = task.outputs.map(x => apply(x, level + 2)).flatten.toVector

        val body = decls ++
            buildCommandBlock(task.commandTemplate, level + 1) ++
            buildBlock("runtime", runtime, level + 1) ++
            buildBlock("output", outputs, level + 1)

        buildBlock(s"task ${task.name}", body, level)
    }

    def apply(wfo: WorkflowOutput, level: Int) : Vector[String] = {
        val ln = s"${wfo.wdlType.toWdlString} ${wfo.unqualifiedName} = ${wfo.requiredExpression.toWdlString}"
        Vector(indentLine(ln, level))
    }

    def apply(wf: Workflow, level: Int) : Vector[String] = {
        val children = wf.children.map {
            case call: Call => apply(call, level + 1)
            case sc: Scatter => apply(sc, level + 1)
            case decl: Declaration => apply(decl, level + 1)
            case x => throw new Exception(s"Unimplemented workflow element ${x.toString}")
        }.flatten
        val outputs = wf.outputs.map(x => apply(x, level + 2)).flatten

        val lines = children.toVector ++
            buildBlock("outputs", outputs.toVector, level + 1)
        buildBlock( s"workflow ${wf.unqualifiedName}", lines, level)
    }
}
