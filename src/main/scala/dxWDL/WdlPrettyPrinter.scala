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
        if (line == "\n") {
            line
        } else {
            val spaces = genNSpaces(indentLevel * I_STEP)
            spaces + line
        }
    }

    // All blocks except for task command.
    //
    // The top and bottom lines are indented, the middle lines must
    // already have the correct indentation.
    def buildBlock(top: String,
                   middle: Vector[String],
                   level: Int,
                   force: Boolean = false) : Vector[String] = {
        if (force || !middle.isEmpty) {
            val firstLine = indentLine(s"${top} {", level)
            val endLine = indentLine("}", level)
            firstLine +: middle :+ endLine
        } else {
            Vector.empty
        }
    }

    // The command block is special because spaces and tabs must be
    // faithfully preserved. There are shell commands that are
    // sensitive to white space and tabs.
    //
    def buildCommandBlock(commandTemplate: Seq[CommandPart], level: Int) : Vector[String] = {
        val command: String = commandTemplate.map {part =>
            part match  {
                case x:ParameterCommandPart => x.toString()
                case x:StringCommandPart => x.toString()
            }
        }.mkString("")

        // remove empty lines; we are not sure how they are generated, but they mess
        // up pretty printing downstream.
        val nonEmptyLines: Vector[String] = command.split("\n").filter(l => !l.trim().isEmpty).toVector

        // special syntex for short commands
        val (bgnSym,endSym) =
            if (nonEmptyLines.size <= 1) ("{", "}")
            else ("<<<", ">>>")

        val firstLine = indentLine(s"command ${bgnSym}", level)
        val endLine = indentLine(endSym, level)
        firstLine +: nonEmptyLines :+ endLine
    }

    def apply(call: TaskCall, level: Int) : Vector[String] = {
        val aliasStr = call.alias match {
            case None => ""
            case Some(nm) => " as " ++ nm
        }
        val inputs: Seq[String] = call.inputMappings.map { case (key, expr) =>
            val rhs = WdlExpression.toString(expr.ast)
            s"${key}=${rhs}"
        }.toList
        val inputsVec: Vector[String] =
            if (inputs.isEmpty) {
                Vector.empty
            } else {
                val line = "input:  " + inputs.mkString(", ")
                Vector(indentLine(line, level+1))
            }

        buildBlock(s"call ${call.task.fullyQualifiedName} ${aliasStr}", inputsVec, level, true)
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
            case x:TaskCall => apply(x, level + 1)
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
                case x:Declaration =>
                    Declaration(x.wdlType, x.unqualifiedName,
                                x.expression.map(transform), x.parent, x.ast)
                case _ => throw new Exception("Unimplemented scatter element")
            }
        }
        val tChildren = ssc.children.map(x => transformChild(x))

        val top: String = s"scatter (${ssc.item} in ${transform(ssc.collection).toWdlString})"
        val children = tChildren.map{
            case x:TaskCall => apply(x, level + 1)
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

        buildBlock(s"task ${task.fullyQualifiedName}", body, level)
    }

    def apply(wfo: WorkflowOutput, level: Int) : Vector[String] = {
        val ln = s"${wfo.wdlType.toWdlString} ${wfo.unqualifiedName} = ${wfo.requiredExpression.toWdlString}"
        Vector(indentLine(ln, level))
    }

    // TODO
    // Handle:
    //   meta: Map[String, String]
    //   parameterMeta: Map[String, String]
    //
    def apply(wf: Workflow, level: Int) : Vector[String] = {
        val children = wf.children.map {
            case call: TaskCall => apply(call, level + 1)
            case sc: Scatter => apply(sc, level + 1)
            case decl: Declaration => apply(decl, level + 1)
            case x => throw new Exception(s"Unimplemented workflow element ${x.toString}")
        }.flatten
        val outputs = wf.outputs.map(x => apply(x, level + 2)).flatten

        val lines = children.toVector ++
            buildBlock("outputs", outputs.toVector, level + 1)
        buildBlock( s"workflow ${wf.fullyQualifiedName}", lines, level)
    }

    def apply(ns: WdlNamespace, level: Int) : Vector[String] = {
        // print the imports
        val importLines: Vector[String] = ns.imports.map{
            imp => s"import ${imp.uri} as ${imp.namespaceName}"
        }.toVector

        // tasks
        val taskLines: Vector[String] = ns.tasks.map(
            task => apply(task, level) :+ "\n"
        ).toVector.flatten

        // workflow, if it exists
        val wfLines: Vector[String] = ns match {
            case nswf : WdlNamespaceWithWorkflow => apply(nswf.workflow, level)
            case _ => Vector()
        }

        val allLines = importLines ++ Vector("\n") ++
            taskLines ++ Vector("\n") ++ wfLines
        allLines.map(x => indentLine(x, level))
    }
}
