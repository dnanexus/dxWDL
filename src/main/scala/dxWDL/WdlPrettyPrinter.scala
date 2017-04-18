/**
  *  Print a WDL structure in internal representation, to a valid
  *  textual WDL string.
  */
package dxWDL

import wdl4s.{Call, Declaration, Scatter, Scope,
    Task, TaskCall, TaskOutput,
    WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow,
    Workflow, WorkflowCall, WdlSource}
import wdl4s.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}

object WdlPrettyPrinter {
    val I_STEP = 4


    // Create an indentation of [n] spaces
    private def genNSpaces(n: Int) = {
        s"${" " * n}"
    }

    def buildBlock(top: String,
                   middle: Vector[String],
                   indent: Int) : Vector[String] = {
        if (middle.isEmpty) {
            Vector.empty
        } else {
            val spaces = genNSpaces(indent + I_STEP)
            val (firstLine, endLine) =
                if (top == "command" && middle.length > 1) {
                    // Special symbols for a multi-line shell command
                    (top + " <<<", ">>>")
                } else {
                    (top + " {", "}")
                }
            firstLine +: (middle.map(x => spaces + x)).toVector :+ endLine
        }
    }


    def apply(call: Call, indent: Int) : Vector[String] = {
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
        val inputsConcat = "  " + inputs.mkString(", ")

        buildBlock(s"call ${name} ${aliasStr}", Vector("input:", inputsConcat), indent)
    }

    def apply(decl: Declaration, indent: Int) : Vector[String] = {
        val exprStr = decl.expression match {
            case None => ""
            case Some(x) => " = " ++ x.toWdlString
        }
        val line = s"""|${genNSpaces(indent)}
                       |${decl.wdlType.toWdlString} ${decl.unqualifiedName}
                       |${exprStr}""".stripMargin.replaceAll("\n","")
        Vector(line)
    }

    def apply(ssc: Scatter, indent: Int) : Vector[String] = {
        val top: String = s"scatter (${ssc.item} in ${ssc.collection.toWdlString})"
        val children = ssc.children.map{
            case x:Call => apply(x, indent)
            case x:Declaration => apply(x, indent)
            case x:Scatter => apply(x, indent)
            case _ => throw new Exception("Unimplemented scatter element")
        }.flatten.toVector
        buildBlock(top, children.toVector, indent)
    }

    // transform the expressions in a scatter, and then pretty print
    def scatterRewrite(ssc: Scatter,
                       indent: Int,
                       transform: WdlExpression => WdlExpression) : Vector[String] = {
        def transformChild(scope: Scope): Scope = {
            case x:TaskCall =>
                val inputs = x.inputMappings.map((k,expr) => (k, transform(expr))).toMap
                TaskCall(x.alias, x.task, inputs, x.ast)
            case x:WorkflowCall =>
                val inputs = x.inputMappings.map((k,expr) => (k, transform(expr))).toMap
                WorkflowCall(x.alias, x.task, inputs, x.ast)
            case x:Declaration =>
                x.expression match {
                    case None => x
                    case Some(expr) => Declaration(x.wdlType, x.unqualifiedName,
                                                   transform(expr), x.parent, x.ast)
                }
            case x:Scatter => throw new Exception("Unimplemented nested scatter renaming")
            case _ => throw new Exception("Unimplemented scatter element")
        }
        val tChildren = ssc.children.map(x => transformChild(x))

        val top: String = s"scatter (${ssc.item} in ${ssc.collection.toWdlString})"
        val children = tChildren.map{
            case x:Call => apply(x, indent)
            case x:Declaration => apply(x, indent)
            case x:Scatter => apply(x, indent)
            case _ => throw new Exception("Unimplemented scatter element")
        }.flatten.toVector
        buildBlock(top, children.toVector, indent)
    }

    def apply(tso: TaskOutput, indent: Int): Vector[String] = {
        val spaces = genNSpaces(indent)
        val line = s"${spaces}${tso.wdlType.toWdlString} ${tso.unqualifiedName} = ${tso.requiredExpression.toWdlString}"
        Vector(line)
    }

    // TODO: support meta and parameterMeta
    def apply(task: Task, indent:Int) : Vector[String] = {
        val decls = task.declarations.map(x => apply(x, indent)).flatten.toVector

        // This section must remain exactly the same as in the original source. There
        // are shell commands that are sensitive to white space and tabs.
        val command = task.commandTemplate.map{
            case x: ParameterCommandPart => x.toString()
            case x: StringCommandPart => x.toString()
        }.toVector
        val runtime = task.runtimeAttributes.attrs.map{ case (key, expr) =>
            s"${key}: ${expr.toWdlString}"
        }.toVector
        val outputs = task.outputs.map(x => apply(x, indent)).flatten.toVector

        val taskBody = decls ++
            (buildBlock("command", command, indent)) ++
            (buildBlock("runtime", runtime, indent)) ++
            (buildBlock("output", outputs, indent))

        buildBlock( s"task ${task.name}", taskBody, indent)
    }

    def apply(wf: Workflow, indent: Int) : Vector[String] = {
        val decls = task.declarations.map(x => apply(x, indent)).flatten
        val children = wf.children.map {
            case call: Call => apply(call, indent)
            case sc: Scatter => apply(sc, indent)
            case decl: Declaration => apply(decl, indent)
            case x => throw new Exception(s"Unimplemented workflow element ${x.toString}")
        }.flatten
        val outputs = task.outputs.map(x => apply(x, indent)).flatten

        val lines = decls.toVector ++
            children.toVector ++
            buildBlock("outputs", outputs.toVector, indent)
        buildBlock( s"workflow ${wf.unqualifiedName}", lines, indent)
    }
}
