/**
  *  Print a WDL class as a valid, human readable,
  *  textual string. The output is then palatable to
  *  the WDL parser. The printing process is configurable. For example:
  *  to print fully qualified names use:
  *
  *  val pp = new WdlPrettyPrinter(true)
  *  pp.apply(x)
  *
  * TODO: for an unknown reason, the pretty printer mangles workflow
  * outputs. The work around is to pass the original workflow outputs
  * unmodified. Fix this.
  */
package dxWDL

import wdl4s.wdl._
import wdl4s.wdl.command.{CommandPart, ParameterCommandPart, StringCommandPart}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}

case class WdlPrettyPrinter(fqnFlag: Boolean, workflowOutputs: Option[Seq[WorkflowOutput]]) {

    private val I_STEP = 4

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

    def apply(call: WdlTaskCall, level: Int) : Vector[String] = {
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
        val taskName =
            if (fqnFlag) call.task.fullyQualifiedName
            else call.task.name
        buildBlock(s"call ${taskName} ${aliasStr}", inputsVec, level, true)
    }

    def apply(decl: Declaration, level: Int) : Vector[String] = {
        val exprStr = decl.expression match {
            case None => ""
            case Some(x) => " = " ++ x.toWdlString
        }
        val ln = s"${decl.wdlType.toWdlString} ${decl.unqualifiedName} ${exprStr}"
        Vector(indentLine(ln, level))
    }

    def apply(cond: If, level: Int) : Vector[String] = {
        val top: String = s"if (${cond.condition.toWdlString})"
        val children = cond.children.map(x =>
            apply(x, level + 1)
        ).flatten.toVector
        buildBlock(top, children.toVector, level)
    }

    def apply(ssc: Scatter, level: Int) : Vector[String] = {
        val top: String = s"scatter (${ssc.item} in ${ssc.collection.toWdlString})"
        val children = ssc.children.map(x =>
            apply(x, level + 1)
        ).flatten.toVector
        buildBlock(top, children.toVector, level)
    }

    def apply(scope: Scope, level: Int) : Vector[String] = {
        scope match {
            case x:WdlTaskCall => apply(x, level)
            case x:Declaration => apply(x, level)
            case x:Scatter => apply(x, level)
            case x:If => apply(x, level)
            case other =>
                throw new Exception(s"Unimplemented scope element ${other.getClass.getName}")
        }
    }

    def apply(tso: TaskOutput, level: Int): Vector[String] = {
        val ln = s"""|${tso.wdlType.toWdlString} ${tso.unqualifiedName} =
                     |${tso.requiredExpression.toWdlString}"""
            .stripMargin.replaceAll("\n", " ").trim
        Vector(indentLine(ln, level))
    }

    def apply(task: WdlTask, level:Int) : Vector[String] = {
        val decls = task.declarations.map(x => apply(x, level + 1)).flatten.toVector
        val runtime = task.runtimeAttributes.attrs.map{ case (key, expr) =>
            indentLine(s"${key}: ${expr.toWdlString}", level + 2)
        }.toVector
        val outputs = task.outputs.map(x => apply(x, level + 2)).flatten.toVector
        val paramMeta = task.parameterMeta.map{ case (x,y) =>
            indentLine(s"""${x}: "${y}" """, level + 2)
        }.toVector
        val meta = task.meta.map{ case (x,y) =>  s"${x}: ${y}" }.toVector

        val body = decls ++
            buildCommandBlock(task.commandTemplate, level + 1) ++
            buildBlock("runtime", runtime, level + 1) ++
            buildBlock("output", outputs, level + 1) ++
            buildBlock("parameter_meta", paramMeta, level + 1) ++
            buildBlock("meta", meta, level + 1)

        buildBlock(s"task ${task.unqualifiedName}", body, level)
    }

    /* There are several legal formats

    output {
        Array[String] keys = value
        bam_file
        Add.sum
     */
    def apply(wfo: WorkflowOutput, level: Int) : Vector[String] = {
        val ln =
            if (wfo.unqualifiedName == wfo.requiredExpression.toWdlString)
                wfo.unqualifiedName
            else
                wfo.toWdlString
        Vector(indentLine(ln, level))
    }

    def apply(wf: WdlWorkflow, level: Int) : Vector[String] = {
        // split the workflow outputs from the other children, they
        // are treated separately
        val (wfChildren, wfOutputs) = wf.children.partition {
            case _:WorkflowOutput => false
            case _ => true
        }
        val children = wfChildren.map {
            case call: WdlTaskCall => apply(call, level + 1)
            case sc: Scatter => apply(sc, level + 1)
            case decl: Declaration => apply(decl, level + 1)
            case cond: If  => apply(cond, level + 1)
            case x => throw new Exception(
                s"Unimplemented workflow element ${x.getClass.getName} ${x.toString}")
        }.flatten.toVector

        // An error occurs in wdl4s if we use the wf.outputs method,
        // where there are no outputs.  We use the explicit output
        // list instead.
        val wos: Seq[WorkflowOutput] = workflowOutputs match {
            case None => wfOutputs.map(x => x.asInstanceOf[WorkflowOutput])
            case Some(outputs) => outputs
        }
        val outputs = wos.map(apply(_, level + 2)).flatten.toVector
        val paramMeta = wf.parameterMeta.map{ case (x,y) =>  s"${x}: ${y}" }.toVector
        val meta = wf.meta.map{ case (x,y) =>  s"${x}: ${y}" }.toVector

        val lines = children ++
            buildBlock("output", outputs, level + 1, force=true) ++
            buildBlock("parameter_meta", paramMeta, level + 1) ++
            buildBlock("meta", meta, level + 1)
        val wfName =
            if (fqnFlag) wf.fullyQualifiedName
            else wf.unqualifiedName
        buildBlock( s"workflow ${wfName}", lines, level)
    }

    def apply(ns: WdlNamespace, level: Int) : Vector[String] = {
        // print the imports
        val importLines: Vector[String] = ns.imports.map{
            imp => s"""import "${imp.uri}" as ${imp.namespaceName}"""
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
