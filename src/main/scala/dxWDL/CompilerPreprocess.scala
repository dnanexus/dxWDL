/**
  *  Preprocessing pass, simplifies the original WDL.
  *
  *  Instead of handling expressions in calls directly,
  *  we lift the expressions, generate auxiliary variables, and
  *  call the task with values or variables (no expressions).
  */
package dxWDL

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Path, Paths}
import wdl4s.AstTools
import wdl4s.{Task, WdlSource, WdlNamespace, WdlNamespaceWithWorkflow, Workflow}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._

object CompilerPreprocess {
    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(pw: PrintWriter,
                     terminalMap: Map[Terminal, WdlSource],
                     verbose: Boolean)

    // Add a suffix to a filename, before the regular suffix. For example:
    //  xxx.wdl -> xxx.simplified.wdl
    def createTargetFileName(src: Path) : String = {
        val secondSuffix = ".simplified"
        val fName = src.toFile().getName()
        val index = fName.lastIndexOf('.')
        if (index == -1) {
            fName + secondSuffix
        }
        else {
            val prefix = fName.substring(0, index)
            val suffix = fName.substring(index)
            prefix + secondSuffix + suffix
        }
    }

    def line(t:Terminal, cState: State): String =
        cState.terminalMap.get(t).get.split("\n")(t.getLine - 1)



    // Print a task, without modification, to the output
    def printTask(task: Task, cState: State) = {
        val name: Terminal = task.ast.getAttribute("name").asInstanceOf[Terminal]
        val ln = line(name, cState)
        cState.pw.println(ln)
        cState.pw.println("-------------------")

        val allTerminals : Seq[Terminal] = AstTools.findTerminals(task.ast)
        val buf = allTerminals.map(x => x.getSourceString()).mkString(" ")
        cState.pw.println(buf)
        cState.pw.println("-------------------")

        val firstLine = allTerminals.head.getLine -1
        val lastLine = allTerminals.last.getLine

        val lines : Array[String] = cState.terminalMap.get(allTerminals.head).get.split("\n")
        val (_, taskPlus) = lines.splitAt(firstLine)
        val (middle,_) = taskPlus.splitAt(lastLine - firstLine)
        val buf2 = middle.mkString("\n")
        cState.pw.println(buf2)
    }

    def simplifyWorkflow(wf: WdlNamespaceWithWorkflow, cState: State) = {
    }

    // Simplify the original WDL file
    def simplify(ns: WdlNamespace, cState: State) = {
        // Do not modify the tasks
        ns.tasks.foreach(task => printTask(task, cState))

        ns match {
            case nswf : WdlNamespaceWithWorkflow => simplifyWorkflow(nswf, cState)
            case _ => ()
        }
    }

    def apply(wdlSourceFile : Path,
              verbose: Boolean) : Path = {
        Utils.trace(verbose, "Preprocessing")

        val ns = WdlNamespaceWithWorkflow.load(
            Utils.readFileContent(wdlSourceFile),
            Seq.empty).get
        val tm = ns.wdlSyntaxErrorFormatter.terminalMap

        // Create a new file to hold the result.
        //
        // Assuming the source file is xxx.wdl, the new name will
        // be xxx.simplified.wdl.
        val trgName: String = createTargetFileName(wdlSourceFile)
        val simplWdl = Utils.appCompileDirPath.resolve(trgName).toFile
        val fos = new FileWriter(simplWdl)
        val pw = new PrintWriter(fos)
        val cState = State(pw, tm, verbose)

        // Process the original WDL file, write the output to
        // xxx.simplified.wdl
        simplify(ns, cState)

        pw.flush()
        pw.close()
        //simplWdl.toPath
        wdlSourceFile
    }
}
