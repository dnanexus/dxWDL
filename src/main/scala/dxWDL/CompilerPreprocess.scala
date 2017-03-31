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
import wdl4s.{WdlSource, WdlNamespace, WdlNamespaceWithWorkflow, Workflow}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._

object CompilerPreprocess {
    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(terminalMap: Map[Terminal, WdlSource],
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

    // Simplify the original WDL file
    def simplify(ns: WdlNamespace, pw: PrintWriter) = {
        throw new Exception("TODO")
    }

    def apply(wdlSourceFile : Path,
              verbose: Boolean) : Path = {
        Utils.trace(verbose, "Preprocessing")

        val ns = WdlNamespaceWithWorkflow.load(
            Utils.readFileContent(wdlSourceFile),
            Seq.empty).get
        val tm = ns.wdlSyntaxErrorFormatter.terminalMap
        val cState = State(tm, verbose)

        // Create a new file to hold the result.
        //
        // Assuming the source file is xxx.wdl, the new name will
        // be xxx.simplified.wdl.
        val trgName: String = createTargetFileName(wdlSourceFile)
        val simplWdl = Utils.appCompileDirPath.resolve(trgName).toFile
        val fos = new FileWriter(simplWdl)
        val pw = new PrintWriter(fos)

        // Process the original WDL file, write the output to
        // xxx.simplified.wdl
        simplify(ns, pw)

        pw.flush()
        pw.close()
        simplWdl.toPath
    }
}
