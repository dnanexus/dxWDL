package dxWDL.compiler

import dxWDL.{Utils, Verbose, WdlPrettyPrinter}
import java.nio.file.{Path, Paths}
import java.io.{FileWriter, PrintWriter}
import wdl._
import wom.core.WorkflowSource

// Convert a namespace to string representation and apply WDL
// parser again. This fixes the ASTs, as well as any other
// imperfections in our WDL rewriting technology.
//
// Note: by keeping the namespace in memory, instead of writing to
// a temporary file on disk, we keep the resolver valid.
case class WhitewashNamespace(wdlSourceFile: Path,
                              verbose: Verbose)  {
    // Resolving imports. Look for referenced files in the
    // source directory.
    private def resolver(filename: String) : WorkflowSource = {
        var sourceDir:Path = wdlSourceFile.getParent()
        if (sourceDir == null) {
            // source file has no parent directory, use the
            // current directory instead
            sourceDir = Paths.get(System.getProperty("user.dir"))
        }
        val p:Path = sourceDir.resolve(filename)
        Utils.readFileContent(p)
    }

    // Add a suffix to a filename, before the regular suffix. For example:
    //  xxx.wdl -> xxx.sorted.wdl
    private def addFilenameSuffix(src: Path, secondSuffix: String) : String = {
        val fName = src.toFile().getName()
        val index = fName.lastIndexOf('.')
        if (index == -1) {
            fName + secondSuffix
        } else {
            val prefix = fName.substring(0, index)
            val suffix = fName.substring(index)
            prefix + secondSuffix + suffix
        }
    }

    // Assuming the source file is xxx.wdl, the new name will
    // be xxx.SUFFIX.wdl.
    private def writeToFile(suffix: String, lines: String) : Unit = {
        val trgName: String = addFilenameSuffix(wdlSourceFile, suffix)
        val simpleWdl = Utils.appCompileDirPath.resolve(trgName).toFile
        val fos = new FileWriter(simpleWdl)
        val pw = new PrintWriter(fos)
        pw.println(lines)
        pw.flush()
        pw.close()
        Utils.trace(verbose.on, s"Wrote WDL to ${simpleWdl.toString}")
    }

    private def getWorkflowOutputs(ns: WdlNamespace,
                                   verbose: Verbose) : Option[Seq[WorkflowOutput]] = {
        ns match {
            case nswf: WdlNamespaceWithWorkflow =>
                val wf = nswf.workflow
                try {
                    Some(wf.outputs)
                } catch {
                    case e: Throwable if (wf.hasEmptyOutputSection) =>
                        throw new Exception(
                            """|The workflow has an empty output section, the default WDL option
                               |is to output everything, which is
                               |currently not supported. please explicitly specify the outputs.
                               |""".stripMargin.replaceAll("\n", " "))
                    case e: Throwable =>
                        throw e
                }
            case _ => None
        }
    }

    def apply(rewrittenNs: WdlNamespace,
              suffix: String) : WdlNamespace = {
        val wfOutputs = getWorkflowOutputs(rewrittenNs, verbose)
        val pp = WdlPrettyPrinter(true, wfOutputs)
        val lines: String = pp.apply(rewrittenNs, 0).mkString("\n")
        if (verbose.on)
            writeToFile("." + suffix, lines)
        val cleanNs = WdlNamespace.loadUsingSource(
            lines, None, Some(List(resolver))
        ).get
        cleanNs
    }
}
