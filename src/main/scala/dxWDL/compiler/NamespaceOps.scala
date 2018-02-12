package dxWDL.compiler

import dxWDL.{Utils, Verbose, WdlPrettyPrinter}
import java.nio.file.{Path, Paths}
import java.io.{FileWriter, PrintWriter}
import wdl._
import wdl4s.parser.WdlParser.{Terminal}
import wom.core.WorkflowSource

object NamespaceOps {

    sealed trait Tree {
        // The URI from which this namespace was loaded. Typically,
        // a filesystem path.
        def importUri : Option[String]

        // A debugging function, pretty prints the namespace
        // as one concatenated string.
        def prettyPrint : String

        // Apply a rewrite transformation to all the workflows in the
        // namespace
        def transform(f: WdlWorkflow => WdlWorkflow) : Tree

        // Convert a namespace to string representation and apply WDL
        // parser again. This fixes the ASTs, as well as any other
        // imperfections in our WDL rewriting technology.
        //
        def whitewash : WdlNamespace
    }

    // A namespace that is a library of tasks; it has no workflow
    case class TreeLeaf(importUri: Option[String],
                        tasks: Map[String, WdlTask]) {
        private def toNamespace() =
            new WdlNamespaceWithoutWorkflow(
                None,
                Seq.empty,
                Vector.empty[WdlNamespace],
                tasks.map{ case(_,task) => task }.toVector,
                Map.empty[Terminal, WorkflowSource],
                WdlRewrite.INVALID_AST,
                "",
                importUri)

        def prettyPrint : String = {
            val ns = toNamespace()
            WdlPrettyPrinter(false, None).apply(ns, 0).mkString("\n")
        }

        // There is nothing to do here, there is no workflow
        def transform(f: WdlWorkflow => WdlWorkflow) : Tree = {
            this.asInstanceOf[Tree]
        }

        def whitewash : WdlNamespace = {
            val pp = WdlPrettyPrinter(false, None)
            val ns = toNamespace()
            val lines: String = pp.apply(ns, 0).mkString("\n")
            val cleanNs = WdlNamespace.loadUsingSource(
                lines, None, None
            ).get
            cleanNs
        }
    }

    // A branch in the namespace tree, includes a workflow, and
    // may import other namespaces
    //
    case class TreeNode(importUri: Option[String],
                        imports: Seq[Import],
                        wdlSourceFile: Path,
                        workflow: WdlWorkflow,
                        tasks: Map[String, WdlTask],
                        children: Vector[Tree]) {
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

        private def toNamespace() =
            new WdlNamespaceWithWorkflow(
                None,
                workflow,
                imports,
                Vector.empty[WdlNamespace],
                tasks.map{ case(_,task) => task }.toVector,
                Map.empty[Terminal, WorkflowSource],
                WdlRewrite.INVALID_ERR_FORMATTER,
                WdlRewrite.INVALID_AST,
                "",
                importUri
            )

        def prettyPrint : String = {
            val ns = toNamespace()
            val pp:WdlPrettyPrinter = WdlPrettyPrinter(true, Some(workflow.outputs))
            val topNs = importUri + "\n" + pp.apply(ns, 0)

            val childrenStrings = children.map{ child =>
                child.importUri + "\n" + child.prettyPrint
            }.toVector

            (topNs +: childrenStrings).mkString("\n\n")
        }

        def transform(f: WdlWorkflow => WdlWorkflow) : Tree = {
            val tn = TreeNode(importUri, imports, wdlSourceFile, f(workflow), tasks, children)
            tn.asInstanceOf[Tree]
        }

        def whitewash : WdlNamespace = {
            val ns = toNamespace()
            val pp = WdlPrettyPrinter(true, Some(workflow.outputs))
            val lines: String = pp.apply(ns, 0).mkString("\n")
            val cleanNs = WdlNamespace.loadUsingSource(
                lines, None, Some(List(resolver))
            ).get
            cleanNs
        }
    }

    def load(ns: WdlNamespace,
             wdlSourceFile: Path) : Tree = {
        ns match {
            case _:WdlNamespaceWithoutWorkflow =>
                val taskDict = ns.tasks.map{ task => task.name -> task}.toMap
                val leaf = new TreeLeaf(ns.importUri, taskDict)
                leaf.asInstanceOf[Tree]

            case nswf:WdlNamespaceWithWorkflow =>
                // recurse into sub-namespaces
                val children:Vector[Tree] = nswf.namespaces.map{
                    child => load(child, wdlSourceFile)
                }.toVector
                val taskDict = nswf.tasks.map{ task => task.name -> task}.toMap
                val node = new TreeNode(nswf.importUri,
                                        nswf.imports,
                                        wdlSourceFile,
                                        nswf.workflow,
                                        taskDict,
                                        children)
                node.asInstanceOf[Tree]
        }
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
    private def writeToFile(wdlSourceFile: Path,
                            suffix: String,
                            lines: String,
                            verbose: Verbose) : Unit = {
        val trgName: String = addFilenameSuffix(wdlSourceFile, suffix)
        val simpleWdl = Utils.appCompileDirPath.resolve(trgName).toFile
        val fos = new FileWriter(simpleWdl)
        val pw = new PrintWriter(fos)
        pw.println(lines)
        pw.flush()
        pw.close()
        Utils.trace(verbose.on, s"Wrote WDL to ${simpleWdl.toString}")
    }

    // convenience method, for printing an entire namespace into a file.
    def prettyPrint(wdlSourceFile: Path,
                    tree: Tree,
                    suffix: String,
                    verbose: Verbose) : Unit = {
        val buf: String = tree.prettyPrint
        writeToFile(wdlSourceFile,
                    "." + suffix,
                    buf,
                    verbose)
    }
}
