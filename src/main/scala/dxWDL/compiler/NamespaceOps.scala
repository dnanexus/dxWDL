package dxWDL.compiler

// An in-memory representation of a WDL namespace, with direct and
// indirectly referenced namespaces.
//
import dxWDL.{CompilerErrorFormatter, Utils, Verbose, WdlPrettyPrinter}
import java.nio.file.Path
import java.io.{FileWriter, PrintWriter}
import wdl._
import wdl4s.parser.WdlParser.{Terminal}
import wom.core.WorkflowSource

object NamespaceOps {

    sealed trait Tree {
        // The URI from which this namespace was loaded. Typically,
        // a filesystem path.
        def name : String

        def cef: CompilerErrorFormatter

        // This namespace may be imported, if so, what name is used?
        def importedAs: Option[String]

        def tasks: Map[String, WdlTask]

        // A debugging function, pretty prints the namespace
        // as one concatenated string.
        def prettyPrint : String

        // Apply a rewrite transformation to all the workflows in the
        // namespace
        def transform(f: (WdlWorkflow, CompilerErrorFormatter) => WdlWorkflow) : Tree
    }

    // A namespace that is a library of tasks; it has no workflow
    case class TreeLeaf(name: String,
                        importedAs: Option[String],
                        cef: CompilerErrorFormatter,
                        resolver: ImportResolver,
                        tasks: Map[String, WdlTask]) extends Tree {
        private def toNamespace() =
            new WdlNamespaceWithoutWorkflow(
                None,
                Seq.empty,
                Vector.empty[WdlNamespace],
                tasks.map{ case(_,task) => task }.toVector,
                Map.empty[Terminal, WorkflowSource],
                WdlRewrite.INVALID_AST,
                "",
                Some(name))

        def prettyPrint : String = {
            val ns = toNamespace()
            val lines: Vector[String] = WdlPrettyPrinter(false, None, Some(name)).apply(ns, 0)
            val desc = s"### Namespace  ${name}"
            (desc +: lines).mkString("\n")
        }

        // There is nothing to do here, there is no workflow
        def transform(f: (WdlWorkflow, CompilerErrorFormatter) => WdlWorkflow) : Tree = {
            this
        }
    }

    // A branch in the namespace tree, includes a workflow, and
    // may import other namespaces
    //
    case class TreeNode(name: String,
                        importedAs: Option[String],
                        cef: CompilerErrorFormatter,
                        resolver: ImportResolver,
                        imports: Seq[Import],
                        workflow: WdlWorkflow,
                        tasks: Map[String, WdlTask],
                        children: Vector[Tree]) extends Tree {
        private def toNamespace(wf: WdlWorkflow) =
            new WdlNamespaceWithWorkflow(
                None,
                wf,
                imports,
                Vector.empty[WdlNamespace],
                tasks.map{ case(_,task) => task }.toVector,
                Map.empty[Terminal, WorkflowSource],
                WdlRewrite.INVALID_ERR_FORMATTER,
                WdlRewrite.INVALID_AST,
                "",
                Some(name)
            )

        def prettyPrint : String = {
            val ns = toNamespace(workflow)
            val lines = WdlPrettyPrinter(true, Some(workflow.outputs), importedAs).apply(ns, 0)
            val desc = s"### Namespace  ${name}"
            val top = (desc +: lines).mkString("\n")

            val childrenStrings:Vector[String] = children.map{ child =>
                child.prettyPrint
            }.toVector

            (top +: childrenStrings).mkString("\n\n")
        }


        // Rewrite the workflow. Because the rewrite leaves the semantic
        // tree invalid, we re-parse it.
        def transform(f: (WdlWorkflow, CompilerErrorFormatter) => WdlWorkflow) : Tree = {
            // recurse into children
            val childrenTr = this.children.map(child => child.transform(f))

            val wfTr = f(workflow, cef)
            // Convert a namespace to string representation and apply WDL
            // parser again. This fixes the ASTs, as well as any other
            // imperfections in our WDL rewriting technology.
            val useFqn = imports.length > 0
            val ns = toNamespace(wfTr)
            val wfOutputs: Vector[WorkflowOutput] =
                wfTr.children.filter(x => x.isInstanceOf[WorkflowOutput])
                    .map(_.asInstanceOf[WorkflowOutput])
                    .toVector

            val pp = WdlPrettyPrinter(useFqn, Some(wfOutputs), importedAs)
            val lines: String = pp.apply(ns, 0).mkString("\n")
            val cleanNs = WdlNamespace.loadUsingSource(
                lines, None, Some(List(resolver))
            ).get
            val cleanWf = cleanNs match {
                case nswf: WdlNamespaceWithWorkflow => nswf.workflow
                case _ => throw new Exception("sanity")
            }
            val cleanCef = new CompilerErrorFormatter(cef.resource, cleanNs.terminalMap)
            this.copy(workflow = cleanWf,
                      cef = cleanCef,
                      children = childrenTr)
        }
    }

    def load(ns: WdlNamespace,
             resolver: ImportResolver) : Tree = {
        val name = ns.importUri match {
            case None => "Unknown namespace"
            case Some(x) => x
        }
        val taskDict = ns.tasks.map{ task => task.name -> task}.toMap
        val cef = new CompilerErrorFormatter(ns.resource, ns.terminalMap)
        ns match {
            case _:WdlNamespaceWithoutWorkflow =>
                new TreeLeaf(name, ns.importedAs, cef, resolver, taskDict)

            case nswf:WdlNamespaceWithWorkflow =>
                // recurse into sub-namespaces
                val children:Vector[Tree] = nswf.namespaces.map{
                    child => load(child, resolver)
                }.toVector
                TreeNode(name,
                         ns.importedAs,
                         cef,
                         resolver,
                         nswf.imports,
                         nswf.workflow,
                         taskDict,
                         children)
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
