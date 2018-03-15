package dxWDL.compiler

// An in-memory representation of a WDL namespace, with direct and
// indirectly referenced namespaces.
//
import dxWDL.{CompilerErrorFormatter, Utils, Verbose, WdlPrettyPrinter}
import java.nio.file.{Path, Paths}
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

        // A debugging function, pretty prints the namespace
        // as one concatenated string.
        def prettyPrint : String

        // Apply a rewrite transformation to all the workflows in the
        // namespace.
        def transform(f: (WdlWorkflow, CompilerErrorFormatter) => WdlWorkflow) : Tree =
            this

        // Apply a rewrite transformation to all the workflows in the
        // namespace. The transformation returns a rewrite of the workflow,
        // and, potentially, a list of sub-workflows.
        def transform2(f: (WdlWorkflow, CompilerErrorFormatter) =>
            (WdlWorkflow, Vector[WdlWorkflow])) : Tree =
            this
    }


    private def makeResolver(allWdlSources: Map[String, String]) : ImportResolver = {
        filename => allWdlSources.get(filename) match {
            case None => throw new Exception(s"Unable to find ${filename}")
            case Some(content) => content
        }
    }


    // A namespace that is a library of tasks; it has no workflow
    case class TreeLeaf(name: String,
                        cef: CompilerErrorFormatter,
                        tasks: Map[String, WdlTask]) extends Tree {
        private def toNamespace =
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
            val ns = this.toNamespace
            val lines: Vector[String] = WdlPrettyPrinter(false, None).apply(ns, 0)
            val desc = s"### Namespace  ${name}"
            (desc +: lines).mkString("\n")
        }

        def genWdlSource : String = {
            val ns = this.toNamespace
            val lines: Vector[String] = WdlPrettyPrinter(false, None).apply(ns, 0)
            lines.mkString("\n")
        }

    }

    // A branch in the namespace tree, includes a workflow, and
    // may import other namespaces
    //
    case class TreeNode(name: String,
                        cef: CompilerErrorFormatter,
                        allWdlSources: Map[String, String],
                        imports: Seq[Import],
                        workflow: WdlWorkflow,
                        children: Vector[Tree]) extends Tree {
        private def toNamespace(wf: WdlWorkflow) =
            new WdlNamespaceWithWorkflow(
                None,
                wf,
                imports,
                Vector.empty[WdlNamespace],
                Vector.empty,   // tasks
                Map.empty[Terminal, WorkflowSource],
                WdlRewrite.INVALID_ERR_FORMATTER,
                WdlRewrite.INVALID_AST,
                "",
                Some(name)
            )

        def prettyPrint : String = {
            val ns = toNamespace(workflow)
            val lines = WdlPrettyPrinter(true, Some(workflow.outputs)).apply(ns, 0)
            val desc = s"### Namespace  ${name}"
            val top = (desc +: lines).mkString("\n")

            val childrenStrings:Vector[String] = children.map{ child =>
                child.prettyPrint
            }.toVector

            (top +: childrenStrings).mkString("\n\n")
        }


        // Rewrite the workflow. Because the rewrite leaves the semantic
        // tree invalid, we re-parse it.
        override def transform(f: (WdlWorkflow, CompilerErrorFormatter) => WdlWorkflow) : Tree = {
            // recurse into children
            val childrenTr = this.children.map(child => child.transform(f))

            val wfTr = f(workflow, cef)
            // Convert a namespace to string representation and apply WDL
            // parser again. This fixes the ASTs, as well as any other
            // imperfections in our WDL rewriting technology.
            val ns = toNamespace(wfTr)
            val wfOutputs: Vector[WorkflowOutput] =
                wfTr.children.filter(x => x.isInstanceOf[WorkflowOutput])
                    .map(_.asInstanceOf[WorkflowOutput])
                    .toVector

            val pp = WdlPrettyPrinter(true, Some(wfOutputs))
            val lines: String = pp.apply(ns, 0).mkString("\n")
            val resolver = makeResolver(allWdlSources)
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


    // Extract all the tasks from the workflow, assume they are placed
    // in library [libName]. Rewrite the calls appropriately.
    //
    // import "libName" as xxxx_lib
    //    call  foo    ->   call xxxx_lib.foo
    private def rewriteWorkflowExtractTasks(nswf: WdlNamespaceWithWorkflow,
                                            libPath: String,
                                            libName: String,
                                            taskNames: Set[String],
                                            wdlSources: Map[String, String])
            : WdlNamespaceWithWorkflow  = {
        val wfOutputs: Vector[WorkflowOutput] =
            nswf.workflow.children.filter(x => x.isInstanceOf[WorkflowOutput])
                .map(_.asInstanceOf[WorkflowOutput])
                .toVector

        // Modify the workflow: add an import, and rename task calls.
        val pp = WdlPrettyPrinter(true, Some(wfOutputs), Some((libPath, libName, taskNames)))
        val lines: String = pp.apply(nswf, 0).mkString("\n")
        val resolver = makeResolver(wdlSources)
        val cleanNs = WdlNamespace.loadUsingSource(
            lines, None, Some(List(resolver))
        ).get
        cleanNs.asInstanceOf[WdlNamespaceWithWorkflow]
    }


    //  /A/B/C/xxx.wdl -> xxx
    private def stripFileSuffix(src: Path) : String = {
        val fName = src.toFile().getName()
        val index = fName.lastIndexOf('.')
        if (index == -1)
            fName
        else
            fName.substring(0, index)
    }

    // Create a leaf node from a name and a bunch of tasks
    private def genLeaf(tasksLibName: String,
                        taskDict: Map[String, WdlTask]) : TreeLeaf = {
        val tasks = taskDict.map{ case (_, task) => task }.toVector
        val wf = new WdlNamespaceWithoutWorkflow(
            None,
            Vector.empty,
            Vector.empty,
            tasks,
            Map.empty,
            WdlRewrite.INVALID_AST,
            tasksLibName,
            None)
        val lines = WdlPrettyPrinter(true, None, None).apply(wf, 0).mkString("\n")
        val cleanNs = WdlNamespace.loadUsingSource(
            lines, None, None
        ).get
        val cef = new CompilerErrorFormatter(cleanNs.resource, cleanNs.terminalMap)
        TreeLeaf(tasksLibName, cef, taskDict)
    }

    def load(ns: WdlNamespace,
             allWdlSources: Map[String, String],
             verbose: Verbose) : Tree = {
        val name = ns.importUri match {
            case None => "Unknown namespace"
            case Some(x) => x
        }
        ns match {
            case _:WdlNamespaceWithoutWorkflow =>
                val cef = new CompilerErrorFormatter(ns.resource, ns.terminalMap)
                val taskDict = ns.tasks.map{ task => task.name -> task}.toMap
                new TreeLeaf(name, cef, taskDict)

            case nswf:WdlNamespaceWithWorkflow =>
                // recurse into sub-namespaces
                val children:Vector[Tree] = nswf.namespaces.map{
                    child => load(child, allWdlSources, verbose)
                }.toVector
                if (ns.tasks.isEmpty) {
                    val cef = new CompilerErrorFormatter(ns.resource, ns.terminalMap)
                    TreeNode(name,
                             cef,
                             allWdlSources,
                             nswf.imports,
                             nswf.workflow,
                             children)
                } else {
                    // The workflow has tasks, split into a separate node,
                    // and update the imports and file-list,
                    val taskDict = ns.tasks.map{ task => task.name -> task}.toMap
                    val tasksLibPath = stripFileSuffix(Paths.get(name)) + "_task_lib.wdl"
                    val tasksLibName = stripFileSuffix(Paths.get(tasksLibPath))
                    if (allWdlSources contains tasksLibPath)
                        throw new Exception(s"Module name collision, ${tasksLibPath} already exists")
                    Utils.trace(verbose.on, s"""|Splitting out tasks into separate source file
                                                |path=${tasksLibPath}  name=${tasksLibName}"""
                                    .stripMargin)

                    val child: TreeLeaf = genLeaf(tasksLibName, taskDict)
                    val wdlSourcesWithTaskLib = allWdlSources + (tasksLibPath -> child.genWdlSource)
                    val nswf2 = rewriteWorkflowExtractTasks(
                        nswf, tasksLibPath, tasksLibName,
                        taskDict.keys.toSet, wdlSourcesWithTaskLib)
                    val cef = new CompilerErrorFormatter(nswf2.resource, nswf2.terminalMap)
                    TreeNode(name,
                             cef,
                             wdlSourcesWithTaskLib,
                             nswf2.imports,
                             nswf2.workflow,
                             children :+ child)
                }
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
