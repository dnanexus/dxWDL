package dxWDL.compiler

// An in-memory representation of a WDL namespace, with direct and
// indirectly referenced namespaces.
//
import dxWDL.{CompilerErrorFormatter, Utils, Verbose, WdlPrettyPrinter}
import java.nio.file.{Path, Paths}
import java.io.{FileWriter, PrintWriter}
import scala.util.{Failure, Success}
import wdl.draft2.model._
import wdl.draft2.parser.WdlParser.{Terminal}
import wom.core.WorkflowSource
import wom.types._

object NamespaceOps {

    case class CleanWf(node: TreeNode,
                       name: String,
                       wdlSource: String)

    sealed trait Tree {
        // The URI from which this namespace was loaded. Typically,
        // a filesystem path.
        def name : String
        def cef: CompilerErrorFormatter

        // A debugging function, pretty prints the namespace
        // as one concatenated string.
        def prettyPrint : String
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
                name,
                Some(name))

        def prettyPrint : String = {
            val ns = this.toNamespace
            val lines: Vector[String] = WdlPrettyPrinter(false).apply(ns, 0)
            val desc = s"### Namespace  ${name}"
            (desc +: lines).mkString("\n")
        }

        def genWdlSource : String = {
            val ns = this.toNamespace
            val lines: Vector[String] = WdlPrettyPrinter(false).apply(ns, 0)
            lines.mkString("\n")
        }
    }

    // A branch in the namespace tree, includes a workflow, and
    // may import other namespaces.
    //
    // An artifical workflow is one that was generated by the decompose step.
    case class TreeNode(name: String,
                        cef: CompilerErrorFormatter,
                        imports: Seq[Import],
                        workflow: WdlWorkflow,
                        children: Vector[Tree],
                        kind: IR.WorkflowKind.Value,
                        fullyReduced: Boolean,
                        originalWorkflowName: String) extends Tree {

        private def calcUsedImports(wf: WdlWorkflow,
                                    allImports: Seq[Import],
                                    ctx: Context) : Vector[Import] = {
            val wfCalls = Block.findCalls(Vector(wf))
            val accessedNamespaces: Set[String] = wfCalls.map{
                case taskCall:WdlTaskCall =>
                    taskCall.task.namespace.unqualifiedName
                case wfc:WdlWorkflowCall =>
                    wfc.calledWorkflow.namespace.unqualifiedName
            }.toSet
            val accessed = allImports.filter{ imp =>
                accessedNamespaces contains imp.namespaceName
            }.toVector
            Utils.trace(ctx.verbose2,
                        s"calcUsedImports  ${allImports.size}  -> ${accessed.size}")
            accessed
        }

        private def reportError(ns: WdlNamespace) : Unit = {
            val lines = WdlPrettyPrinter(true).apply(ns, 0).mkString("\n")
            System.err.println(
                s"""|=== NamespaceOps ===
                    |Generated an invalid WDL namespace
                    |
                    |${lines}
                    |====================""".stripMargin)
        }

        private def validate(ns: WdlNamespace) : Unit = {
            ns match {
                case nswf:WdlNamespaceWithWorkflow =>
                    val wfOutputs: Vector[WorkflowOutput] =
                        workflow.children.filter(x => x.isInstanceOf[WorkflowOutput])
                            .map(_.asInstanceOf[WorkflowOutput])
                            .toVector

                    // make sure we don't generate double optional types like [Int??]
                    wfOutputs.foreach { wot =>
                        wot.womType match {
                            case WomOptionalType(WomOptionalType(_)) =>
                                reportError(ns)
                                throw new Exception(s"Bad type ${wot.womType.toDisplayString}")
                            case _ => ()
                        }
                    }
                case _ => ()
            }
        }

        // prune unused imports, and create a namespace from a workflow
        private def toNamespace(wf: WdlWorkflow) : WdlNamespaceWithWorkflow = {
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
        }

        // Convert a workflow to string representation and apply WDL
        // parser again. This fixes the ASTs, as well as any other
        // imperfections in our WDL rewriting technology.
        //
        // extraImports: additional WDL libraries to import
        // wdlSources:   all the WDL source files, so we can parse the
        //               rewritten source workflow code.
        private def whiteWashWorkflow(wf: WdlWorkflow,
                                      extraImports: Vector[String],
                                      ctx: Context,
                                      blockKind: IR.WorkflowKind.Value) : CleanWf = {
            val ns = toNamespace(wf)
            val extraImportsText = extraImports.map{ libName =>
                s"""import "${libName}.wdl" as ${libName}"""
            }
            val lines = WdlPrettyPrinter(true).apply(ns, 0)
            val cleanWdlSrc = (extraImportsText ++ lines).mkString("\n")
            ctx.addWdlSourceFile(wf.unqualifiedName + ".wdl", cleanWdlSrc)
            val resolver = ctx.makeResolver

            Utils.trace(ctx.verbose2, s"loadUsingSource[")
            val cleanNs =
                WdlNamespace.loadUsingSource(cleanWdlSrc, None, Some(List(resolver))) match {
                    case Success(x) => x
                    case Failure(f) =>
                        reportError(ns)
                        throw f
                }
            Utils.trace(ctx.verbose2, s"]")
            validate(ns)

            // Clean up the new workflow. Remove unused imports.
            val cleanCef = new CompilerErrorFormatter(cef.resource, cleanNs.terminalMap)
            val cleanWf = cleanNs.asInstanceOf[WdlNamespaceWithWorkflow].workflow
            val usedImports = calcUsedImports(cleanWf, cleanNs.imports, ctx)
            val node = TreeNode(wf.unqualifiedName,
                                cleanCef,
                                usedImports,
                                cleanWf,
                                Vector.empty, // children
                                blockKind,
                                false,
                                originalWorkflowName)
            CleanWf(node, wf.unqualifiedName, cleanWdlSrc)
        }


        def prettyPrint : String = {
            val ns = toNamespace(workflow)
            val lines = WdlPrettyPrinter(true).apply(ns, 0)
            val desc = s"### Namespace  ${name}"
            val top = (desc +: lines).mkString("\n")

            val childrenStrings:Vector[String] = children.map{ child =>
                child.prettyPrint
            }.toVector

            (top +: childrenStrings).mkString("\n\n")
        }

        // The workflow was split into a toplevel workflow, and a
        // sub-workflow. Because the rewrite leaves the semantic tree
        // invalid, we re-parse it.
        //
        // The workflow was broken down into a subworkflow, called from
        // a toplevel workflow. The sub-workflow needs to have access to
        // all the original imports.
        def cleanAfterRewrite( topwf: WdlWorkflow,
                               subWf: WdlWorkflow,
                               ctx: Context,
                               kind: IR.WorkflowKind.Value) : TreeNode = {
            val CleanWf(subWf2, subWfName2, subWdlSrc2) =
                whiteWashWorkflow(subWf, Vector.empty, ctx, IR.WorkflowKind.Sub)

            // white wash the top level workflow
            ctx.addWdlSourceFile(subWfName2 + ".wdl", subWdlSrc2)
            val CleanWf(topwf2, _, _) = whiteWashWorkflow(topwf, Vector(subWfName2), ctx, kind)
            topwf2.copy(children = this.children :+ subWf2)
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
                                            resource: String,
                                            ctx: Context)
            : (WdlNamespaceWithWorkflow, CompilerErrorFormatter)  = {
        // Modify the workflow: add an import, and rename task calls.
        val pp = WdlPrettyPrinter(true, Some((libPath, libName, taskNames)))
        val lines: String = pp.apply(nswf, 0).mkString("\n")
        val resolver = ctx.makeResolver
        val cleanNs = WdlNamespace.loadUsingSource(
            lines, None, Some(List(resolver))
        ).get
        val nswf2 = cleanNs.asInstanceOf[WdlNamespaceWithWorkflow]
        val cef = new CompilerErrorFormatter(resource, nswf2.terminalMap)
        (nswf2, cef)
    }


    // Create a leaf node from a name and a bunch of tasks
    private def genLeaf(tasksLibName: String,
                        resource: String,
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
        val lines = WdlPrettyPrinter(true, None).apply(wf, 0).mkString("\n")
        val cleanNs = WdlNamespace.loadUsingSource(
            lines, None, None
        ).get
        val cef = new CompilerErrorFormatter(resource, cleanNs.terminalMap)
        TreeLeaf(tasksLibName, cef, taskDict)
    }

    // Use default runtime attributes, where they are unset in the task
    private def setDefaultAttributes(task: WdlTask ,
                                     defaultRuntimeAttributes: Map[String, WdlExpression]) : WdlTask = {
        if (defaultRuntimeAttributes.isEmpty)
            return task
        val existingAttrNames =  task.runtimeAttributes.attrs.keys.toSet
        val defaultAttrs: Map[String, WdlExpression] = defaultRuntimeAttributes
            .filter{ case (name, _) => !(existingAttrNames contains name) }
        val attrs: Map[String, WdlExpression] = task.runtimeAttributes.attrs ++ defaultAttrs
        WdlRewrite.taskReplaceRuntimeAttrs(task, WdlRuntimeAttributes(attrs))
    }

    def load(ns: WdlNamespace,
             ctx: Context,
             defaultRuntimeAttributes: Map[String, WdlExpression]) : Tree = {
        ns match {
            case _:WdlNamespaceWithoutWorkflow =>
                val cef = new CompilerErrorFormatter(ns.resource, ns.terminalMap)
                val taskDict = ns.tasks.map{ task =>
                    task.name -> setDefaultAttributes(task, defaultRuntimeAttributes)
                }.toMap
                val name = ns.importUri match {
                    case None => "Unknown"
                    case Some(x) =>
                        val p = Paths.get(x)
                        p.getFileName().toString
                }
                new TreeLeaf(name, cef, taskDict)

            case nswf:WdlNamespaceWithWorkflow =>
                val cef = new CompilerErrorFormatter(nswf.resource, nswf.terminalMap)

                // recurse into sub-namespaces
                val children:Vector[Tree] = nswf.namespaces.map{
                    child => load(child, ctx, defaultRuntimeAttributes)
                }.toVector
                if (ns.tasks.isEmpty) {
                    TreeNode(nswf.workflow.unqualifiedName,
                             cef,
                             nswf.imports,
                             nswf.workflow,
                             children,
                             IR.WorkflowKind.TopLevel,
                             false,
                             nswf.workflow.unqualifiedName)
                } else {
                    // The workflow has tasks, split into a separate node,
                    // and update the imports and file-list,
                    val taskDict = ns.tasks.map{ task =>
                        task.name -> setDefaultAttributes(task, defaultRuntimeAttributes)
                    }.toMap
                    val tasksLibName = nswf.workflow.unqualifiedName + "_task_lib"
                    val tasksLibPath = tasksLibName + ".wdl"
                    if (ctx.allSourceFiles contains tasksLibPath)
                        throw new Exception(s"Module name collision, ${tasksLibPath} already exists")
                    Utils.trace(ctx.verbose.on, s"""|Splitting out tasks into separate source file
                                                    |path=${tasksLibPath}  name=${tasksLibName}"""
                                    .stripMargin)

                    val child: TreeLeaf = genLeaf(tasksLibName, tasksLibPath, taskDict)
                    ctx.addWdlSourceFile(tasksLibPath, child.genWdlSource)

                    val (nswf2, cef2) = rewriteWorkflowExtractTasks(
                        nswf, tasksLibPath, tasksLibName,
                        taskDict.keys.toSet, nswf.resource, ctx)
                    TreeNode(nswf.workflow.unqualifiedName,
                             cef2,
                             nswf2.imports,
                             nswf2.workflow,
                             children :+ child,
                             IR.WorkflowKind.TopLevel,
                             false,
                             nswf.workflow.unqualifiedName)
                }
        }
    }


    private def calledFromNode(tree: Tree) : Set[String] = {
        tree match {
            case node: TreeNode =>
                node.workflow.calls.map{
                    call => Utils.calleeGetName(call)
                }.toSet
            case leaf: TreeLeaf =>
                Set.empty
        }
    }

    // collect the names of all directly and indirectly accessed
    // tasks/workflows.
    private def collectRefs(tree: Tree,
                            accessed: Set[String]): Set[String] = {
        tree match {
            case node:TreeNode if (accessed contains node.workflow.unqualifiedName) =>
                // This workflow is accessed from the top level
                val allAccessed = calledFromNode(node) ++ accessed
                node.children.foldLeft(allAccessed) {
                    case (accu, leaf: TreeLeaf) =>
                        accu
                    case (accu, child: TreeNode) =>
                        accu ++ collectRefs(child, accu)
                }

            case _ =>
                accessed
        }
    }

    // remove from a tree all the tasks/workflows not in the named set.
    private def filter(tree: Tree,
                       taskWfNames: Set[String]) : Option[Tree] = {
        tree match {
            case leaf: TreeLeaf =>
                val accessedTasks = leaf.tasks.filter{
                    case (_, task) => taskWfNames contains task.unqualifiedName
                }.toMap
                if (accessedTasks.isEmpty)
                    None
                else
                    Some(leaf.copy(tasks = accessedTasks))

            case node: TreeNode if (taskWfNames contains node.workflow.unqualifiedName) =>
                val children = node.children.flatMap{ child => filter(child, taskWfNames) }
                if (children.isEmpty)
                    None
                else
                    Some(node.copy(children = children))

            case _ =>
                None
        }
    }

    // Remove tasks and workflows that are not reachable from the top level
    // workflow.
    //
    // If the entire tree is one leaf, do nothing.
    // The user may be trying to compile standalone tasks as
    // applets.
    def prune(tree: Tree,
              ctx: Context) : Tree = {
        tree match {
            case leaf: TreeLeaf =>
                leaf
            case node: TreeNode =>
                val taskWfNames = collectRefs(node, Set(node.workflow.unqualifiedName))
                filter(node, taskWfNames) match {
                    case None =>
                        // The workflow doesn't call any tasks or sub-workflows
                        val node2 = node.copy(children= Vector.empty)
                        val topWdlFilename: String = ctx.toplevelWdlSourceFile.toString
                        val topWdlSource = Utils.readFileContent(ctx.toplevelWdlSourceFile)
                        ctx.setWdlSourceFiles(topWdlFilename, topWdlSource)
                        node2
                    case Some(tree2) =>
                        ctx.filterUnusedFiles(taskWfNames)
                        Utils.trace(ctx.verbose.on, s"pruned sources = ${ctx.allSourceFiles.keys}")
                        tree2
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
