/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import wom.core.WorkflowSource
import wom.callable.{Callable, CallableTaskDefinition, ExecutableTaskDefinition, WorkflowDefinition}
import wom.graph._
import wom.types._

import dxWDL.util._

case class GenerateIR(verbose: Verbose) {
    val verbose2 : Boolean = verbose.containsKey("GenerateIR")

    def sortByDependencies(allCallables: Vector[Callable]) : Vector[Callable] = {
        // figure out, for each element, what it depends on.
        // tasks don't depend on anything else. They are at the bottom of the dependency
        // tree.
        val immediateDeps : Map[String, Set[String]] = allCallables.map{ callable =>
            val deps = callable match {
                case _ : ExecutableTaskDefinition => Set.empty[String]
                case _ : CallableTaskDefinition => Set.empty[String]
                case wf: WorkflowDefinition =>
                    val nodes = wf.innerGraph.allNodes
                    val callNodes : Vector[CallNode] = nodes.collect{
                        case cNode: CallNode => cNode
                    }.toVector

                    callNodes.collect{
                        case cNode : WorkflowCallNode =>
                            // We need to ignore calls to scatters that converted by
                            // wdl to internal workflows.
                            val name = Utils.getUnqualifiedName(cNode.callable.name)
                            if (name.startsWith("Scatter"))
                                throw new Exception("nested scatters")
                            name

                        case cNode : CallNode =>
                            // The name is fully qualified, for example, lib.add, lib.concat.
                            // We need the task/workflow itself ("add", "concat"). We are
                            // assuming that the namespace can be flattened; there are
                            // no lib.add and lib2.add.
                            Utils.getUnqualifiedName(cNode.callable.name)
                    }.toSet
                case other =>
                    throw new Exception(s"Don't know how to deal with class ${other.getClass.getSimpleName}")
            }
            Utils.getUnqualifiedName(callable.name) -> deps
        }.toMap

        // Find executables such that all of their dependencies are
        // satisfied. These can be compiled.
        def next(callables: Vector[Callable],
                 ready: Vector[Callable]) : Vector[Callable] = {
            val readyNames = ready.map(_.name).toSet
            val satisfiedCallables = callables.filter{ c =>
                val deps = immediateDeps(c.name)
                Utils.trace(verbose2, s"immediateDeps(${c.name}) = ${deps}")
                deps.subsetOf(readyNames)
            }
            if (satisfiedCallables.isEmpty)
                throw new Exception("Sanity: cannot find the next callable to compile.")
            satisfiedCallables
        }

        var accu = Vector.empty[Callable]
        var crnt = allCallables
        while (!crnt.isEmpty) {
            Utils.trace(verbose2, s"accu=${accu.map(_.name)}")
            Utils.trace(verbose2, s"crnt=${crnt.map(_.name)}")
            val execsToCompile = next(crnt, accu)
            accu = accu ++ execsToCompile
            val alreadyCompiled: Set[String] = accu.map(_.name).toSet
            crnt = crnt.filter{ exec => !(alreadyCompiled contains exec.name) }
        }
        assert(accu.length == allCallables.length)
        accu
    }

    private def compileWorkflow(wf : WorkflowDefinition,
                                typeAliases: Map[String, WomType],
                                wfSource: String,
                                callables: Map[String, IR.Callable],
                                language: Language.Value,
                                locked : Boolean,
                                reorg : Boolean) : (IR.Workflow, Vector[IR.Callable]) = {
        val callToSrcLine = ParseWomSourceFile.scanForCalls(wfSource)

        // sort from low to high according to the source lines.
        val callsLoToHi : Vector[(String, Int)] = callToSrcLine.toVector.sortBy(_._2)

        // Make a list of all task/workflow calls made inside the block. We will need to link
        // to the equivalent dx:applets and dx:workflows.
        val callablesUsedInWorkflow : Vector[IR.Callable] =
            wf.graph.allNodes.collect {
                case cNode : WorkflowCallNode =>
                    // We need to ignore calls to scatters that converted by
                    // wdl to internal workflows.
                    val localName = Utils.getUnqualifiedName(cNode.callable.name)
                    if (localName.startsWith("Scatter"))
                        throw new Exception("""|The workflow contains a nested scatter, it is not
                                               |handled currently due to a cromwell WOM library issue
                                               |""".stripMargin.replaceAll("\n", " "))
                    callables(localName)
                case cNode : CallNode =>
                    val localname = Utils.getUnqualifiedName(cNode.callable.name)
                    callables(localname)
            }.toVector

        val WdlCodeSnippet(wfSourceStandAlone) =
            WdlCodeGen(verbose, typeAliases).standAloneWorkflow(wfSource,
                                                                callablesUsedInWorkflow,
                                                                language)

        val gir = new GenerateIRWorkflow(wf, wfSource, wfSourceStandAlone,
                                         callsLoToHi, callables, language, verbose)
        gir.apply(locked, reorg)
    }

    // Entry point for compiling tasks and workflows into IR
    private def compileCallable(callable: Callable,
                                typeAliases: Map[String, WomType],
                                taskDir: Map[String, String],
                                workflowDir: Map[String, String],
                                callables: Map[String, IR.Callable],
                                language: Language.Value,
                                locked: Boolean,
                                reorg: Boolean) : (IR.Callable, Vector[IR.Callable]) = {
        def compileTask2(task : CallableTaskDefinition) = {
            val taskSourceCode = taskDir.get(task.name) match {
                case None => throw new Exception(s"Did not find task ${task.name}")
                case Some(x) => x
            }
            GenerateIRTask(verbose, typeAliases).apply(task, taskSourceCode)
        }
        callable match {
            case exec : ExecutableTaskDefinition =>
                val task = exec.callableTaskDefinition
                (compileTask2(task), Vector.empty)
            case task : CallableTaskDefinition =>
                (compileTask2(task), Vector.empty)
            case wf: WorkflowDefinition =>
                workflowDir.get(wf.name) match {
                    case None =>
                        throw new Exception(s"Did not find sources for workflow ${wf.name}")
                    case Some(wfSource) =>
                        compileWorkflow(wf, typeAliases, wfSource, callables, language, locked, reorg)
                }
            case x =>
                throw new Exception(s"""|Can't compile: ${callable.name}, class=${callable.getClass}
                                        |${x}
                                        |""".stripMargin.replaceAll("\n", " "))
        }
    }

    // Entrypoint
    def apply(womBundle : wom.executable.WomBundle,
              allSources: Map[String, WorkflowSource],
              language: Language.Value,
              locked: Boolean,
              reorg: Boolean) : IR.Bundle = {
        Utils.trace(verbose.on, s"IR pass")
        Utils.traceLevelInc()

        // Scan the source files and extract the tasks. It is hard
        // to generate WDL from the abstract syntax tree (AST). One
        // issue is that tabs and special characters have to preserved.
        // There is no built-in method for this.
        val taskDir = allSources.foldLeft(Map.empty[String, String]) {
            case (accu, (filename, srcCode)) =>
                val d = ParseWomSourceFile.scanForTasks(language, srcCode)
                accu ++ d
        }
        Utils.trace(verbose.on, s"tasks=${taskDir.keys}")

        val workflowDir = allSources.foldLeft(Map.empty[String, String]) {
            case (accu, (filename, srcCode)) =>
                ParseWomSourceFile.scanForWorkflow(language, srcCode) match {
                    case None =>
                        accu
                    case Some((wfName, wfSource)) =>
                        accu + (wfName -> wfSource)
                }
        }
        Utils.trace(verbose.on,
                    s"sortByDependencies ${womBundle.allCallables.values.map{_.name}}")
        Utils.traceLevelInc()
        val depOrder : Vector[Callable] = sortByDependencies(womBundle.allCallables.values.toVector)
        Utils.trace(verbose.on,
                    s"depOrder =${depOrder.map{_.name}}")
        Utils.traceLevelDec()

        // compile the tasks/workflows from bottom to top.
        var allCallables = Map.empty[String, IR.Callable]
        var allCallablesSorted = Vector.empty[IR.Callable]

        // Only the toplevel workflow may be unlocked. This happens
        // only if the user specifically compiles it as "unlocked".
        def isLocked(callable: Callable): Boolean = {
            (callable, womBundle.primaryCallable) match {
                case (wf: WorkflowDefinition, Some(wf2: WorkflowDefinition)) =>
                    if (wf.name == wf2.name)
                        locked
                    else
                        true
                case (_, _) =>
                    true
            }
        }

        for (callable <- depOrder) {
            val (exec, auxCallables) = compileCallable(callable,
                                                       womBundle.typeAliases,
                                                       taskDir,
                                                       workflowDir,
                                                       allCallables,
                                                       language,
                                                       isLocked(callable),
                                                       reorg)
            allCallables = allCallables ++ (auxCallables.map{ apl => apl.name -> apl}.toMap)
            allCallables = allCallables + (exec.name -> exec)

            // Add the auxiliary applets while preserving the dependency order
            allCallablesSorted = allCallablesSorted ++ auxCallables :+ exec
        }

        // There could be duplicates, remove them here
        allCallablesSorted = allCallablesSorted.distinct

        // We already compiled all the individual wdl:tasks and
        // wdl:workflows, let's find the entrypoint.
        val primary = womBundle.primaryCallable.map{ callable =>
            allCallables(Utils.getUnqualifiedName(callable.name))
        }
        val allCallablesSortedNames = allCallablesSorted.map{_.name}
        Utils.trace(verbose.on, s"allCallables=${allCallables.map(_._1)}")
        Utils.trace(verbose.on, s"allCallablesSorted=${allCallablesSortedNames}")
        assert(allCallables.size == allCallablesSortedNames.size)

        Utils.traceLevelDec()
        IR.Bundle(primary, allCallables, allCallablesSortedNames)
    }
}
