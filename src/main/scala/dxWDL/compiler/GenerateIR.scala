/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import wom.core.WorkflowSource
import wom.callable.{Callable, CallableTaskDefinition, ExecutableTaskDefinition, WorkflowDefinition}
import wom.graph._

import dxWDL.util._

object GenerateIR {
    def sortByDependencies(allCallables: Vector[Callable],
                           verbose: Verbose) : Vector[Callable] = {
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
                    callNodes.map{ cNode =>
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
                //Utils.trace(verbose.on, s"immediateDeps(${c.name}) = ${deps}")
                deps.subsetOf(readyNames)
            }
            if (satisfiedCallables.isEmpty)
                throw new Exception("Sanity: cannot find the next callable to compile.")
            satisfiedCallables
        }

        var accu = Vector.empty[Callable]
        var crnt = allCallables
        while (!crnt.isEmpty) {
            /*Utils.trace(verbose.on, s"""|  accu=${accu.map(_.name)}
                                        |  crnt=${crnt.map(_.name)}
                                        |""".stripMargin)*/
            val execsToCompile = next(crnt, accu)
            accu = accu ++ execsToCompile
            val alreadyCompiled: Set[String] = accu.map(_.name).toSet
            crnt = crnt.filter{ exec => !(alreadyCompiled contains exec.name) }
        }
        assert(accu.length == allCallables.length)
        accu
    }

    private def compileWorkflow(wf : WorkflowDefinition,
                                wfSource: String,
                                callables: Map[String, IR.Callable],
                                language: Language.Value,
                                locked : Boolean,
                                reorg : Boolean,
                                verbose: Verbose) : (IR.Workflow, Vector[IR.Callable]) = {
        val callToSrcLine = ParseWomSourceFile.scanForCalls(wfSource)

        // sort from low to high according to the source lines.
        val callsLoToHi : Vector[(String, Int)] = callToSrcLine.toVector.sortBy(_._2)

        // Make a list of all task/workflow calls made inside the block. We will need to link
        // to the equivalent dx:applets and dx:workflows.
        val callablesUsedInWorkflow : Vector[IR.Callable] =
            wf.graph.allNodes.collect {
                case cNode : CallNode =>
                    val localname = Utils.getUnqualifiedName(cNode.callable.name)
                    callables(localname)
            }.toVector

        val WdlCodeSnippet(wfSourceStandAlone) =
            WdlCodeGen(verbose).standAloneWorkflow(wfSource,
                                                   callablesUsedInWorkflow,
                                                   language)

        val gir = new GenerateIRWorkflow(wf, wfSource, wfSourceStandAlone,
                                         callsLoToHi, callables, language, verbose)
        gir.apply(locked, reorg)
    }

    // Entry point for compiling tasks and workflows into IR
    private def compileCallable(callable: Callable,
                                taskDir: Map[String, String],
                                workflowDir: Map[String, String],
                                callables: Map[String, IR.Callable],
                                language: Language.Value,
                                locked: Boolean,
                                reorg: Boolean,
                                verbose: Verbose) : (IR.Callable, Vector[IR.Callable]) = {
        def compileTask2(task : CallableTaskDefinition) = {
            val taskSourceCode = taskDir.get(task.name) match {
                case None => throw new Exception(s"Did not find task ${task.name}")
                case Some(x) => x
            }
            GenerateIRTask(verbose).apply(task, taskSourceCode)
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
                        compileWorkflow(wf, wfSource, callables, language, locked, reorg, verbose)
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
              reorg: Boolean,
              verbose: Verbose) : IR.Bundle = {
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
        Utils.trace(verbose.on, s" tasks=${taskDir.keys}")

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
                    s" sortByDependencies ${womBundle.allCallables.values.map{_.name}}")
        val depOrder : Vector[Callable] = sortByDependencies(womBundle.allCallables.values.toVector,
                                                             verbose)
        Utils.trace(verbose.on,
                    s"depOrder =${depOrder.map{_.name}}")

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
                                                       taskDir,
                                                       workflowDir,
                                                       allCallables,
                                                       language,
                                                       isLocked(callable),
                                                       reorg,
                                                       verbose)
            allCallables = allCallables ++ (auxCallables.map{ apl => apl.name -> apl}.toMap)
            allCallables = allCallables + (exec.name -> exec)

            // Add the auxiliary applets while preserving the dependency order
            allCallablesSorted = allCallablesSorted ++ auxCallables :+ exec
        }

        // We already compiled all the individual wdl:tasks and
        // wdl:workflows, let's find the entrypoint.
        val primary = womBundle.primaryCallable.map{ callable =>
            allCallables(Utils.getUnqualifiedName(callable.name))
        }
        val allCallablesSorted2 = allCallablesSorted.map{_.name}
        Utils.trace(verbose.on, s"allCallablesSorted=${allCallablesSorted2}")
        assert(allCallables.size == allCallablesSorted2.size)

        Utils.traceLevelDec()
        IR.Bundle(primary, allCallables, allCallablesSorted2)
    }
}
