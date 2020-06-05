/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.types.WdlTypes

import dxWDL.base._
import dxWDL.util.Block

case class GenerateIR(verbose: Verbose,
                      defaultRuntimeAttrs: WdlRuntimeAttrs,
                      defaultHintAttrs: WdlHintAttrs) {
  val verbose2: Boolean = verbose.containsKey("GenerateIR")

  def sortByDependencies(allCallables: Vector[TAT.Callable]): Vector[TAT.Callable] = {
    // figure out, for each element, what it depends on.
    // tasks don't depend on anything else. They are at the bottom of the dependency
    // tree.
    val immediateDeps: Map[String, Set[String]] = allCallables.map { callable =>
      val deps: Set[String] = callable match {
        case _: TAT.Task => Set.empty[String]
        case wf: TAT.Workflow =>
          Block
            .deepFindCalls(wf.body)
            .map {
              case call: TAT.Call =>
                // The name is fully qualified, for example, lib.add, lib.concat.
                // We need the task/workflow itself ("add", "concat"). We are
                // assuming that the namespace can be flattened; there are
                // no lib.add and lib2.add.
                Utils.getUnqualifiedName(call.callee.name)
            }
            .toSet
      }
      Utils.getUnqualifiedName(callable.name) -> deps
    }.toMap

    // Find executables such that all of their dependencies are
    // satisfied. These can be compiled.
    def next(callables: Vector[TAT.Callable], ready: Vector[TAT.Callable]): Vector[TAT.Callable] = {
      val readyNames = ready.map(_.name).toSet
      val satisfiedCallables = callables.filter { c =>
        val deps = immediateDeps(c.name)
        Utils.trace(verbose2, s"immediateDeps(${c.name}) = ${deps}")
        deps.subsetOf(readyNames)
      }
      if (satisfiedCallables.isEmpty) {
        val stuck = callables.map(_.name).toSet -- readyNames
        val stuckWaitingOn: Map[String, Set[String]] = stuck.map { name =>
          name -> (immediateDeps(name) -- readyNames)
        }.toMap
        val explanationLines = stuckWaitingOn.mkString("\n")
        throw new Exception(s"""|Sanity: cannot find the next callable to compile.
                                |ready = ${readyNames}
                                |stuck = ${stuck}
                                |stuckWaitingOn =
                                |${explanationLines}
                                |""".stripMargin)
      }
      satisfiedCallables
    }

    var accu = Vector.empty[TAT.Callable]
    var crnt = allCallables
    while (!crnt.isEmpty) {
      Utils.trace(verbose2, s"accu=${accu.map(_.name)}")
      Utils.trace(verbose2, s"crnt=${crnt.map(_.name)}")
      val execsToCompile = next(crnt, accu)
      accu = accu ++ execsToCompile
      val alreadyCompiled: Set[String] = accu.map(_.name).toSet
      crnt = crnt.filter { exec =>
        !(alreadyCompiled contains exec.name)
      }
    }
    assert(accu.length == allCallables.length)
    accu
  }

  private def compileWorkflow(
      wf: TAT.Workflow,
      typeAliases: Map[String, WdlTypes.T],
      wfSource: String,
      callables: Map[String, IR.Callable],
      language: Language.Value,
      locked: Boolean,
      reorg: Either[Boolean, ReorgAttrs],
      adjunctFiles: Option[Vector[Adjuncts.AdjunctFile]]
  ): (IR.Workflow, Vector[IR.Callable]) = {
    // Make a list of all task/workflow calls made inside the block. We will need to link
    // to the equivalent dx:applets and dx:workflows.
    val callablesUsedInWorkflow: Vector[IR.Callable] =
      Block
        .deepFindCalls(wf.body)
        .map {
          case cNode: TAT.Call =>
            val localname = Utils.getUnqualifiedName(cNode.callee.name)
            callables(localname)
        }
        .toVector

    val WdlCodeSnippet(wfSourceStandAlone) =
      WdlCodeGen(verbose, typeAliases, language)
        .standAloneWorkflow(wfSource, callablesUsedInWorkflow)

    val gir = new GenerateIRWorkflow(wf,
                                     wfSource,
                                     wfSourceStandAlone,
                                     callables,
                                     language,
                                     verbose,
                                     locked,
                                     reorg,
                                     adjunctFiles)
    gir.apply()
  }

  // Entry point for compiling tasks and workflows into IR
  private def compileCallable(
      callable: TAT.Callable,
      typeAliases: Map[String, WdlTypes.T],
      taskDir: Map[String, String],
      workflowDir: Map[String, String],
      callables: Map[String, IR.Callable],
      language: Language.Value,
      locked: Boolean,
      reorg: Either[Boolean, ReorgAttrs],
      adjunctFiles: Option[Vector[Adjuncts.AdjunctFile]]
  ): (IR.Callable, Vector[IR.Callable]) = {
    def compileTask2(task: TAT.Task) = {
      val taskSourceCode = taskDir.get(task.name) match {
        case None    => throw new Exception(s"Did not find task ${task.name}")
        case Some(x) => x
      }
      GenerateIRTask(verbose, typeAliases, language, defaultRuntimeAttrs, defaultHintAttrs)
        .apply(task, taskSourceCode, adjunctFiles)
    }
    callable match {
      case task: TAT.Task =>
        (compileTask2(task), Vector.empty)
      case wf: TAT.Workflow =>
        workflowDir.get(wf.name) match {
          case None =>
            throw new Exception(s"Did not find sources for workflow ${wf.name}")
          case Some(wfSource) =>
            compileWorkflow(wf,
                            typeAliases,
                            wfSource,
                            callables,
                            language,
                            locked,
                            reorg,
                            adjunctFiles)
        }
      case x =>
        throw new Exception(s"""|Can't compile: ${callable.name}, class=${callable.getClass}
                                |${x}
                                |""".stripMargin.replaceAll("\n", " "))
    }
  }

  // Entrypoint
  def apply(womBundle: WomBundle,
            allSources: Map[String, String],
            language: Language.Value,
            locked: Boolean,
            reorg: Either[Boolean, ReorgAttrs],
            adjunctFiles: Map[String, Vector[Adjuncts.AdjunctFile]]): IR.Bundle = {
    Utils.trace(verbose.on, s"IR pass")
    Utils.traceLevelInc()

    val taskDir = allSources.foldLeft(Map.empty[String, String]) {
      case (accu, (filename, wdlSourceCode)) =>
        val d = ParseWomSourceFile(verbose.on).scanForTasks(wdlSourceCode)
        accu ++ d
    }
    Utils.trace(verbose.on, s"tasks=${taskDir.keys}")

    val workflowDir = allSources.foldLeft(Map.empty[String, String]) {
      case (accu, (filename, srcCode)) =>
        ParseWomSourceFile(verbose.on).scanForWorkflow(srcCode) match {
          case None =>
            accu
          case Some((wfName, wfSource)) =>
            accu + (wfName -> wfSource)
        }
    }
    Utils.trace(verbose.on, s"sortByDependencies ${womBundle.allCallables.values.map { _.name }}")
    Utils.traceLevelInc()

    val depOrder: Vector[TAT.Callable] = sortByDependencies(womBundle.allCallables.values.toVector)
    Utils.trace(verbose.on, s"depOrder =${depOrder.map { _.name }}")
    Utils.traceLevelDec()

    // compile the tasks/workflows from bottom to top.
    var allCallables = Map.empty[String, IR.Callable]
    var allCallablesSorted = Vector.empty[IR.Callable]

    // Only the toplevel workflow may be unlocked. This happens
    // only if the user specifically compiles it as "unlocked".
    def isLocked(callable: TAT.Callable): Boolean = {
      (callable, womBundle.primaryCallable) match {
        case (wf: TAT.Workflow, Some(wf2: TAT.Workflow)) =>
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
                                                 reorg,
                                                 adjunctFiles.get(callable.name))
      allCallables = allCallables ++ (auxCallables.map { apl =>
        apl.name -> apl
      }.toMap)
      allCallables = allCallables + (exec.name -> exec)

      // Add the auxiliary applets while preserving the dependency order
      allCallablesSorted = allCallablesSorted ++ auxCallables :+ exec
    }

    // There could be duplicates, remove them here
    allCallablesSorted = allCallablesSorted.distinct

    // We already compiled all the individual wdl:tasks and
    // wdl:workflows, let's find the entrypoint.
    val primary = womBundle.primaryCallable.map { callable =>
      allCallables(Utils.getUnqualifiedName(callable.name))
    }
    val allCallablesSortedNames = allCallablesSorted.map { _.name }
    Utils.trace(verbose.on, s"allCallables=${allCallables.map(_._1)}")
    Utils.trace(verbose.on, s"allCallablesSorted=${allCallablesSortedNames}")
    assert(allCallables.size == allCallablesSortedNames.size)

    Utils.traceLevelDec()
    IR.Bundle(primary, allCallables, allCallablesSortedNames, womBundle.typeAliases)
  }
}
