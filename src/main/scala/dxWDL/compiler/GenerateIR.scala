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

  // Convert a fully qualified name to a local name.
  // Examples:
  //   SOURCE         RESULT
  //   lib.concat     concat
  //   lib.sum_list   sum_list
  private def getUnqualifiedName(fqn: String): String = {
    if (fqn contains ".") {
      fqn.split("\\.").last
    } else {
      fqn
    }
  }

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
            .map { call: TAT.Call =>
              // The name is fully qualified, for example, lib.add, lib.concat.
              // We need the task/workflow itself ("add", "concat"). We are
              // assuming that the namespace can be flattened; there are
              // no lib.add and lib2.add.
              call.unqualifiedName
            }
            .toSet
      }
      getUnqualifiedName(callable.name) -> deps
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
    while (crnt.nonEmpty) {
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
        .map { cNode: TAT.Call =>
          val localname = cNode.unqualifiedName
          callables(localname)
        }

    val standAloneWorkflow =
      WdlCodeGen(verbose, typeAliases, language)
        .standAloneWorkflow(wf, callablesUsedInWorkflow)

    val gir = GenerateIRWorkflow(wf,
                                 standAloneWorkflow,
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
      callables: Map[String, IR.Callable],
      language: Language.Value,
      locked: Boolean,
      reorg: Either[Boolean, ReorgAttrs],
      adjunctFiles: Option[Vector[Adjuncts.AdjunctFile]]
  ): (IR.Callable, Vector[IR.Callable]) = {
    def compileTask2(task: TAT.Task): IR.Applet = {
      GenerateIRTask(verbose, typeAliases, language, defaultRuntimeAttrs, defaultHintAttrs)
        .apply(task, adjunctFiles)
    }
    callable match {
      case task: TAT.Task =>
        (compileTask2(task), Vector.empty)
      case wf: TAT.Workflow =>
        compileWorkflow(wf, typeAliases, callables, language, locked, reorg, adjunctFiles)
      case x =>
        throw new Exception(s"""|Can't compile: ${callable.name}, class=${callable.getClass}
                                |${x}
                                |""".stripMargin.replaceAll("\n", " "))
    }
  }

  // Entrypoint
  def apply(womBundle: WomBundle,
            allSources: Map[String, TAT.Document],
            language: Language.Value,
            locked: Boolean,
            reorg: Either[Boolean, ReorgAttrs],
            adjunctFiles: Map[String, Vector[Adjuncts.AdjunctFile]]): IR.Bundle = {
    Utils.trace(verbose.on, s"IR pass")
    Utils.traceLevelInc()

    val taskDir = allSources.foldLeft(Map.empty[String, TAT.Task]) {
      case (accu, (_, doc)) =>
        accu ++ doc.elements.collect {
          case t: TAT.Task => t.name -> t
        }.toMap
    }
    Utils.trace(verbose.on, s"tasks=${taskDir.keys}")
    Utils.trace(verbose.on, s"sortByDependencies ${womBundle.allCallables.values.map { _.name }}")
    Utils.traceLevelInc()

    val depOrder: Vector[TAT.Callable] = sortByDependencies(womBundle.allCallables.values.toVector)
    Utils.trace(verbose.on, s"depOrder =${depOrder.map { _.name }}")
    Utils.traceLevelDec()

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

    val (allCallables, allCallablesSorted): (Map[String, IR.Callable], Vector[IR.Callable]) =
      depOrder.foldLeft((Map.empty[String, IR.Callable], Vector.empty[IR.Callable])) {
        case ((allCallables, allCallablesSorted), callable) =>
          val (exec, auxCallables) = compileCallable(callable,
                                                     womBundle.typeAliases,
                                                     allCallables,
                                                     language,
                                                     isLocked(callable),
                                                     reorg,
                                                     adjunctFiles.get(callable.name))
          // Add the auxiliary applets while preserving the dependency order
          (
              allCallables ++ auxCallables.map(apl => apl.name -> apl).toMap ++ Map(
                  exec.name -> exec
              ),
              allCallablesSorted ++ auxCallables :+ exec
          )
      }

    // We already compiled all the individual wdl:tasks and
    // wdl:workflows, let's find the entrypoint.
    val primary = womBundle.primaryCallable.map { callable =>
      allCallables(getUnqualifiedName(callable.name))
    }
    val allCallablesSortedNames = allCallablesSorted.map(_.name).distinct
    Utils.trace(verbose.on, s"allCallables=${allCallables.keys}")
    Utils.trace(verbose.on, s"allCallablesSorted=${allCallablesSortedNames}")
    assert(allCallables.size == allCallablesSortedNames.size)

    Utils.traceLevelDec()
    IR.Bundle(primary, allCallables, allCallablesSortedNames, womBundle.typeAliases)
  }
}
