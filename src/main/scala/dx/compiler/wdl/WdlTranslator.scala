package dx.compiler.wdl

import java.nio.file.Path

import dx.api.{DxApi, DxFile, DxProject}
import dx.compiler.ir.{Bundle, Callable, Extras, Parameter, ReorgAttributes, Translator}
import dx.core.io.DxFileDescCache
import dx.core.languages.wdl.{Block, Utils => WdlUtils}
import spray.json.JsValue
import wdlTools.eval.WdlValues
import wdlTools.types.{TypeCheckingRegime, TypedAbstractSyntax => TAT}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.util.{Adjuncts, FileSourceResolver, FileUtils, LocalFileSource, Logger}

/**
  * Compiles WDL to IR.
  * TODO: remove limitation that two callables cannot have the same name
  * TODO: rewrite sortByDependencies using a graph data structure
  */
case class WdlTranslator(extras: Option[Extras[WdlValues.V]] = None,
                         regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                         fileResolver: FileSourceResolver = FileSourceResolver.get,
                         dxApi: DxApi = DxApi.get,
                         logger: Logger = Logger.get)
    extends Translator(fileResolver, dxApi, logger) {

  override protected def translateInput(parameter: Parameter,
                                        jsv: JsValue,
                                        dxName: String,
                                        encodeDots: Boolean): Map[String, JsValue] = {}

  // check the declarations in [graph], and make sure they
  // do not contain the reserved '___' substring.
  def checkVariableName(decls: Vector[TAT.Variable]): Unit = {
    decls.foreach {
      case TAT.Declaration(name, _, _, _) if name.contains("___") =>
        throw new Exception(s"Variable ${name} is using the reserved substring ___")
    }
  }

  // check that streaming annotations are only done for files.
  private def validateVariableNames(callable: TAT.Callable): Unit = {
    callable match {
      case wf: TAT.Workflow =>
        if (wf.parameterMeta.isDefined) {
          logger.warning("dxWDL workflows ignore their parameter meta section")
        }
        checkVariableName(wf.inputs)
        checkVariableName(wf.outputs)
        val allDeclarations: Vector[TAT.Declaration] = wf.body.collect {
          case d: TAT.Declaration => d
        }
        checkVariableName(allDeclarations)
      case task: TAT.Task =>
        checkVariableName(task.inputs)
        checkVariableName(task.outputs)
    }
  }

  private def bundleInfoFromDoc(doc: TAT.Document): WdlBundle = {
    // Add source and adjuncts for main file
    val (sources, adjunctFiles) = doc.source match {
      case localFs: LocalFileSource =>
        val absPath: Path = localFs.localPath
        val sources = Map(absPath.toString -> doc)
        val adjunctFiles = Adjuncts.findAdjunctFiles(absPath)
        (sources, adjunctFiles)
      case fs =>
        val sources = Map(fs.toString -> doc)
        (sources, Map.empty[String, Vector[Adjuncts.AdjunctFile]])
    }
    val tasks: Map[String, TAT.Task] = doc.elements.collect {
      case x: TAT.Task =>
        validateVariableNames(x)
        x.name -> x
    }.toMap
    val workflows = doc.workflow.map { x =>
      validateVariableNames(x)
      x.name -> x
    }.toMap
    val primaryCallable: Option[TAT.Callable] = doc.workflow match {
      case None if tasks.size == 1 => Some(tasks.values.head)
      case wf                      => wf
    }
    WdlBundle(doc.version.value,
              primaryCallable,
              tasks,
              workflows,
              tasks.keySet ++ workflows.keySet,
              sources,
              adjunctFiles)
  }

  private def mergeBundleInfo(accu: WdlBundle, from: WdlBundle): WdlBundle = {
    val version = accu.version
    if (version != from.version) {
      throw new RuntimeException(s"Different WDL versions: ${version} != ${from.version}")
    }
    val intersection = (accu.callableNames & from.callableNames)
      .map { name =>
        val aCallable = accu.tasks.getOrElse(name, accu.workflows(name))
        val bCallable = from.tasks.getOrElse(name, from.workflows(name))
        name -> (aCallable, bCallable)
      }
      .filter {
        // The comparision is done with "toString", because otherwise two identical
        // definitions are somehow, through the magic of Scala, unequal.
        case (_, (ac, bc)) => ac == bc
      }
      .toMap
    if (intersection.nonEmpty) {
      intersection.foreach {
        case (name, (ac, bc)) =>
          logger.error(s"""|name ${name} appears with two different callable definitions
                           |1)
                           |${ac}
                           |2)
                           |${bc}
                           |""".stripMargin)
      }
      throw new Exception(
          s"callable(s) ${intersection.keySet} appears multiple times with different definitions"
      )
    }
    WdlBundle(
        version,
        accu.primaryCallable.orElse(from.primaryCallable),
        accu.tasks ++ from.tasks,
        accu.workflows ++ from.workflows,
        accu.callableNames | from.callableNames,
        accu.sources ++ from.sources,
        accu.adjunctFiles ++ from.adjunctFiles
    )
  }

  // recurse into the imported packages
  // Check the uniqueness of tasks, Workflows, and Types
  // merge everything into one bundle.
  private def flattenDepthFirst(tDoc: TAT.Document): WdlBundle = {
    val topLevelInfo = bundleInfoFromDoc(tDoc)
    val imports: Vector[TAT.ImportDoc] = tDoc.elements.collect {
      case x: TAT.ImportDoc => x
    }
    imports.foldLeft(topLevelInfo) {
      case (accu: WdlBundle, imp) =>
        val flatImportInfo = flattenDepthFirst(imp.doc)
        mergeBundleInfo(accu, flatImportInfo)
    }
  }

  private def getUnqualifiedName(name: String): String = {
    if (name contains ".") {
      name.split("\\.").last
    } else {
      name
    }
  }

  private def sortByDependencies(bundleInfo: WdlBundle,
                                 traceLogger: Logger): Vector[TAT.Callable] = {
    // We only need to figure out the dependency order of workflows. Tasks don't depend
    // on anything else - they are at the bottom of the dependency tree.
    val wfDeps: Map[String, Set[String]] = bundleInfo.workflows.map {
      case (name, wf) =>
        getUnqualifiedName(name) -> Block
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

    // Iteratively identify executables for which all dependencies are satisfied -
    // these can be compiled.
    var remainingWorkflows = bundleInfo.workflows.values.toVector
    var orderedWorkflows = Vector.empty[TAT.Workflow]
    var orderedNames = bundleInfo.tasks.keySet
    traceLogger.trace("Sorting workflows by dependency order")
    while (remainingWorkflows.nonEmpty) {
      if (traceLogger.isVerbose) {
        traceLogger.trace(s"ordered: ${orderedWorkflows.map(_.name)}")
        traceLogger.trace(s"remaining: ${remainingWorkflows.map(_.name)}")
      }
      // split the remaining workflows into those who have all dependencies satisfied
      // and those who do not
      val (satisfied, unsatisfied) =
        remainingWorkflows.partition(wf => wfDeps(wf.name).subsetOf(orderedNames))
      // no workflows were fully satisfied on this pass - we're stuck :(
      if (satisfied.nonEmpty) {
        if (traceLogger.isVerbose) {
          satisfied.foreach { wf =>
            traceLogger.trace(
                s"Satisifed all workflow ${wf.name} dependencies ${wfDeps(wf.name)}"
            )
          }
        }
        orderedWorkflows ++= satisfied
        orderedNames |= satisfied.map(_.name).toSet
        remainingWorkflows = unsatisfied
      } else {
        val stuck = remainingWorkflows.map(_.name)
        val stuckWaitingOn: Map[String, Set[String]] = stuck.map { name =>
          name -> (wfDeps(name) -- orderedNames)
        }.toMap
        throw new Exception(s"""|Cannot find the next callable to compile.
                                |ready = ${orderedNames}
                                |stuck = ${stuck}
                                |stuckWaitingOn =
                                |${stuckWaitingOn.mkString("\n")}
                                |""".stripMargin)
      }
    }
    // ensure we've accounted for all the callables
    assert(orderedNames == bundleInfo.callableNames)
    // Add tasks to the beginning - it doesn't matter what order these are compiled
    bundleInfo.tasks.values.toVector ++ orderedWorkflows
  }

  override protected def translateDocument(source: Path,
                                           locked: Boolean,
                                           reorgEnabled: Option[Boolean]): Bundle = {
    val sourceAbsPath = FileUtils.absolutePath(source)
    val sourceFileResolver = fileResolver.addToLocalSearchPath(Vector(sourceAbsPath.getParent))
    val (tDoc, typeAliases) =
      WdlUtils.parseSource(sourceAbsPath, sourceFileResolver, regime, logger)
    val wdlBundle: WdlBundle = flattenDepthFirst(tDoc)
    // sort callables by dependencies
    val logger2 = logger.withIncTraceIndent()
    val depOrder: Vector[TAT.Callable] = sortByDependencies(wdlBundle, logger2)
    if (logger.isVerbose) {
      logger2.trace(s"all tasks: ${wdlBundle.tasks.keySet}")
      logger2.trace(s"all callables in dependency order: ${depOrder.map { _.name }}")
    }
    // load defaults from extras
    val defaultRuntimeAttrs =
      extras.map(_.defaultRuntimeAttributes).getOrElse(Map.empty)
    val reorgAttrs = (extras.flatMap(_.customReorgAttributes), reorgEnabled) match {
      case (Some(attr), None)    => attr
      case (Some(attr), Some(b)) => attr.copy(enabled = b)
      case (None, Some(b))       => ReorgAttributes(enabled = b)
      case (None, None)          => ReorgAttributes(enabled = false)
    }
    // translate callables
    val callableTranslator = CallableTranslator(
        wdlBundle,
        typeAliases,
        locked,
        defaultRuntimeAttrs,
        reorgAttrs,
        dxApi,
        sourceFileResolver,
        logger
    )
    val (allCallables, sortedCallables) =
      depOrder.foldLeft((Map.empty[String, Callable], Vector.empty[Callable])) {
        case ((allCallables, sortedCallables), callable) =>
          val translatedCallables = callableTranslator.translateCallable(callable, allCallables)
          (
              allCallables ++ translatedCallables.map(c => c.name -> c).toMap,
              sortedCallables ++ translatedCallables
          )
      }
    val allCallablesSortedNames = sortedCallables.map(_.name).distinct
    val primaryCallable = wdlBundle.primaryCallable.map { callable =>
      allCallables(getUnqualifiedName(callable.name))
    }
    if (logger2.isVerbose) {
      logger2.trace(s"allCallables: ${allCallables.keys}")
      logger2.trace(s"allCallablesSorted: ${allCallablesSortedNames}")
    }
    Bundle(primaryCallable, allCallables, allCallablesSortedNames, irTypeAliases)
  }

  override protected def filterFiles(
      fields: Map[String, JsValue],
      dxProject: DxProject
  ): (Map[String, DxFile], Vector[DxFile]) = ???

  override protected def embedDefaults(bundle: Bundle,
                                       fileResolver: FileSourceResolver,
                                       pathToDxFile: Map[String, DxFile],
                                       dxFileDescCache: DxFileDescCache,
                                       defaults: Map[String, JsValue]): Bundle = ???

}
