package dx.compiler.wdl

import java.nio.file.Path

import dx.api.{DxApi, DxFile, DxProject}
import dx.compiler
import dx.compiler.{GenerateIR, ReorgAttrs, WdlHintAttrs, WdlRuntimeAttrs}
import dx.compiler.ir.{Bundle, Extras, Parameter, RuntimeAttributes, Translator}
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache}
import dx.core.languages.wdl.{Block, ParseSource, Utils, Bundle => WdlBundle}
import spray.json.JsValue
import wdlTools.types.{TypeCheckingRegime, TypedAbstractSyntax => TAT}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.util.{Adjuncts, FileSourceResolver, FileUtils, LocalFileSource, Logger}

/**
  * Compiles WDL to IR.
  * @Todo remove limitation that two callables cannot have the same name
  * @Todo rewrite sortByDependencies using a graph data structure
  */
case class WdlTranslator(extras: Option[Extras] = None,
                         reorgEnabled: Option[Boolean] = None,
                         fileResolver: FileSourceResolver = FileSourceResolver.get,
                         regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                         dxApi: DxApi = DxApi.get,
                         logger: Logger = Logger.get)
    extends Translator(dxApi, logger) {
  private case class BundleInfo(tasks: Map[String, TAT.Task],
                                workflows: Map[String, TAT.Workflow],
                                callableNames: Set[String],
                                sources: Map[String, TAT.Document],
                                adjunctFiles: Map[String, Vector[Adjuncts.AdjunctFile]])

  def bundleInfoFromDoc(doc: TAT.Document): BundleInfo = {
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
      case x: TAT.Task => x.name -> x
    }.toMap
    val workflows = doc.workflow.map(x => x.name -> x).toMap
    BundleInfo(tasks, workflows, tasks.keySet ++ workflows.keySet, sources, adjunctFiles)
  }

  private def mergeBundleInfo(a: BundleInfo, b: BundleInfo): BundleInfo = {
    val intersection = (a.callableNames & b.callableNames)
      .map { name =>
        val aCallable = a.tasks.getOrElse(name, a.workflows(name))
        val bCallable = b.tasks.getOrElse(name, b.workflows(name))
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
    BundleInfo(
        a.tasks ++ b.tasks,
        a.workflows ++ b.workflows,
        a.callableNames | b.callableNames,
        a.sources ++ b.sources,
        a.adjunctFiles ++ b.adjunctFiles
    )
  }

  // recurse into the imported packages
  // Check the uniqueness of tasks, Workflows, and Types
  // merge everything into one bundle.
  private def flattenDepthFirst(tDoc: TAT.Document): BundleInfo = {
    val topLevelInfo = bundleInfoFromDoc(tDoc)
    val imports: Vector[TAT.ImportDoc] = tDoc.elements.collect {
      case x: TAT.ImportDoc => x
    }
    imports.foldLeft(topLevelInfo) {
      case (accu: BundleInfo, imp) =>
        val flatImportInfo = flattenDepthFirst(imp.doc)
        mergeBundleInfo(accu, flatImportInfo)
    }
  }

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

  private def getUnqualifiedName(name: String): String = {
    if (name contains ".") {
      name.split("\\.").last
    } else {
      name
    }
  }

  private def sortByDependencies(bundleInfo: BundleInfo,
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

  override protected def translateDocument(source: Path): Bundle = {
    val sourceAbsPath = FileUtils.absolutePath(source)
    val sourceFileResolver = fileResolver.addToLocalSearchPath(Vector(sourceAbsPath.getParent))
    val (tDoc, typeAliases) = Utils.parseSource(sourceAbsPath, sourceFileResolver, regime, logger)
    // validate document and variable names
    val primaryCallable =
      tDoc.workflow match {
        case None =>
          val tasks: Vector[TAT.Task] = tDoc.elements.collect {
            case x: TAT.Task => x
          }
          if (tasks.size == 1) {
            Some(tasks.head)
          } else {
            None
          }
        case Some(wf) => Some(wf)
        case _        => None
      }
    primaryCallable match {
      case None    => ()
      case Some(x) => validateVariableNames(x)
    }
    val flatInfo: BundleInfo = flattenDepthFirst(tDoc)
    (flatInfo.tasks ++ flatInfo.workflows).values.foreach(validateVariableNames)
    // sort callables by dependencies
    val logger2 = logger.withIncTraceIndent()
    val depOrder: Vector[TAT.Callable] = sortByDependencies(flatInfo, logger2)
    if (logger.isVerbose) {
      logger2.trace(s"all tasks: ${flatInfo.tasks.keySet}")
      logger2.trace(s"all callables in dependency order: ${depOrder.map { _.name }}")
    }

    val defaultRuntimeAttrs =
      extras.map(_.defaultRuntimeAttributes).getOrElse(RuntimeAttributes.empty)
    val reorgAttrs = extras.map(_.customReorgAttributes)
  }

  override protected def translateInput(parameter: Parameter,
                                        jsv: JsValue,
                                        dxName: String,
                                        encodeDots: Boolean): Map[String, JsValue] = ???

  override protected def filterFiles(
      fields: Map[String, JsValue]
  ): (Map[String, DxFile], Vector[DxFile]) = ???

  override def embedDefaults(bundle: Bundle,
                             fileResolver: FileSourceResolver,
                             pathToDxFile: Map[String, DxFile],
                             dxFileDescCache: DxFileDescCache,
                             defaults: Map[String, JsValue]): Bundle = ???

  override protected def translate(source: Path): Bundle = {
    val (_, language, everythingBundle, allSources, adjunctFiles) =
      ParseSource(dxApi).apply(source, cOpt.importDirs)

    // validate
    everythingBundle.allCallables.foreach { case (_, c) => validateVariableNames(c) }
    everythingBundle.primaryCallable match {
      case None    => ()
      case Some(x) => validateVariableNames(x)
    }

    // Compile the WDL workflow into an Intermediate
    // Representation (IR)
    val defaultRuntimeAttrs = extras match {
      case None     => RuntimeAttributes.empty
      case Some(ex) => ex.defaultRuntimeAttributes
    }

    val reorg = reorgEnabled.orElse(extras.map(_.customReorgAttributes.isDefined)).getOrElse(false)

    // TODO: load default hints from attrs
    val defaultHintAttrs = WdlHintAttrs(Map.empty)
    compiler
      .GenerateIR(dxApi, defaultRuntimeAttrs, defaultHintAttrs)
      .apply(everythingBundle, allSources, language, cOpt.locked, reorgApp, adjunctFiles)
  }

  def apply(source: Path,
            inputs: Vector[Path] = Vector.empty,
            defaults: Option[Path] = None): Bundle = {
    // generate IR
    val bundle: Bundle = translate(source)

    // lookup platform files in bulk
    val (pathToDxFile, dxFileDescCache) = bulkFileDescribe(bundle, dxProject)
    val dxProtocol = DxFileAccessProtocol(dxApi, dxFileDescCache)
    val fileResolver =
      FileSourceResolver.create(userProtocols = Vector(dxProtocol), logger = logger)

    // handle changes resulting from setting defaults, and
    // generate DNAx input files.
    handleInputFiles(bundle, fileResolver, pathToDxFile, dxFileDescCache)
  }
}
