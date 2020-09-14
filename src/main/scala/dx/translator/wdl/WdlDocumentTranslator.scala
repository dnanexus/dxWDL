package dx.translator.wdl

import java.nio.file.Path

import dx.api.{DxApi, DxProject}
import dx.core.ir._
import dx.core.languages.Language
import dx.core.languages.Language.Language
import dx.core.languages.wdl.{Utils => WdlUtils}
import dx.translator.{
  CommonStage,
  DocumentTranslator,
  DocumentTranslatorFactory,
  ExactlyOnce,
  InputFile,
  ReorgAttributes
}
import spray.json._
import wdlTools.eval.WdlValueSerde
import wdlTools.types.{TypeCheckingRegime, WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.util.{DefaultBindings, FileSourceResolver, JsUtils, Logger}

/**
  * Compiles WDL to IR.
  * TODO: remove limitation that two callables cannot have the same name
  * TODO: rewrite sortByDependencies using a graph data structure
  */
case class WdlDocumentTranslator(doc: TAT.Document,
                                 typeAliases: DefaultBindings[WdlTypes.T_Struct],
                                 inputs: Vector[Path],
                                 defaults: Option[Path],
                                 locked: Boolean,
                                 defaultRuntimeAttrs: Map[String, Value],
                                 reorgAttrs: ReorgAttributes,
                                 project: DxProject,
                                 fileResolver: FileSourceResolver = FileSourceResolver.get,
                                 dxApi: DxApi = DxApi.get,
                                 logger: Logger = Logger.get)
    extends DocumentTranslator(fileResolver, dxApi) {

  private def wdlInputToIr(dxType: Type, jsValue: JsValue): Value = {
    // convert js to WDL value
    val wdlType = WdlUtils.fromIRType(dxType, typeAliases.bindings)
    val wdlValue = WdlValueSerde.deserialize(jsValue, wdlType)
    // convert WDL value to IR value
    WdlUtils.toIRValue(wdlValue, wdlType)
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
        getUnqualifiedName(name) -> WdlUtils
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

  // skip comment lines, these start with ##
  private def removeCommentFields(inputs: Map[String, JsValue]): Map[String, JsValue] = {
    inputs.foldLeft(Map.empty[String, JsValue]) {
      case (accu, (k, v)) if !k.startsWith("##") => accu + (k -> v)
    }
  }

  private lazy val rawBundle: Bundle = {
    val wdlBundle: WdlBundle = WdlBundle.flattenDepthFirst(doc)
    // sort callables by dependencies
    val logger2 = logger.withIncTraceIndent()
    val depOrder: Vector[TAT.Callable] = sortByDependencies(wdlBundle, logger2)
    if (logger.isVerbose) {
      logger2.trace(s"all tasks: ${wdlBundle.tasks.keySet}")
      logger2.trace(s"all callables in dependency order: ${depOrder.map { _.name }}")
    }
    // translate callables
    val callableTranslator = CallableTranslator(
        wdlBundle,
        typeAliases,
        locked,
        defaultRuntimeAttrs,
        reorgAttrs,
        dxApi,
        fileResolver,
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
    val irTypeAliases = typeAliases.toMap.map {
      case (name, struct: WdlTypes.T_Struct) => name -> WdlUtils.toIRType(struct)
    }
    Bundle(primaryCallable, allCallables, allCallablesSortedNames, irTypeAliases)
  }

  private lazy val wdlJsInputs = inputs
    .map(path => path -> removeCommentFields(JsUtils.getFields(JsUtils.jsFromFile(path))))
    .toMap
  private lazy val wdlJsDefaults = defaults
    .map(path => removeCommentFields(JsUtils.getFields(JsUtils.jsFromFile(path))))
    .getOrElse(Map.empty)
  private lazy val fileResolverWithCache = {
    val inputAndDefaults = (wdlJsInputs.values.flatten ++ wdlJsDefaults).toMap
    fileResolverWithCachedFiles(bundle, inputAndDefaults, project)
  }

  private lazy val bundleWithDefaults: Bundle = {
    logger.trace(s"Embedding defaults into the IR")
    val defaultsExactlyOnce = ExactlyOnce("default", wdlJsDefaults, logger)
    val allCallablesWithDefaults: Map[String, Callable] = rawBundle.allCallables.map {
      case (name, applet: Application) =>
        val inputsWithDefaults = applet.inputs.map { param =>
          val fqn = s"${applet.name}.${param.name}"
          defaultsExactlyOnce.get(fqn) match {
            case None => param
            case Some(default: JsValue) =>
              val irValue = wdlInputToIr(param.dxType, default)
              param.copy(defaultValue = Some(irValue))
          }
        }
        name -> applet.copy(inputs = inputsWithDefaults)
      case (name, workflow: Workflow) =>
        val workflowWithDefaults =
          if (workflow.locked) {
            // locked workflow - we have workflow-level inputs
            val inputsWithDefaults = workflow.inputs.map {
              case (param, stageInput) =>
                val fqn = s"${workflow.name}.${param.name}"
                val stageInputWithDefault = defaultsExactlyOnce.get(fqn) match {
                  case None =>
                    stageInput
                  case Some(default: JsValue) =>
                    StaticInput(wdlInputToIr(param.dxType, default))
                }
                (param, stageInputWithDefault)
            }
            workflow.copy(inputs = inputsWithDefaults)
          } else {
            // Workflow is unlocked, we don't have workflow-level inputs.
            // Instead, set the defaults in the COMMON stage.
            val stagesWithDefaults = workflow.stages.map { stage =>
              val callee: Callable = rawBundle.allCallables(stage.calleeName)
              logger.trace(s"addDefaultToStage ${stage.id.getId}, ${stage.description}")
              val prefix = if (stage.id.getId == s"stage-${CommonStage}") {
                workflow.name
              } else {
                s"${workflow.name}.${stage.description}"
              }
              val inputsWithDefaults = stage.inputs.zipWithIndex.map {
                case (stageInput, idx) =>
                  val param = callee.inputVars(idx)
                  val fqn = s"${prefix}.${param.name}"
                  defaultsExactlyOnce.get(fqn) match {
                    case None =>
                      stageInput
                    case Some(default: JsValue) =>
                      StaticInput(wdlInputToIr(param.dxType, default))
                  }
              }
              stage.copy(inputs = inputsWithDefaults)
            }
            workflow.copy(stages = stagesWithDefaults)
          }
        // check that the stage order hasn't changed
        val allStageNames = workflow.stages.map(_.id)
        val embedAllStageNames = workflowWithDefaults.stages.map(_.id)
        assert(allStageNames == embedAllStageNames)
        name -> workflowWithDefaults
      case other =>
        throw new Exception(s"Unexpected callable ${other}")
    }
    val primaryCallableWithDefaults =
      rawBundle.primaryCallable.map(primary => allCallablesWithDefaults(primary.name))
    defaultsExactlyOnce.checkAllUsed()
    rawBundle.copy(primaryCallable = primaryCallableWithDefaults,
                   allCallables = allCallablesWithDefaults)
  }

  override lazy val bundle: Bundle = {
    if (wdlJsDefaults.isEmpty) {
      rawBundle
    } else {
      bundleWithDefaults
    }
  }

  private lazy val parameterLinkSerializer = ParameterLinkSerializer(fileResolverWithCache, dxApi)

  case class WdlInputFile(fields: Map[String, JsValue])
      extends InputFile(fields, parameterLinkSerializer, logger) {
    override protected def translateInput(parameter: Parameter, jsv: JsValue): Value = {
      wdlInputToIr(parameter.dxType, jsv)
    }
  }

  override lazy val inputFiles: Map[Path, InputFile] = {
    wdlJsInputs.map {
      case (path, inputs) => path -> WdlInputFile(inputs)
    }
  }
}

case class WdlTranslatorFactory(regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                                dxApi: DxApi = DxApi.get,
                                logger: Logger = Logger.get)
    extends DocumentTranslatorFactory {
  override def create(sourceFile: Path,
                      language: Option[Language],
                      inputs: Vector[Path],
                      defaults: Option[Path],
                      locked: Boolean,
                      defaultRuntimeAttrs: Map[String, Value],
                      reorgAttrs: ReorgAttributes,
                      project: DxProject,
                      fileResolver: FileSourceResolver): Option[WdlDocumentTranslator] = {
    val (doc, typeAliases) =
      try {
        WdlUtils.parseSourceFile(sourceFile, fileResolver, regime, logger)
      } catch {
        case _: Throwable =>
          return None
      }
    if (!language.forall(Language.toWdlVersion(_) == doc.version.value)) {
      throw new Exception(s"WDL document ${sourceFile} is not version ${language.get}")
    }
    Some(
        WdlDocumentTranslator(doc,
                              typeAliases,
                              inputs,
                              defaults,
                              locked,
                              defaultRuntimeAttrs,
                              reorgAttrs,
                              project,
                              fileResolver,
                              dxApi,
                              logger)
    )
  }
}
