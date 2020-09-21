package dx.translator.wdl

import java.nio.file.Path

import dx.api.{DxApi, DxProject}
import dx.core.ir.Type.TSchema
import dx.core.ir._
import dx.core.languages.Language
import dx.core.languages.Language.Language
import dx.core.languages.wdl.{Utils => WdlUtils}
import dx.translator.{InputTranslator, ReorgAttributes, Translator, TranslatorFactory}
import spray.json.{JsArray, JsObject, JsString, JsValue}
import wdlTools.types.{TypeCheckingRegime, WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.util.{FileSourceResolver, Logger}

case class WdlInputTranslator(bundle: Bundle,
                              inputs: Vector[Path],
                              defaults: Option[Path],
                              project: DxProject,
                              baseFileResolver: FileSourceResolver = FileSourceResolver.get,
                              dxApi: DxApi = DxApi.get,
                              logger: Logger = Logger.get)
    extends InputTranslator(bundle, inputs, defaults, project, baseFileResolver, dxApi, logger) {

  /**
    * Overridable function converts a language-specific JSON value to one that can be
    * deserialized to an IR Value.
    *
    * @return
    */
  override protected def translateJsInput(jsv: JsValue, t: Type): JsValue = {
    (t, jsv) match {
      case (pairType: TSchema, JsArray(pair))
          if WdlUtils.isPairSchema(pairType) && pair.size == 2 =>
        // pair represented as [left, right]
        JsObject(WdlUtils.PairLeftReserved -> pair(0), WdlUtils.PairRightReserved -> pair(1))
      case (pairType: TSchema, JsObject(fields))
          if WdlUtils.isPairSchema(pairType) && WdlUtils.isUserPairValue(fields) =>
        JsObject(WdlUtils.PairLeftReserved -> fields(WdlUtils.PairLeftUser),
                 WdlUtils.PairRightReserved -> fields(WdlUtils.PairRightUser))
      case (mapType: TSchema, JsObject(fields))
          if WdlUtils.isMapSchema(mapType) && WdlUtils.isUserMapValue(fields) =>
        // map represented using keys with the the '___' suffix
        JsObject(WdlUtils.MapKeysReserved -> fields(WdlUtils.MapKeysUser),
                 WdlUtils.MapValuesReserved -> fields(WdlUtils.MapValuesUser))
      case (mapType: TSchema, JsObject(fields))
          if WdlUtils.isMapSchema(mapType) && !WdlUtils.isReservedMapValue(fields) =>
        // map represented as a JSON object (i.e. has keys that are coercible from String)
        JsObject(WdlUtils.MapKeysReserved -> JsArray(fields.keys.map(JsString(_)).toVector),
                 WdlUtils.MapValuesReserved -> JsArray(fields.values.toVector))
      case _ =>
        jsv
    }
  }
}

/**
  * Compiles WDL to IR.
  * TODO: remove limitation that two callables cannot have the same name
  * TODO: rewrite sortByDependencies using a graph data structure
  */
case class WdlTranslator(doc: TAT.Document,
                         typeAliases: Map[String, WdlTypes.T_Struct],
                         locked: Boolean,
                         defaultRuntimeAttrs: Map[String, Value],
                         reorgAttrs: ReorgAttributes,
                         fileResolver: FileSourceResolver = FileSourceResolver.get,
                         dxApi: DxApi = DxApi.get,
                         logger: Logger = Logger.get)
    extends Translator {

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

  override lazy val apply: Bundle = {
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
    val irTypeAliases = typeAliases.map {
      case (name, struct: WdlTypes.T_Struct) => name -> WdlUtils.toIRType(struct)
    }
    Bundle(primaryCallable, allCallables, allCallablesSortedNames, irTypeAliases)
  }

  override def translateInputs(bundle: Bundle,
                               inputs: Vector[Path],
                               defaults: Option[Path],
                               project: DxProject): (Bundle, FileSourceResolver) = {
    val inputTranslator = WdlInputTranslator(bundle, inputs, defaults, project, fileResolver)
    inputTranslator.writeTranslatedInputs()
    (inputTranslator.bundleWithDefaults, inputTranslator.fileResolver)
  }
}

case class WdlTranslatorFactory(regime: TypeCheckingRegime = TypeCheckingRegime.Moderate)
    extends TranslatorFactory {
  override def create(sourceFile: Path,
                      language: Option[Language],
                      locked: Boolean,
                      defaultRuntimeAttrs: Map[String, Value],
                      reorgAttrs: ReorgAttributes,
                      fileResolver: FileSourceResolver,
                      dxApi: DxApi = DxApi.get,
                      logger: Logger = Logger.get): Option[WdlTranslator] = {
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
        WdlTranslator(doc,
                      typeAliases.bindings,
                      locked,
                      defaultRuntimeAttrs,
                      reorgAttrs,
                      fileResolver,
                      dxApi,
                      logger)
    )
  }
}
