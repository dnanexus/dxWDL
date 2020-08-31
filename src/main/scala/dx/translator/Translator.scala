package dx.translator

import java.nio.file.{Path, Paths}

import dx.api.{DxApi, DxFile, DxProject}
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache}
import dx.core.ir.Type.{TArray, TFile, THash, TMap, TOptional, TSchema}
import dx.core.ir._
import dx.core.languages.Language.Language
import dx.translator.wdl.WdlTranslatorFactory
import spray.json._
import wdlTools.util.{FileSourceResolver, FileUtils, JsUtils, Logger}

/**
  * Base class for Translators, which convert from language-specific AST into IR.
  * @param dxApi DxApi
  * @param logger Logger
  */
case class Translator(extras: Option[Extras] = None,
                      baseFileResolver: FileSourceResolver = FileSourceResolver.get,
                      dxApi: DxApi = DxApi.get,
                      logger: Logger = Logger.get) {

  private val translatorFactories = Vector(
      WdlTranslatorFactory(dxApi = dxApi, logger = logger)
  )

  /**
    * Build a dx input file, based on the JSON input file and the workflow.
    *
    * The general idea is to figure out the ancestry of each
    * app(let)/call/workflow input. This provides the fully-qualified-name (fqn)
    * of each IR variable. Then we check if the fqn is defined in the input file.
    * @param inputFields input fields to translate
    * @param bundle IR bundle
    * @return
    */
  private def translateInputFile(inputFields: InputFile, bundle: Bundle): JsObject = {
    def handleTask(application: Application): Unit = {
      application.inputs.foreach { parameter =>
        val fqn = s"${application.name}.${parameter.name}"
        val dxName = s"${parameter.name}"
        inputFields.checkAndBind(fqn, dxName, parameter)
      }
    }

    // If there is one task, we can generate one input file for it.
    val tasks: Vector[Application] = bundle.allCallables.collect {
      case (_, callable: Application) => callable
    }.toVector

    bundle.primaryCallable match {
      // File with WDL tasks only, no workflows
      case None if tasks.isEmpty =>
        ()
      case None if tasks.size == 1 =>
        handleTask(tasks.head)
      case None =>
        throw new Exception(s"Cannot generate one input file for ${tasks.size} tasks")
      case Some(task: Application) =>
        handleTask(task)

      case Some(wf: Workflow) if wf.locked =>
        // Locked workflow. A user can set workflow level
        // inputs; nothing else.
        wf.inputs.foreach {
          case (parameter, _) =>
            val fqn = s"${wf.name}.${parameter.name}"
            val dxName = s"${parameter.name}"
            inputFields.checkAndBind(fqn, dxName, parameter)
        }
      case Some(wf: Workflow) if wf.stages.isEmpty =>
        // edge case: workflow, with zero stages
        ()

      case Some(wf: Workflow) =>
        // unlocked workflow with at least one stage.
        // Workflow inputs go into the common stage
        val commonStage = wf.stages.head.id.getId
        wf.inputs.foreach {
          case (parameter, _) =>
            val fqn = s"${wf.name}.${parameter.name}"
            val dxName = s"${commonStage}.${parameter.name}"
            inputFields.checkAndBind(fqn, dxName, parameter)
        }

        // filter out auxiliary stages
        val auxStages =
          Set(s"stage-${CommonStage}", s"stage-${OutputSection}", s"stage-${ReorgStage}")
        val middleStages = wf.stages.filter { stg =>
          !(auxStages contains stg.id.getId)
        }

        // Inputs for top level calls
        middleStages.foreach { stage =>
          // Find the input definitions for the stage, by locating the callee
          val callee: Callable = bundle.allCallables.get(stage.calleeName) match {
            case None =>
              throw new Exception(s"callable ${stage.calleeName} is missing")
            case Some(x) => x
          }
          callee.inputVars.foreach { cVar =>
            val fqn = s"${wf.name}.${stage.description}.${cVar.name}"
            val dxName = s"${stage.description}.${cVar.name}"
            inputFields.checkAndBind(fqn, dxName, cVar)
          }
        }

      case other =>
        throw new Exception(s"Unknown case ${other.getClass}")
    }

    inputFields.toJsObject
  }

  // skip comment lines, these start with ##.
  private def removeCommentFields(inputs: Map[String, JsValue]): Map[String, JsValue] = {
    inputs.foldLeft(Map.empty[String, JsValue]) {
      case (accu, (k, v)) if !k.startsWith("##") => accu + (k -> v)
    }
  }

  def apply(source: Path,
            project: DxProject,
            language: Option[Language] = None,
            inputs: Vector[Path] = Vector.empty,
            defaults: Option[Path] = None,
            locked: Boolean = false,
            reorgEnabled: Option[Boolean] = None,
            writeDxInputsFile: Boolean = true): Bundle = {
    val sourceAbsPath = FileUtils.absolutePath(source)
    val fileResolver = baseFileResolver.addToLocalSearchPath(Vector(sourceAbsPath.getParent))
    val translator = translatorFactories
      .collectFirst { factory =>
        factory.create(language, extras, fileResolver) match {
          case Some(translator) => translator
        }
      }
      .getOrElse(
          throw new Exception(s"Language ${language} is not supported")
      )

    language match {
      case Some(lang) => getTranslator(lang, fileResolver)
      case None       => getTranslator(sourceAbsPath, fileResolver)
    }
    // only process inputs if they are needed
    val jsDefaults =
      defaults
        .map(path => removeCommentFields(JsUtils.getFields(JsUtils.jsFromFile(path))))
        .getOrElse(Map.empty)
    if (jsDefaults.nonEmpty || writeDxInputsFile) {
      // read inputs as JSON
      val jsInputs = inputs
        .map(path => path -> removeCommentFields(JsUtils.getFields(JsUtils.jsFromFile(path))))
        .toMap
      val (bundle, inputFiles) = translator.translateDocumentWithDefaults(
          sourceAbsPath,
          locked,
          jsDefaults,
          jsInputs,
          reorgEnabled
      )
      // write DNAnexus input files if requested
      if (writeDxInputsFile) {
        inputFiles.foreach {
          case (path, inputFile) =>
            logger.trace(s"Translating input file ${path}")
            val dxInputs = translateInputFile(inputFile, bundle)
            val fileName = FileUtils.replaceFileSuffix(path, ".dx.json")
            val dxInputFile = path.getParent match {
              case null   => Paths.get(fileName)
              case parent => parent.resolve(fileName)
            }
            logger.trace(s"Writing DNAnexus JSON input file ${dxInputFile}")
            FileUtils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
        }
      }
      bundle
    } else {
      translator.translateDocument(source, locked, reorgEnabled)
    }
  }
}
