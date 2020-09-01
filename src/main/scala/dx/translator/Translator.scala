package dx.translator

import java.nio.file.{Path, Paths}

import dx.api.{DxApi, DxProject}
import dx.core.ir._
import dx.core.languages.Language.Language
import dx.translator.wdl.WdlTranslatorFactory
import spray.json._
import wdlTools.util.{FileSourceResolver, FileUtils, Logger}

/**
  * Base class for Translators, which convert from language-specific AST into IR.
  * @param dxApi DxApi
  * @param logger Logger
  */
case class Translator(extras: Option[Extras] = None,
                      baseFileResolver: FileSourceResolver = FileSourceResolver.get,
                      dxApi: DxApi = DxApi.get,
                      logger: Logger = Logger.get) {

  private val translatorFactories: Vector[DocumentTranslatorFactory] = Vector(
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

    inputFields.serialize
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
    // load defaults from extras
    val defaultRuntimeAttrs = extras.map(_.defaultRuntimeAttributes).getOrElse(Map.empty)
    val reorgAttrs = (extras.flatMap(_.customReorgAttributes), reorgEnabled) match {
      case (Some(attr), None)    => attr
      case (Some(attr), Some(b)) => attr.copy(enabled = b)
      case (None, Some(b))       => ReorgAttributes(enabled = b)
      case (None, None)          => ReorgAttributes(enabled = false)
    }
    val docTranslator = translatorFactories
      .collectFirst { factory =>
        factory.create(sourceAbsPath,
                       language,
                       inputs,
                       defaults,
                       locked,
                       defaultRuntimeAttrs,
                       reorgAttrs,
                       project,
                       fileResolver) match {
          case Some(translator) => translator
        }
      }
      .getOrElse(
          language match {
            case Some(lang) =>
              throw new Exception(s"Language ${lang} is not supported")
            case None =>
              throw new Exception(s"Cannot determine language/version from source file ${source}")
          }
      )
    val bundle = docTranslator.bundle
    // write DNAnexus input files if requesteds
    if (writeDxInputsFile && inputs.nonEmpty) {
      docTranslator.inputFiles.foreach {
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
  }
}
