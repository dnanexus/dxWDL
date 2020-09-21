package dx.translator

import java.nio.file.{Path, Paths}

import dx.api.{DxApi, DxFile, DxProject}
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache}
import dx.core.ir.Type._
import dx.core.ir.{
  Application,
  Bundle,
  Callable,
  ParameterLinkDeserializer,
  ParameterLinkSerializer,
  StaticInput,
  Type,
  Value,
  Workflow
}
import spray.json.{JsArray, JsNull, JsObject, JsString, JsValue}
import wdlTools.util.{FileSourceResolver, FileUtils, JsUtils, Logger}

/**
  * Tracks which keys are accessed in a map and ensures all keys are accessed exactly once.
  */
case class ExactlyOnce(name: String, fields: Map[String, JsValue], logger: Logger) {
  private var retrievedKeys: Set[String] = Set.empty

  def get(fqn: String): Option[JsValue] = {
    fields.get(fqn) match {
      case None =>
        logger.trace(s"getExactlyOnce ${fqn} => None")
        None
      case Some(v: JsValue) if retrievedKeys.contains(fqn) =>
        logger.trace(
            s"getExactlyOnce ${fqn} => Some(${v}); value already retrieved so returning None"
        )
        None
      case Some(v: JsValue) =>
        logger.trace(s"getExactlyOnce ${fqn} => Some(${v})")
        retrievedKeys += fqn
        Some(v)
    }
  }

  def checkAllUsed(): Unit = {
    val unused = fields.keySet -- retrievedKeys
    if (unused.nonEmpty) {
      throw new Exception(s"Could not map all ${name} fields. These were left: ${unused}")
    }
  }
}

object InputTranslator {
  def loadJsonFileWithComments(path: Path): Map[String, JsValue] = {
    // skip comment lines, which start with ##
    JsUtils.getFields(JsUtils.jsFromFile(path)).view.filterKeys(!_.startsWith("##")).toMap
  }
}

abstract class InputTranslator(bundle: Bundle,
                               inputs: Vector[Path],
                               defaults: Option[Path],
                               project: DxProject,
                               baseFileResolver: FileSourceResolver = FileSourceResolver.get,
                               dxApi: DxApi = DxApi.get,
                               logger: Logger = Logger.get) {

  private lazy val inputsJs: Map[Path, Map[String, JsValue]] =
    inputs.map(path => path -> InputTranslator.loadJsonFileWithComments(path)).toMap
  private lazy val defaultsJs =
    defaults.map(InputTranslator.loadJsonFileWithComments).getOrElse(Map.empty)

  /**
    * Overridable function that converts a language-specific JSON value to one that can be
    * deserialized to an IR Value. This is the mechanism to support compile-time inputs/
    * defaults that do not map implicitly to an IR type.
    * @return
    */
  protected def translateJsInput(jsv: JsValue, t: Type): JsValue

  /**
    * Extract Dx files from a JSON input.
    * Can be over-ridden to extract files in a language-specific way.
    * @param t the parameter type
    * @param jsv the JSON value
    * @return a Vector of JSON values that describe Dx files
    */
  private def extractDxFiles(jsv: JsValue, t: Type): Vector[JsValue] = {
    val updatedValue = translateJsInput(jsv, t)
    (t, updatedValue) match {
      case (TOptional(_), JsNull)   => Vector.empty
      case (TOptional(inner), _)    => extractDxFiles(updatedValue, inner)
      case (TFile, fileValue)       => Vector(fileValue)
      case _ if Type.isPrimitive(t) => Vector.empty
      case (TArray(elementType, _), JsArray(array)) =>
        array.flatMap(element => extractDxFiles(element, elementType))
      case (TSchema(name, members), JsObject(fields)) =>
        members.flatMap {
          case (memberName, memberType) =>
            fields.get(memberName) match {
              case Some(memberValue) =>
                extractDxFiles(memberValue, memberType)
              case None if Type.isOptional(memberType) =>
                Vector.empty
              case _ =>
                throw new Exception(s"missing value for struct ${name} member ${memberName}")
            }
        }.toVector
      case (THash, JsObject(_)) =>
        // anonymous objects will never result in file-typed members, so just skip these
        Vector.empty
      case _ =>
        throw new Exception(s"value ${updatedValue} cannot be deserialized to ${t}")
    }
  }

  // scan all inputs and defaults and extract files so we can resolve them in bulk
  lazy val dxFiles: Vector[DxFile] = {
    val allInputs: Map[String, JsValue] = defaultsJs ++ inputsJs.values.flatten
    val fileJs = bundle.allCallables.values.toVector.flatMap { callable: Callable =>
      callable.inputVars.flatMap { param =>
        val fqn = s"${callable.name}.${param.name}"
        allInputs
          .get(fqn)
          .map(jsv => extractDxFiles(jsv, param.dxType))
          .getOrElse(Vector.empty)
      }
    }
    val (dxFiles, dxPaths) = fileJs.foldLeft((Vector.empty[DxFile], Vector.empty[String])) {
      case ((files, paths), obj: JsObject) =>
        (files :+ DxFile.fromJson(dxApi, obj), paths)
      case ((files, paths), JsString(path)) =>
        (files, paths :+ path)
      case (_, other) =>
        throw new Exception(s"invalid file ${other}")
    }
    val resolvedPaths = dxApi
      .resolveBulk(dxPaths, project)
      .map {
        case (key, dxFile: DxFile) => key -> dxFile
        case (_, dxobj) =>
          throw new Exception(s"Scanning the input file produced ${dxobj} which is not a file")
      }
    // lookup platform files in bulk
    dxApi.fileBulkDescribe(dxFiles ++ resolvedPaths.values)
  }

  lazy val dxFileDescCache: DxFileDescCache = DxFileDescCache(dxFiles)
  lazy val fileResolver: FileSourceResolver = {
    val dxProtocol = DxFileAccessProtocol(dxFileCache = dxFileDescCache)
    baseFileResolver.replaceProtocol[DxFileAccessProtocol](dxProtocol)
  }

  private lazy val parameterLinkSerializer = ParameterLinkSerializer(fileResolver, dxApi)
  private lazy val parameterLinkDeserializer = ParameterLinkDeserializer(dxFileDescCache, dxApi)

  lazy val bundleWithDefaults: Bundle = {
    logger.trace(s"Embedding defaults into the IR")
    val defaultsExactlyOnce = ExactlyOnce("default", defaultsJs, logger)
    val allCallablesWithDefaults: Map[String, Callable] = bundle.allCallables.map {
      case (name, applet: Application) =>
        val inputsWithDefaults = applet.inputs.map { param =>
          val fqn = s"${applet.name}.${param.name}"
          defaultsExactlyOnce.get(fqn) match {
            case None => param
            case Some(default: JsValue) =>
              val irValue =
                parameterLinkDeserializer.deserializeInputWithType(default,
                                                                   param.dxType,
                                                                   Some(translateJsInput))
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
                    val irValue =
                      parameterLinkDeserializer.deserializeInputWithType(default,
                                                                         param.dxType,
                                                                         Some(translateJsInput))
                    StaticInput(irValue)
                }
                (param, stageInputWithDefault)
            }
            workflow.copy(inputs = inputsWithDefaults)
          } else {
            // Workflow is unlocked, we don't have workflow-level inputs.
            // Instead, set the defaults in the COMMON stage.
            val stagesWithDefaults = workflow.stages.map { stage =>
              val callee: Callable = bundle.allCallables(stage.calleeName)
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
                      val irValue =
                        parameterLinkDeserializer.deserializeInputWithType(default,
                                                                           param.dxType,
                                                                           Some(translateJsInput))
                      StaticInput(irValue)
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
      bundle.primaryCallable.map(primary => allCallablesWithDefaults(primary.name))
    defaultsExactlyOnce.checkAllUsed()
    bundle.copy(primaryCallable = primaryCallableWithDefaults,
                allCallables = allCallablesWithDefaults)

  }

  private lazy val tasks: Vector[Application] = bundle.allCallables.collect {
    case (_, callable: Application) => callable
  }.toVector

  private def translateJsonInputs(fields: Map[String, JsValue]): Map[String, (Type, Value)] = {
    val fieldsExactlyOnce = ExactlyOnce("input", fields, logger)

    // If variable fully qualified name [fqn] was provided in the
    // input file, set [stage.cvar] to its JSON value
    def checkAndBindCallableInputs(
        callable: Callable,
        dxPrefix: Option[String] = None,
        fqnPrefix: Option[String] = None
    ): Map[String, (Type, Value)] = {
      callable.inputVars.flatMap { parameter =>
        val name = parameter.name
        val fqn = s"${fqnPrefix.getOrElse(callable.name)}.${name}"
        val dxName = dxPrefix.map(p => s"${p}.${name}").getOrElse(name)
        fieldsExactlyOnce.get(fqn) match {
          case None =>
            Map.empty
          case Some(value) =>
            // Do not assign the value to any later stages.
            // We found the variable declaration, the others
            // are variable uses.
            logger.trace(s"checkAndBind, found: ${fqn} -> dxName")
            val irValue =
              parameterLinkDeserializer.deserializeInputWithType(value,
                                                                 parameter.dxType,
                                                                 Some(translateJsInput))
            Map(dxName -> (parameter.dxType, irValue))
        }
      }.toMap
    }

    val inputs: Map[String, (Type, Value)] = bundle.primaryCallable match {
      // File with WDL tasks only, no workflows
      case None if tasks.isEmpty =>
        Map.empty
      case None if tasks.size > 1 =>
        throw new Exception(s"Cannot generate one input file for ${tasks.size} tasks")
      case None =>
        checkAndBindCallableInputs(tasks.head)
      case Some(task: Application) =>
        checkAndBindCallableInputs(task)
      case Some(wf: Workflow) if wf.locked =>
        // Locked workflow. A user can set workflow level
        // inputs; nothing else.
        checkAndBindCallableInputs(wf)
      case Some(wf: Workflow) if wf.stages.isEmpty =>
        // edge case: workflow, with zero stages
        Map.empty
      case Some(wf: Workflow) =>
        // unlocked workflow with at least one stage.
        // Workflow inputs go into the common stage
        val commonStage = wf.stages.head.id.getId
        val commonInputs = checkAndBindCallableInputs(wf, Some(commonStage))
        // filter out auxiliary stages
        val auxStages =
          Set(s"stage-${CommonStage}", s"stage-${OutputSection}", s"stage-${ReorgStage}")
        val middleStages = wf.stages.filterNot(stg => auxStages.contains(stg.id.getId))
        // Inputs for top level calls
        val middleInputs = middleStages.flatMap { stage =>
          // Find the input definitions for the stage, by locating the callee
          val callee: Callable = bundle.allCallables.get(stage.calleeName) match {
            case None =>
              throw new Exception(s"callable ${stage.calleeName} is missing")
            case Some(x) => x
          }
          checkAndBindCallableInputs(callee,
                                     Some(stage.description),
                                     Some(s"${wf.name}.${stage.description}"))
        }
        commonInputs ++ middleInputs
      case other =>
        throw new Exception(s"Unknown case ${other.getClass}")
    }
    fieldsExactlyOnce.checkAllUsed()
    inputs
  }

  /**
    * Build a dx input file, based on the raw input file and the Bundle.
    * The general idea is to figure out the ancestry of each app(let)/call/workflow
    * input. This provides the fully-qualified-name (fqn) of each IR variable. Then
    * we check if the fqn is defined in the input file.
    * @return
    */
  lazy val translatedInputs: Map[Path, Map[String, (Type, Value)]] = {
    inputsJs.map {
      case (path, inputs) =>
        logger.trace(s"Translating input file ${path}")
        path -> translateJsonInputs(inputs)
    }
  }

  lazy val translatedInputFields: Map[Path, Map[String, JsValue]] = {
    translatedInputs.view.mapValues { inputs =>
      inputs.flatMap {
        case (name, (t, v)) =>
          parameterLinkSerializer.createFields(name, t, v, encodeDots = false)
      }
    }.toMap
  }

  def writeTranslatedInputs(): Unit = {
    translatedInputFields.foreach {
      case (path, inputs) if inputs.nonEmpty =>
        val fileName = FileUtils.replaceFileSuffix(path, ".dx.json")
        val dxInputFile = path.getParent match {
          case null   => Paths.get(fileName)
          case parent => parent.resolve(fileName)
        }
        logger.trace(s"Writing DNAnexus JSON input file ${dxInputFile}")
        FileUtils.writeFileContent(dxInputFile, JsObject(inputs).prettyPrint)
    }
  }
}
