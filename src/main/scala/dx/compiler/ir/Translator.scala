package dx.compiler.ir

import java.nio.file.{Path, Paths}

import dx.api.{DxApi, DxFile, DxProject}
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache}
import dx.core.ir.{Application, Bundle, Callable, Parameter, Workflow}
import spray.json._
import wdlTools.util.{FileSourceResolver, FileUtils, JsUtils, Logger}

/**
  * Base class for Translators, which convert from language-specific AST into IR.
  * @param dxApi DxApi
  * @param logger Logger
  */
abstract class Translator(dxApi: DxApi = DxApi.get, logger: Logger = Logger.get) {
  private class Fields(name: String, fields: Map[String, JsValue]) {
    private var retrievedKeys: Set[String] = Set.empty

    def getExactlyOnce(fqn: String): Option[JsValue] = {
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
        throw new Exception(s"""|Could not map all ${name} fields.
                                |These were left: ${unused}""".stripMargin)

      }
    }
  }

  /**
    * Given an input field value, translate it to the DNAnexus input field(s).
    * @param parameter input parameter
    * @param jsv input JsValue
    * @param dxName DNAnexus field name
    * @param encodeName whether to convert dots in the field name
    * @return
    */
  protected def translateInput(parameter: Parameter,
                               jsv: JsValue,
                               dxName: String,
                               fileResolver: FileSourceResolver,
                               encodeName: Boolean): Map[String, JsValue]

  private case class InputFile(fields: Map[String, JsValue], fileResolver: FileSourceResolver) {
    private val inputFields: Fields = new Fields("input", fields)
    private var dxFields: Map[String, JsValue] = Map.empty

    // If WDL variable fully qualified name [fqn] was provided in the
    // input file, set [stage.cvar] to its JSON value
    def checkAndBind(fqn: String, dxName: String, parameter: Parameter): Unit = {
      inputFields.getExactlyOnce(fqn) match {
        case None      => ()
        case Some(jsv) =>
          // Do not assign the value to any later stages.
          // We found the variable declaration, the others
          // are variable uses.
          logger.trace(s"checkAndBind, found: ${fqn} -> ${dxName}")
          dxFields ++= translateInput(parameter, jsv, dxName, fileResolver, encodeName = false)
      }
    }

    // Check if all the input fields were actually used. Otherwise, there are some
    // key/value pairs that were not translated to DNAx.
    def toJsObject: JsObject = {
      inputFields.checkAllUsed()
      JsObject(dxFields)
    }
  }

  // skip comment lines, these start with ##.
  private def removeCommentFields(inputs: Map[String, JsValue]): Map[String, JsValue] = {
    inputs.foldLeft(Map.empty[String, JsValue]) {
      case (accu, (k, v)) if !k.startsWith("##") => accu + (k -> v)
    }
  }

  // Build a dx input file, based on the JSON input file and the workflow
  //
  // The general idea here is to figure out the ancestry of each
  // app(let)/call/workflow input. This provides the fully-qualified-name (fqn)
  // of each IR variable. Then we check if the fqn is defined in
  // the input file.
  private def dxFromInputJson(bundle: Bundle,
                              inputs: Map[String, JsValue],
                              fileResolver: FileSourceResolver): JsObject = {
    val inputFields = InputFile(inputs, fileResolver)

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
        val auxStages = Set(s"stage-${CommonStage}", s"stage-${OutputSection}", s"stage-${Reorg}")
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

  /**
    * Translate a document in a supported workflow language to a Bundle.
    * @param source the source file.
    * @return
    */
  protected def translateDocument(source: Path,
                                  locked: Boolean,
                                  reorgEnabled: Option[Boolean],
                                  fileResolver: FileSourceResolver): Bundle

  /**
    * Given a mapping of field name to value, selects just the file-type fields
    * and returns them as DxFiles.
    * @param fields all input + default fields
    * @return tuple of (pathToFile, allFiles), where pathToFile is a mapping of the
    *         original path value to the resolved DxFile, and allFiles is all of the
    *         input + default DxFiles.
    */
  protected def filterFiles(fields: Map[String, JsValue],
                            dxProject: DxProject): (Map[String, DxFile], Vector[DxFile])

  /**
    * Updates `bundle` with default values.
    * @param bundle The raw bundle
    * @param fileResolver FileSourceResolver
    * @param pathToDxFile mapping of original paths to DNAnexus paths
    * @param dxFileDescCache cache of DxFileDescribe
    * @param defaults default values
    * @return
    */
  protected def embedDefaults(bundle: Bundle,
                              fileResolver: FileSourceResolver,
                              pathToDxFile: Map[String, DxFile],
                              dxFileDescCache: DxFileDescCache,
                              defaults: Map[String, JsValue]): Bundle

  def apply(source: Path,
            dxProject: DxProject,
            inputs: Vector[Path] = Vector.empty,
            defaults: Option[Path] = None,
            locked: Boolean = false,
            reorgEnabled: Option[Boolean] = None,
            writeDxInputsFile: Boolean = true,
            fileResolver: FileSourceResolver = FileSourceResolver.get): Bundle = {
    val sourceAbsPath = FileUtils.absolutePath(source)
    val sourceFileResolver = fileResolver.addToLocalSearchPath(Vector(sourceAbsPath.getParent))
    // generate IR
    val bundle: Bundle = translateDocument(source, locked, reorgEnabled, sourceFileResolver)
    val jsDefaults =
      defaults
        .map(path => removeCommentFields(JsUtils.getFields(JsUtils.jsFromFile(path))))
        .getOrElse(Map.empty)
    // exit early if there are neither any defaults nor any inputs to translate
    if (jsDefaults.isEmpty && !writeDxInputsFile) {
      return bundle
    }
    // read inputs as JSON
    val jsInputs = inputs
      .map(path => path -> removeCommentFields(JsUtils.getFields(JsUtils.jsFromFile(path))))
      .toMap
    // Scan the JSON inputs files for dx:files, and batch describe them. This
    // reduces the number of API calls.
    val inputAndDefaultFields = (jsInputs.values.flatten ++ jsDefaults).toMap
    val (pathToDxFile, dxFiles) = filterFiles(inputAndDefaultFields, dxProject)
    // lookup platform files in bulk
    val allFiles = dxApi.fileBulkDescribe(dxFiles)
    val dxFileDescCache = DxFileDescCache(allFiles)
    // update bundle with default values
    val dxProtocol = DxFileAccessProtocol(dxApi, dxFileDescCache)
    val fileResolverWithCache = sourceFileResolver.replaceProtocol[DxFileAccessProtocol](dxProtocol)
    val finalBundle = if (jsDefaults.isEmpty) {
      bundle
    } else {
      embedDefaults(
          bundle,
          fileResolverWithCache,
          pathToDxFile,
          dxFileDescCache,
          jsDefaults
      )
    }
    // generate DNAnexus input files if requested
    if (writeDxInputsFile) {
      jsInputs.foreach {
        case (path, jsValues) =>
          logger.trace(s"Translating WDL input file ${path}")
          val dxInputs = dxFromInputJson(finalBundle, jsValues, fileResolverWithCache)
          val fileName = FileUtils.replaceFileSuffix(path, ".dx.json")
          val dxInputFile = path.getParent match {
            case null   => Paths.get(fileName)
            case parent => parent.resolve(fileName)
          }
          logger.trace(s"Writing DNAnexus JSON input file ${dxInputFile}")
          FileUtils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
      }
    }
    finalBundle
  }
}
