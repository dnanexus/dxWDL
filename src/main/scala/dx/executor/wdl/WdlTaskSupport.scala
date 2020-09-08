package dx.executor.wdl

import java.nio.file.{Files, Path}

import dx.api.{DxJob, DxPath}
import dx.core.io.{
  DxFileAccessProtocol,
  DxFileSource,
  DxWorkerPaths,
  DxdaManifest,
  DxdaManifestBuilder,
  DxfuseManifest,
  DxfuseManifestBuilder
}
import dx.core.ir.ParameterLink
import dx.core.languages.wdl.{DxMetaHints, Runtime, Utils => WdlUtils}
import dx.executor.{FileUploader, JobMeta, TaskSupport, TaskSupportFactory}
import dx.translator.wdl.IrToWdlValueBindings
import spray.json._
import wdlTools.eval.WdlValues._
import wdlTools.eval.{
  Eval,
  EvalPaths,
  Hints,
  Meta,
  WdlValueBindings,
  WdlValueSerde,
  WdlValues,
  Utils => WdlValueUtils
}
import wdlTools.exec.{
  ExecPaths,
  SafeLocalizationDisambiguator,
  TaskCommandFileGenerator,
  TaskInputOutput
}
import wdlTools.syntax.WdlVersion
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT, Utils => WdlTypeUtils}
import wdlTools.util.{
  DefaultBindings,
  FileSource,
  LocalFileSource,
  Logger,
  RealFileSource,
  TraceLevel
}

case class WdlTaskSupport(task: TAT.Task,
                          wdlVersion: WdlVersion,
                          typeAliases: DefaultBindings[WdlTypes.T_Struct],
                          jobMeta: JobMeta,
                          workerPaths: DxWorkerPaths,
                          fileUploader: FileUploader)
    extends TaskSupport {

  private val dxFileDescCache = jobMeta.dxFileDescCache
  private val fileResolver = jobMeta.fileResolver
  private val dxApi = jobMeta.dxApi
  private val logger = jobMeta.logger

  private lazy val evaluator = Eval(
      EvalPaths(workerPaths.homeDir, workerPaths.tmpDir),
      Some(wdlVersion),
      jobMeta.fileResolver,
      Logger.Quiet
  )
  private lazy val taskIO = TaskInputOutput(task, logger)

  private def getInputs: Map[String, WdlValues.V] = {
    // Discard auxiliary fields
    val taskInputs = task.inputs.map(inp => inp.name -> inp).toMap
    // convert IR to WDL values
    val inputWdlValues: Map[String, WdlValues.V] = jobMeta.inputs.collect {
      case (name, value) if !name.endsWith(ParameterLink.FlatFilesSuffix) =>
        val wdlType = taskInputs(name).wdlType
        name -> WdlUtils.fromIRValue(value, wdlType, name)
    }
    // add default values for any missing inputs
    taskIO.inputsFromValues(inputWdlValues, evaluator, strict = true).bindings
  }

  private lazy val inputDefs: Map[String, TAT.InputDefinition] =
    task.inputs.map(d => d.name -> d).toMap

  private def printInputs(inputs: Map[String, WdlValues.V]): Unit = {
    if (logger.isVerbose) {
      val inputStr = task.inputs
        .map { inputDef =>
          s"${inputDef.name} -> (${inputDef.wdlType}, ${inputs.get(inputDef.name)})"
        }
        .mkString("\n")
      logger.traceLimited(s"inputs: ${inputStr}")
    }
  }

  private def evaluateDeclarations(inputs: Map[String, WdlValues.V]): Map[String, WdlValues.V] = {
    // evaluate the declarations using the inputs
    val env: Map[String, WdlValues.V] =
      task.declarations.foldLeft(inputs) {
        case (env, TAT.Declaration(name, wdlType, Some(expr), _)) =>
          val wdlValue =
            evaluator.applyExprAndCoerce(expr, wdlType, WdlValueBindings(env))
          env + (name -> wdlValue)
        case (_, TAT.Declaration(name, _, None, _)) =>
          throw new Exception(s"Declaration ${name} has no expression")
      }
    env
  }

  private def createRuntime(env: Map[String, WdlValues.V]): Runtime[IrToWdlValueBindings] = {
    Runtime(
        wdlVersion,
        task.runtime,
        task.hints,
        evaluator,
        IrToWdlValueBindings(jobMeta.defaultRuntimeAttrs),
        Some(WdlValueBindings(env))
    )
  }

  private def getRequiredInstanceType(inputs: Map[String, WdlValues.V]): String = {
    logger.traceLimited("calcInstanceType", minLevel = TraceLevel.VVerbose)
    printInputs(inputs)
    val env = evaluateDeclarations(inputs)
    val runtime = createRuntime(env)
    val request = runtime.parseInstanceType
    logger.traceLimited(s"calcInstanceType $request")
    jobMeta.instanceTypeDb.apply(request)
  }

  override lazy val getRequiredInstanceType: String = getRequiredInstanceType(getInputs)

  private def extractFiles(v: WdlValues.V): Vector[FileSource] = {
    v match {
      case V_File(s) =>
        Vector(fileResolver.resolve(s))
      case V_Optional(value) =>
        extractFiles(value)
      case V_Array(value) =>
        value.flatMap(extractFiles)
      case V_Map(m) =>
        m.flatMap {
          case (k, v) => extractFiles(k) ++ extractFiles(v)
        }.toVector
      case V_Pair(lf, rt) =>
        extractFiles(lf) ++ extractFiles(rt)
      case V_Object(m) =>
        m.values.flatMap(extractFiles).toVector
      case V_Struct(_, m) =>
        m.values.flatMap(extractFiles).toVector
      case _ =>
        Vector.empty
    }
  }

  private lazy val parameterMeta = Meta.create(wdlVersion, task.parameterMeta)

  private def serializeValues(
      values: Map[String, (WdlTypes.T, WdlValues.V)]
  ): Map[String, JsValue] = {
    values.map {
      case (name, (t, v)) =>
        val jsType = WdlUtils.serializeType(t)
        val jsValue = WdlValueSerde.serialize(v)
        name -> JsObject("type" -> jsType, "value" -> jsValue)
    }
  }

  private def deserializeValues(
      values: Map[String, JsValue]
  ): Map[String, (WdlTypes.T, WdlValues.V)] = {
    values.map {
      case (name, JsObject(fields)) =>
        val t = WdlUtils.deserializeType(fields("type"), typeAliases.bindings)
        val v = WdlValueSerde.deserialize(fields("value"))
        name -> (t, v)
    }
  }

  /**
    * Input files are represented as dx URLs (dx://proj-xxxx:file-yyyy::/A/B/C.txt)
    * instead of local files (/home/C.txt). A file may be referenced more than once,
    * but we want to download it just once.
    * @return
    */
  override def localizeInputFiles: (Map[String, JsValue],
                                    Map[FileSource, Path],
                                    Option[DxdaManifest],
                                    Option[DxfuseManifest]) = {
    assert(workerPaths.inputFilesDir != workerPaths.dxfuseMountpoint)

    val inputs = getInputs
    printInputs(inputs)

    val (localFiles, filesToStream, filesToDownload) =
      inputs.foldLeft((Set.empty[FileSource], Set.empty[FileSource], Set.empty[FileSource])) {
        case ((localFiles, filesToStream, filesToDownload), (name, value)) =>
          val (local, remote) = extractFiles(value).partition {
            case _: LocalFileSource =>
              // The file is already on the local disk, there is no need to download it.
              // TODO: make sure this file is NOT in the applet input/output directories.
              true
            case _ => false
          }
          if (remote.isEmpty) {
            (localFiles ++ local, filesToStream, filesToDownload)
          } else {
            val stream = workerPaths.streamAllFiles || (parameterMeta.get(name) match {
              case Some(V_String(DxMetaHints.ParameterMetaStream)) =>
                true
              case Some(V_Object(fields)) =>
                // This enables the stream annotation in the object form of metadata value, e.g.
                // bam_file : {
                //   stream : true
                // }
                // We also support two aliases, dx_stream and localizationOptional
                fields.view
                  .filterKeys(
                      Set(DxMetaHints.ParameterMetaStream,
                          DxMetaHints.ParameterHintStream,
                          Hints.Keys.LocalizationOptional)
                  )
                  .values
                  .exists {
                    case V_Boolean(b) => b
                    case _            => false
                  }
              case _ => false
            })
            if (stream) {
              (localFiles ++ local, filesToStream ++ remote, filesToDownload)
            } else {
              (localFiles ++ local, filesToStream, filesToDownload ++ remote)
            }
          }
      }

    val localFilesToPath = localFiles.map(fs => fs -> fs.localPath).toMap

    // build dxda and/or dxfuse manifests
    // We use a SafeLocalizationDisambiguator to determine the local path and deal
    // with file name collisions in the manner specified by the WDL spec

    dxApi.logger.traceLimited(s"downloading files = ${filesToDownload}")
    val downloadLocalizer =
      SafeLocalizationDisambiguator(workerPaths.inputFilesDir,
                                    existingPaths = localFilesToPath.values.toSet)
    val downloadFileSourceToPath: Map[FileSource, Path] =
      filesToDownload.map(fs => fs -> downloadLocalizer.getLocalPath(fs)).toMap
    val dxdaManifest = DxdaManifestBuilder(dxApi).apply(downloadFileSourceToPath.map {
      case (dxFs: DxFileSource, localPath) =>
        dxFs.dxFile.id -> (dxFs.dxFile, localPath)
    })

    dxApi.logger.traceLimited(s"streaming files = ${filesToStream}")
    val streamingLocalizer =
      SafeLocalizationDisambiguator(workerPaths.dxfuseMountpoint,
                                    existingPaths = localFilesToPath.values.toSet)
    val streamFileSourceToPath: Map[FileSource, Path] =
      filesToStream.map(fs => fs -> streamingLocalizer.getLocalPath(fs)).toMap
    val dxfuseManifest =
      DxfuseManifestBuilder(dxApi).apply(streamFileSourceToPath.map {
        case (dxFs: DxFileSource, localPath) => dxFs.dxFile -> localPath
      }, dxFileDescCache, workerPaths)

    val fileSourceToPath = localFilesToPath ++ downloadFileSourceToPath ++ streamFileSourceToPath

    val uriToPath: Map[String, String] = fileSourceToPath.map {
      case (dxFs: DxFileSource, path)     => dxFs.value -> path.toString
      case (localFs: LocalFileSource, p2) => localFs.toString -> p2.toString
      case other                          => throw new RuntimeException(s"unsupported file source ${other}")
    }

    // Replace the URIs with local file paths
    def pathTranslator(v: WdlValues.V): Option[WdlValues.V] = {
      v match {
        case WdlValues.V_File(uri) =>
          uriToPath.get(uri) match {
            case Some(localPath) => Some(WdlValues.V_File(localPath))
            case None =>
              throw new Exception(s"Did not localize file ${uri}")
          }
        case WdlValues.V_Optional(WdlValues.V_File(uri)) =>
          uriToPath.get(uri) match {
            case Some(localPath) =>
              Some(WdlValues.V_Optional(WdlValues.V_File(localPath)))
            case None =>
              Some(WdlValues.V_Null)
          }
        case _ => None
      }
    }

    val localizedInputs: Map[String, WdlValues.V] =
      inputs.view.mapValues(v => WdlValueUtils.transform(v, pathTranslator)).toMap

    // serialize the updated inputs
    val localizedInputsJs = serializeValues(localizedInputs.map {
      case (name, value) => name -> (inputDefs(name).wdlType, value)
    })

    (localizedInputsJs, fileSourceToPath, Some(dxdaManifest), Some(dxfuseManifest))
  }

  override def writeCommandScript(
      localizedInputs: Map[String, JsValue]
  ): Map[String, JsValue] = {
    val inputs = deserializeValues(localizedInputs)
    val inputValues = inputs.map {
      case (name, (_, v)) => name -> v
    }
    printInputs(inputValues)
    val inputsWithDecls = evaluateDeclarations(inputValues)
    val ctx = WdlValueBindings(inputsWithDecls)
    val command = evaluator.applyCommand(task.command, ctx) match {
      case s if s.trim.isEmpty => None
      case s                   => Some(s)
    }
    val generator = TaskCommandFileGenerator(logger)
    // the worker and container use identical directory structures
    val execPaths = ExecPaths(workerPaths.homeDir, workerPaths.tmpDir)
    val runtime = createRuntime(inputsWithDecls)
    val container = runtime.container match {
      case Vector()    => None
      case Vector(img) => Some(img, execPaths)
      case v           =>
        // For now we prefer a dx:// url, otherwise just return the first image
        // TODO: if the user provides multiple alternate images, do something
        //  useful with them, e.g. try to resolve each one and pick the first
        //  that is available.
        val img = v
          .collectFirst {
            case img if img.startsWith(DxPath.DxUriPrefix) => img
          }
          .getOrElse(v.head)
        Some(img, execPaths)
    }
    generator.apply(command, execPaths, container)
    serializeValues(ctx.bindings.map {
      case (name, value) => name -> (inputDefs(name).wdlType, value)
    })
  }

  private lazy val outputDefs: Map[String, TAT.OutputDefinition] =
    task.outputs.map(d => d.name -> d).toMap

  override def evaluateOutputs(localizedInputs: Map[String, JsValue],
                               fileSourceToPath: Map[FileSource, Path],
                               fileUploader: FileUploader): Unit = {
    val inputs = deserializeValues(localizedInputs)
    val inputValues = inputs.map {
      case (name, (_, v)) => name -> v
    }

    // Evaluate the output declarations in dependency order
    val outputsLocal: WdlValueBindings =
      taskIO.evaluateOutputs(evaluator, WdlValueBindings(inputValues))

    // We have task outputs, where files are stored locally. Upload the files to
    // the cloud, and replace the WdlValues.Vs with dxURLs.
    // Special cases:
    // 1) If a file is already on the cloud, do not re-upload it. The content has not
    // changed because files are immutable.
    // 2) A file that was initially local, does not need to be uploaded.
    val localInputFiles: Set[Path] = fileSourceToPath.collect {
      case (_: LocalFileSource, path) => path
    }.toSet
    val localOutputFiles = outputsLocal.toMap.flatMap {
      case (name, value) =>
        val outputDef = outputDefs(name)
        val optional = WdlTypeUtils.isOptional(outputDef.wdlType)
        value match {
          case localFs: LocalFileSource if localInputFiles.contains(localFs.valuePath) =>
            // ignore files that were local to begin with, and do not need to be
            // uploaded to the cloud.
            None
          case localFs: LocalFileSource if !Files.exists(localFs.valuePath) =>
            // ignore optional, non-existent files
            if (optional) {
              None
            } else {
              throw new Exception(s"required output file ${name} does not exist")
            }
          case localFs: LocalFileSource =>
            // use valuePath rather than localPath - the former will match the original path
            // while the latter will have been cannonicalized
            Some(localFs.valuePath)
          case other =>
            throw new RuntimeException(s"Non-local file: ${other}")
        }
    }
    // filter out files that are already on the cloud.
    val remoteFiles = fileSourceToPath.values.toSet
    val toUpload = localOutputFiles.filterNot(remoteFiles.contains).toSet
    val uploadedPathToFileSource = fileUploader.upload(toUpload).map {
      case (path, dxFile) =>
        path -> DxFileAccessProtocol.fromDxFile(dxFile, fileResolver.protocols)
    }
    val alreadyRemotePathToFileSource = fileSourceToPath.map {
      case (path, fileSource) => fileSource -> path
    }
    val allRemotePathToFileSource = alreadyRemotePathToFileSource ++ uploadedPathToFileSource

    val pathToUri = allRemotePathToFileSource.map {
      case (path, fs: RealFileSource) => path.toString -> fs.value
      case (_, other)                 => throw new RuntimeException(s"Cannot translate ${other}")
    }

    // Replace the URIs with local file paths
    def pathTranslator(v: WdlValues.V): Option[WdlValues.V] = {
      v match {
        case WdlValues.V_File(localPath) =>
          pathToUri.get(localPath) match {
            case Some(uri) => Some(WdlValues.V_File(uri))
            case None =>
              throw new Exception(s"Did not localize file ${localPath}")
          }
        case WdlValues.V_Optional(WdlValues.V_File(localPath)) =>
          pathToUri.get(localPath) match {
            case Some(uri) =>
              Some(WdlValues.V_Optional(WdlValues.V_File(uri)))
            case None =>
              Some(WdlValues.V_Null)
          }
        case _ => None
      }
    }

    // replace local paths with URIs in outputs, and convert the outputs to IR
    val remoteOutputs = outputsLocal.bindings.map {
      case (name, value) =>
        val wdlType = outputDefs(name).wdlType
        val irType = WdlUtils.toIRType(wdlType)
        val wdlValue = WdlValueUtils.transform(value, pathTranslator)
        val irValue = WdlUtils.toIRValue(wdlValue, wdlType)
        name -> (irType, irValue)
    }

    // serialize the outputs to the job output file
    jobMeta.writeOutputs(remoteOutputs)
  }

  override def linkOutputs(subjob: DxJob): Unit = {
    val irOutputFields = task.outputs.map { outputDef: TAT.OutputDefinition =>
      outputDef.name -> WdlUtils.toIRType(outputDef.wdlType)
    }
    jobMeta.writeOutputLinks(subjob, irOutputFields)
  }
}

case class WdlTaskSupportFactory() extends TaskSupportFactory {
  override def create(jobMeta: JobMeta,
                      workerPaths: DxWorkerPaths,
                      fileUploader: FileUploader): Option[WdlTaskSupport] = {
    val (doc, typeAliases) =
      try {
        WdlUtils.parseSource(jobMeta.sourceCode, jobMeta.fileResolver)
      } catch {
        case _: Throwable =>
          return None
      }
    if (doc.workflow.isDefined) {
      throw new Exception("a workflow that shouldn't be a member of this document")
    }
    val tasks = doc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    if (tasks.isEmpty) {
      throw new Exception("no tasks in this WDL program")
    }
    if (tasks.size > 1) {
      throw new Exception("More than one task in this WDL program")
    }
    Some(
        WdlTaskSupport(tasks.values.head,
                       doc.version.value,
                       typeAliases,
                       jobMeta,
                       workerPaths,
                       fileUploader)
    )
  }
}
