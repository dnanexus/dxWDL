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
import dx.core.languages.wdl.{DxMetaHints, Runtime, WdlUtils}
import dx.executor.{FileUploader, JobMeta, TaskSupport, TaskSupportFactory}
import dx.translator.wdl.IrToWdlValueBindings
import spray.json._
import wdlTools.eval.WdlValues._
import wdlTools.eval.{Eval, EvalUtils, Hints, Meta, WdlValueBindings, WdlValueSerde}
import wdlTools.exec.{
  DockerUtils,
  SafeLocalizationDisambiguator,
  TaskCommandFileGenerator,
  TaskInputOutput
}
import wdlTools.syntax.{SourceLocation, WdlVersion}
import wdlTools.types.WdlTypes._
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.util.{
  DefaultBindings,
  FileSource,
  LocalFileSource,
  Logger,
  RealFileSource,
  TraceLevel
}

object WdlTaskSupport {
  def serializeValues(
      values: Map[String, (T, V)]
  ): Map[String, JsValue] = {
    values.map {
      case (name, (t, v)) =>
        val jsType = WdlUtils.serializeType(t)
        val jsValue = WdlValueSerde.serialize(v)
        name -> JsObject("type" -> jsType, "value" -> jsValue)
    }
  }

  def deserializeValues(
      values: Map[String, JsValue],
      typeAliases: Map[String, T_Struct]
  ): Map[String, (T, V)] = {
    values.map {
      case (name, JsObject(fields)) =>
        val t = WdlUtils.deserializeType(fields("type"), typeAliases)
        val v = WdlValueSerde.deserialize(fields("value"), t)
        name -> (t, v)
      case other =>
        throw new Exception(s"unexpected value ${other}")
    }
  }
}

case class WdlTaskSupport(task: TAT.Task,
                          wdlVersion: WdlVersion,
                          typeAliases: DefaultBindings[T_Struct],
                          jobMeta: JobMeta,
                          workerPaths: DxWorkerPaths,
                          fileUploader: FileUploader)
    extends TaskSupport {

  private val fileResolver = jobMeta.fileResolver
  private val dxApi = jobMeta.dxApi
  private val logger = jobMeta.logger

  private lazy val evaluator = Eval(
      workerPaths,
      Some(wdlVersion),
      jobMeta.fileResolver,
      Logger.Quiet
  )
  private lazy val taskIO = TaskInputOutput(task, logger)

  private def getInputs: Map[String, V] = {
    val taskInputs = task.inputs.map(inp => inp.name -> inp).toMap
    // convert IR to WDL values; discard auxiliary fields
    val inputWdlValues: Map[String, V] = jobMeta.inputs.collect {
      case (name, value) if !name.endsWith(ParameterLink.FlatFilesSuffix) =>
        val wdlType = taskInputs(name).wdlType
        name -> WdlUtils.fromIRValue(value, wdlType, name)
    }
    // add default values for any missing inputs
    taskIO.inputsFromValues(inputWdlValues, evaluator, strict = true).bindings
  }

  private lazy val inputTypes: Map[String, T] =
    task.inputs.map(d => d.name -> d.wdlType).toMap

  private def printInputs(inputs: Map[String, V]): Unit = {
    if (logger.isVerbose) {
      val inputStr = task.inputs
        .map { inputDef =>
          s"${inputDef.name} -> (${inputDef.wdlType}, ${inputs.get(inputDef.name)})"
        }
        .mkString("\n")
      logger.traceLimited(s"inputs: ${inputStr}")
    }
  }

  private def evaluatePrivateVariables(inputs: Map[String, V]): Map[String, V] = {
    // evaluate the private variables using the inputs
    val env: Map[String, V] =
      task.privateVariables.foldLeft(inputs) {
        case (env, TAT.PrivateVariable(name, wdlType, Some(expr), _)) =>
          val wdlValue =
            evaluator.applyExprAndCoerce(expr, wdlType, WdlValueBindings(env))
          env + (name -> wdlValue)
        case (_, TAT.PrivateVariable(name, _, None, _)) =>
          throw new Exception(s"Variable ${name} has no expression")
      }
    env
  }

  private def createRuntime(env: Map[String, V]): Runtime[IrToWdlValueBindings] = {
    Runtime(
        wdlVersion,
        task.runtime,
        task.hints,
        evaluator,
        IrToWdlValueBindings(jobMeta.defaultRuntimeAttrs),
        Some(WdlValueBindings(env))
    )
  }

  private def getRequiredInstanceType(inputs: Map[String, V]): String = {
    logger.traceLimited("calcInstanceType", minLevel = TraceLevel.VVerbose)
    printInputs(inputs)
    val env = evaluatePrivateVariables(inputs)
    val runtime = createRuntime(env)
    val request = runtime.parseInstanceType
    logger.traceLimited(s"calcInstanceType $request")
    jobMeta.instanceTypeDb.apply(request).name
  }

  override lazy val getRequiredInstanceType: String = getRequiredInstanceType(getInputs)

  // TODO: it would be nice to extract dx:// links from VString values - this will
  //  happen in the case where the container is a dx file and being passed in as
  //  an input parameter - so that they could be downloaded using dxda. However,
  //  this would also require some way for the downloaded image tarball to be
  //  discovered and loaded. For now, we rely on DockerUtils to download the image
  //  (via DxFileSource, which uses the API to download the file).
  private def extractFiles(v: V): Vector[FileSource] = {
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

  /**
    * Input files are represented as dx URLs (dx://proj-xxxx:file-yyyy::/A/B/C.txt)
    * instead of local files (/home/C.txt). A file may be referenced more than once,
    * but we want to download it just once.
    * @return
    */
  override def localizeInputFiles(
      streamAllFiles: Boolean
  ): (Map[String, JsValue], Map[FileSource, Path], Option[DxdaManifest], Option[DxfuseManifest]) = {
    assert(workerPaths.getInputFilesDir() != workerPaths.getDxfuseMountDir())

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
            val stream = streamAllFiles || (parameterMeta.get(name) match {
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
                          Hints.LocalizationOptionalKey)
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

    logger.traceLimited(s"downloading files = ${filesToDownload}")
    val downloadLocalizer =
      SafeLocalizationDisambiguator(workerPaths.getInputFilesDir(),
                                    existingPaths = localFilesToPath.values.toSet)
    val downloadFileSourceToPath: Map[FileSource, Path] =
      filesToDownload.map(fs => fs -> downloadLocalizer.getLocalPath(fs)).toMap
    val dxdaManifest: Option[DxdaManifest] =
      DxdaManifestBuilder(dxApi).apply(downloadFileSourceToPath.collect {
        case (dxFs: DxFileSource, localPath) => dxFs.dxFile -> localPath
      })

    logger.traceLimited(s"streaming files = ${filesToStream}")
    val streamingLocalizer =
      SafeLocalizationDisambiguator(workerPaths.getDxfuseMountDir(),
                                    existingPaths = localFilesToPath.values.toSet)
    val streamFileSourceToPath: Map[FileSource, Path] =
      filesToStream.map(fs => fs -> streamingLocalizer.getLocalPath(fs)).toMap
    val dxfuseManifest =
      DxfuseManifestBuilder(dxApi).apply(streamFileSourceToPath.collect {
        case (dxFs: DxFileSource, localPath) => dxFs.dxFile -> localPath
      }, workerPaths)

    val fileSourceToPath = localFilesToPath ++ downloadFileSourceToPath ++ streamFileSourceToPath

    val uriToPath: Map[String, String] = fileSourceToPath.map {
      case (dxFs: DxFileSource, path)     => dxFs.value -> path.toString
      case (localFs: LocalFileSource, p2) => localFs.valuePath.toString -> p2.toString
      case other                          => throw new RuntimeException(s"unsupported file source ${other}")
    }

    // Replace the URIs with local file paths
    def pathTranslator(v: V): Option[V] = {
      v match {
        case V_File(uri) =>
          uriToPath.get(uri) match {
            case Some(localPath) => Some(V_File(localPath))
            case None =>
              throw new Exception(s"Did not localize file ${uri}")
          }
        case V_Optional(V_File(uri)) =>
          uriToPath.get(uri) match {
            case Some(localPath) =>
              Some(V_Optional(V_File(localPath)))
            case None =>
              Some(V_Null)
          }
        case _ => None
      }
    }

    val localizedInputs: Map[String, V] =
      inputs.view.mapValues(v => EvalUtils.transform(v, pathTranslator)).toMap

    // serialize the updated inputs
    val localizedInputsJs = WdlTaskSupport.serializeValues(localizedInputs.map {
      case (name, value) => name -> (inputTypes(name), value)
    })

    (localizedInputsJs, fileSourceToPath, dxdaManifest, dxfuseManifest)
  }

  override def writeCommandScript(
      localizedInputs: Map[String, JsValue]
  ): Map[String, JsValue] = {
    val inputs = WdlTaskSupport.deserializeValues(localizedInputs, typeAliases.bindings)
    val inputValues = inputs.map {
      case (name, (_, v)) => name -> v
    }
    printInputs(inputValues)
    val inputsWithPrivateVars = evaluatePrivateVariables(inputValues)
    val ctx = WdlValueBindings(inputsWithPrivateVars)
    val command = evaluator.applyCommand(task.command, ctx) match {
      case s if s.trim.isEmpty => None
      case s                   => Some(s)
    }
    val generator = TaskCommandFileGenerator(logger)
    val runtime = createRuntime(inputsWithPrivateVars)
    val dockerUtils = DockerUtils(fileResolver, logger)
    val container = runtime.container match {
      case Vector() => None
      case Vector(image) =>
        val resolvedImage = dockerUtils.getImage(image, SourceLocation.empty)
        Some(resolvedImage, workerPaths)
      case v =>
        // we prefer a dx:// url
        val (dxUrls, imageNames) = v.partition(_.startsWith(DxPath.DxUriPrefix))
        val resolvedImage = dockerUtils.getImage(dxUrls ++ imageNames, SourceLocation.empty)
        Some(resolvedImage, workerPaths)
    }
    generator.apply(command, workerPaths, container)
    val inputAndPrivateVarTypes = inputTypes ++ task.privateVariables
      .map(d => d.name -> d.wdlType)
      .toMap
    WdlTaskSupport.serializeValues(ctx.bindings.map {
      case (name, value) => name -> (inputAndPrivateVarTypes(name), value)
    })
  }

  private def getOutputs(env: Map[String, V]): WdlValueBindings = {
    taskIO.evaluateOutputs(evaluator, WdlValueBindings(env))
  }

  private lazy val outputDefs: Map[String, TAT.OutputParameter] =
    task.outputs.map(d => d.name -> d).toMap

  private def extractOutputFiles(name: String, v: V, t: T): Vector[Path] = {
    def getPath(fs: FileSource, optional: Boolean): Vector[Path] = {
      // use valuePath rather than localPath - the former will match the original path
      // while the latter will have been cannonicalized
      fs match {
        case localFs: LocalFileSource if optional && !Files.exists(localFs.valuePath) =>
          // ignore optional, non-existent files
          Vector.empty
        case localFs: LocalFileSource if !Files.exists(localFs.valuePath) =>
          throw new Exception(s"required output file ${name} does not exist")
        case localFs: LocalFileSource =>
          Vector(localFs.valuePath)
        case other =>
          throw new RuntimeException(s"Non-local file: ${other}")
      }
    }

    def extractStructFiles(values: Map[String, V], types: Map[String, T]): Vector[Path] = {
      types.flatMap {
        case (key, t) =>
          (t, values.get(key)) match {
            case (T_Optional(_), None) =>
              Vector.empty
            case (_, None) =>
              throw new Exception(s"missing non-optional member ${key} of struct ${name}")
            case (_, Some(v)) =>
              inner(v, t, optional = false)
          }
      }.toVector
    }

    def inner(innerValue: V, innerType: T, optional: Boolean): Vector[Path] = {
      (innerType, innerValue) match {
        case (T_Optional(_), V_Null) =>
          Vector.empty
        case (T_Optional(optType), V_Optional(optValue)) =>
          inner(optValue, optType, optional = true)
        case (T_Optional(optType), _) =>
          inner(innerValue, optType, optional = true)
        case (T_File, V_File(path)) =>
          getPath(fileResolver.resolve(path), optional)
        case (T_File, V_String(path)) =>
          getPath(fileResolver.resolve(path), optional)
        case (T_Array(_, nonEmpty), V_Array(array)) if nonEmpty && array.isEmpty =>
          throw new Exception(s"Non-empty array ${name} has empty value")
        case (T_Array(elementType, _), V_Array(array)) =>
          array.flatMap(inner(_, elementType, optional = false))
        case (T_Map(keyType, valueType), V_Map(map)) =>
          map.flatMap {
            case (key, value) =>
              inner(key, keyType, optional = false) ++ inner(value, valueType, optional = false)
          }.toVector
        case (T_Pair(leftType, rightType), V_Pair(left, right)) =>
          inner(left, leftType, optional = false) ++ inner(right, rightType, optional = false)
        case (T_Struct(typeName, _), V_Struct(valueName, _)) if typeName != valueName =>
          throw new Exception(s"struct ${name} type ${typeName} does not match value ${valueName}")
        case (T_Struct(_, memberTypes), V_Struct(_, members)) =>
          extractStructFiles(members, memberTypes)
        case (T_Struct(_, memberTypes), V_Object(members)) =>
          extractStructFiles(members, memberTypes)
        case (T_Object, V_Object(members)) =>
          members.values.flatMap(extractFiles).flatMap(getPath(_, optional = true)).toVector
        case _ =>
          Vector.empty
      }
    }
    inner(v, t, optional = false)
  }

  override def evaluateOutputs(localizedInputs: Map[String, JsValue],
                               fileSourceToPath: Map[FileSource, Path],
                               fileUploader: FileUploader): Unit = {
    val inputs = WdlTaskSupport.deserializeValues(localizedInputs, typeAliases.bindings)
    val inputValues = inputs.map {
      case (name, (_, v)) => name -> v
    }

    // Evaluate the output parameters in dependency order
    val outputsLocal: WdlValueBindings = getOutputs(inputValues)

    // We have task outputs, where files are stored locally. Upload the files to
    // the cloud, and replace the WdlValues.Vs with dxURLs.
    // Special cases:
    // 1) If a file is already on the cloud, do not re-upload it. The content has not
    // changed because files are immutable.
    // 2) A file that was initially local, does not need to be uploaded.
    val localOutputFiles: Set[Path] = outputsLocal.toMap.flatMap {
      case (name, value) =>
        val outputDef = outputDefs(name)
        extractOutputFiles(name, value, outputDef.wdlType)
    }.toSet
    val localInputFiles: Set[Path] = fileSourceToPath.collect {
      case (_: LocalFileSource, path) => path
    }.toSet
    val remoteFiles = fileSourceToPath.values.toSet
    val toUpload = localOutputFiles.diff(localInputFiles | remoteFiles)
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
    def pathTranslator(v: V): Option[V] = {
      v match {
        case V_File(localPath) =>
          pathToUri.get(localPath) match {
            case Some(uri) => Some(V_File(uri))
            case None =>
              throw new Exception(s"Did not localize file ${localPath}")
          }
        case V_Optional(V_File(localPath)) =>
          pathToUri.get(localPath) match {
            case Some(uri) =>
              Some(V_Optional(V_File(uri)))
            case None =>
              Some(V_Null)
          }
        case _ => None
      }
    }

    // replace local paths with URIs in outputs, and convert the outputs to IR
    val remoteOutputs = outputsLocal.bindings.map {
      case (name, value) =>
        val wdlType = outputDefs(name).wdlType
        val irType = WdlUtils.toIRType(wdlType)
        val wdlValue = EvalUtils.transform(value, pathTranslator)
        val irValue = WdlUtils.toIRValue(wdlValue, wdlType)
        name -> (irType, irValue)
    }

    // serialize the outputs to the job output file
    jobMeta.writeOutputs(remoteOutputs)
  }

  override def linkOutputs(subjob: DxJob): Unit = {
    val irOutputFields = task.outputs.map { outputDef: TAT.OutputParameter =>
      outputDef.name -> WdlUtils.toIRType(outputDef.wdlType)
    }.toMap
    jobMeta.writeOutputLinks(subjob, irOutputFields)
  }
}

case class WdlTaskSupportFactory() extends TaskSupportFactory {
  override def create(jobMeta: JobMeta,
                      workerPaths: DxWorkerPaths,
                      fileUploader: FileUploader): Option[WdlTaskSupport] = {
    val (task, typeAliases, doc) =
      try {
        WdlUtils.parseSingleTask(jobMeta.sourceCode, jobMeta.fileResolver)
      } catch {
        case ex: Throwable =>
          Logger.error(s"error parsing ${jobMeta.sourceCode}", Some(ex))
          return None
      }
    Some(
        WdlTaskSupport(task, doc.version.value, typeAliases, jobMeta, workerPaths, fileUploader)
    )
  }
}
