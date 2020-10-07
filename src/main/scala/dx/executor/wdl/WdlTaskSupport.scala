package dx.executor.wdl

import java.nio.file.{Files, Path}

import dx.api.{DxJob, DxPath}
import dx.core.io.{
  DxFileSource,
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
  AddressableFileSource,
  DefaultBindings,
  FileNode,
  LocalFileSource,
  Logger,
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
                          fileUploader: FileUploader)
    extends TaskSupport {

  private val fileResolver = jobMeta.fileResolver
  private val dxApi = jobMeta.dxApi
  private val logger = jobMeta.logger

  private lazy val evaluator = Eval(
      jobMeta.workerPaths,
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
        case (env, TAT.PrivateVariable(name, wdlType, expr, _)) =>
          val wdlValue =
            evaluator.applyExprAndCoerce(expr, wdlType, WdlValueBindings(env))
          env + (name -> wdlValue)
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
  private def extractFiles(v: V): Vector[FileNode] = {
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
  ): (Map[String, JsValue], Map[FileNode, Path], Option[DxdaManifest], Option[DxfuseManifest]) = {
    assert(jobMeta.workerPaths.getInputFilesDir() != jobMeta.workerPaths.getDxfuseMountDir())

    val inputs = getInputs
    printInputs(inputs)

    val (localFiles, filesToStream, filesToDownload) =
      inputs.foldLeft((Set.empty[FileNode], Set.empty[FileNode], Set.empty[FileNode])) {
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
      SafeLocalizationDisambiguator(jobMeta.workerPaths.getInputFilesDir(),
                                    existingPaths = localFilesToPath.values.toSet)
    val downloadFileSourceToPath: Map[FileNode, Path] =
      filesToDownload.map(fs => fs -> downloadLocalizer.getLocalPath(fs)).toMap
    val dxdaManifest: Option[DxdaManifest] =
      DxdaManifestBuilder(dxApi).apply(downloadFileSourceToPath.collect {
        case (dxFs: DxFileSource, localPath) => dxFs.dxFile -> localPath
      })

    logger.traceLimited(s"streaming files = ${filesToStream}")
    val streamingLocalizer =
      SafeLocalizationDisambiguator(jobMeta.workerPaths.getDxfuseMountDir(),
                                    existingPaths = localFilesToPath.values.toSet)
    val streamFileSourceToPath: Map[FileNode, Path] =
      filesToStream.map(fs => fs -> streamingLocalizer.getLocalPath(fs)).toMap
    val dxfuseManifest =
      DxfuseManifestBuilder(dxApi).apply(streamFileSourceToPath.collect {
        case (dxFs: DxFileSource, localPath) => dxFs.dxFile -> localPath
      }, jobMeta.workerPaths)

    val fileSourceToPath = localFilesToPath ++ downloadFileSourceToPath ++ streamFileSourceToPath

    val uriToPath: Map[String, String] = fileSourceToPath.map {
      case (dxFs: DxFileSource, path)     => dxFs.address -> path.toString
      case (localFs: LocalFileSource, p2) => localFs.originalPath.toString -> p2.toString
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
    // TODO: there may be private variables that reference files created by the
    //  command, or functions that depend on the execution of the command
    //  (e.g. stdout()). Split the private vars into those that need to be
    //  evaluated before vs after the command, and only evaluate the former here.
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
        Some(resolvedImage, jobMeta.workerPaths)
      case v =>
        // we prefer a dx:// url
        val (dxUrls, imageNames) = v.partition(_.startsWith(DxPath.DxUriPrefix))
        val resolvedImage = dockerUtils.getImage(dxUrls ++ imageNames, SourceLocation.empty)
        Some(resolvedImage, jobMeta.workerPaths)
    }
    generator.apply(command, jobMeta.workerPaths, container)
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

  private def extractOutputFiles(name: String, v: V, t: T): Vector[FileNode] = {
    def getFileNode(varName: String, fs: FileNode, optional: Boolean): Vector[FileNode] = {
      fs match {
        case localFs: LocalFileSource if optional && !Files.exists(localFs.localPath) =>
          // ignore optional, non-existent files
          Vector.empty
        case localFs: LocalFileSource if !Files.exists(localFs.localPath) =>
          throw new Exception(
              s"required output file ${varName} does not exist at ${localFs.localPath}"
          )
        case localFs: LocalFileSource =>
          Vector(localFs)
        case other =>
          throw new RuntimeException(s"${varName} specifies non-local file ${other}")
      }
    }

    def extractStructFiles(structName: String,
                           values: Map[String, V],
                           types: Map[String, T]): Vector[FileNode] = {
      types.flatMap {
        case (key, t) =>
          (t, values.get(key)) match {
            case (T_Optional(_), None) =>
              Vector.empty
            case (_, None) =>
              throw new Exception(s"missing non-optional member ${key} of struct ${name}")
            case (_, Some(v)) =>
              inner(s"${structName}.${key}", v, t, optional = false)
          }
      }.toVector
    }

    def inner(innerName: String,
              innerValue: V,
              innerType: T,
              optional: Boolean): Vector[FileNode] = {
      (innerType, innerValue) match {
        case (T_Optional(_), V_Null) =>
          Vector.empty
        case (T_Optional(optType), V_Optional(optValue)) =>
          inner(innerName, optValue, optType, optional = true)
        case (T_Optional(optType), _) =>
          inner(innerName, innerValue, optType, optional = true)
        case (T_File, V_File(path)) =>
          getFileNode(innerName, fileResolver.resolve(path), optional)
        case (T_File, V_String(path)) =>
          getFileNode(innerName, fileResolver.resolve(path), optional)
        case (T_Array(_, nonEmpty), V_Array(array)) if nonEmpty && array.isEmpty =>
          throw new Exception(s"Non-empty array ${name} has empty value")
        case (T_Array(elementType, _), V_Array(array)) =>
          array.zipWithIndex.flatMap {
            case (element, index) =>
              inner(s"${innerName}[${index}]", element, elementType, optional = false)
          }
        case (T_Map(keyType, valueType), V_Map(map)) =>
          map.flatMap {
            case (key, value) =>
              val keyFiles = inner(s"${innerName}.${key}", key, keyType, optional = false)
              val valueFiles = inner(s"${innerName}.${value}", value, valueType, optional = false)
              keyFiles ++ valueFiles
          }.toVector
        case (T_Pair(leftType, rightType), V_Pair(left, right)) =>
          val leftFiles = inner(s"${innerName}.left", left, leftType, optional = false)
          val rightFiles = inner(s"${innerName}.right", right, rightType, optional = false)
          leftFiles ++ rightFiles
        case (T_Struct(typeName, _), V_Struct(valueName, _)) if typeName != valueName =>
          throw new Exception(s"struct ${name} type ${typeName} does not match value ${valueName}")
        case (T_Struct(name, memberTypes), V_Struct(_, members)) =>
          extractStructFiles(name, members, memberTypes)
        case (T_Struct(name, memberTypes), V_Object(members)) =>
          extractStructFiles(name, members, memberTypes)
        case (T_Object, V_Object(members)) =>
          members.flatMap {
            case (key, value) =>
              val files = extractFiles(value)
              files.flatMap(fs => getFileNode(s"${innerName}.${key}", fs, optional = true))
          }.toVector
        case _ =>
          Vector.empty
      }
    }
    inner(name, v, t, optional = false)
  }

  override def evaluateOutputs(localizedInputs: Map[String, JsValue],
                               fileSourceToPath: Map[FileNode, Path],
                               fileUploader: FileUploader): Unit = {
    val localizedWdlInputs = WdlTaskSupport.deserializeValues(localizedInputs, typeAliases.bindings)

    // TODO: evaluate any private variables that depend on the command

    // Evaluate the output parameters in dependency order.
    // These will include output files without canonicalized paths, which is why we need
    // the following complex logic to match up local outputs to remote URIs.
    val localizedOutputs: WdlValueBindings = getOutputs(localizedWdlInputs.map {
      case (name, (_, v)) => name -> v
    })

    // extract files from the outputs
    val localOutputFileSources: Vector[FileNode] = localizedOutputs.toMap.flatMap {
      case (name, value) =>
        val outputDef = outputDefs(name)
        extractOutputFiles(name, value, outputDef.wdlType)
    }.toVector

    // Build a map of all the string values in the output values that might
    // map to the same (absolute) local path. Some of the outputs may be files
    // that were inputs (in `fileSourceToPath`) - these do not need to be
    // re-uploaded. The `localPath`s will be the same but the `originalPath`s
    // may be different.
    val inputPaths: Set[Path] = fileSourceToPath.flatMap {
      case (fs, path) => Set(fs.localPath, path)
    }.toSet
    val delocalizingValueToPath: Map[String, Path] = localOutputFileSources
      .collect {
        case fs: AddressableFileSource if !inputPaths.contains(fs.localPath) =>
          Map(fs.address -> fs.localPath, fs.localPath.toString -> fs.localPath)
      }
      .flatten
      .toMap

    // upload the files, and map their local paths to their remote URIs
    val delocalizedPathToUri: Map[Path, String] =
      fileUploader.upload(delocalizingValueToPath.values.toSet).map {
        case (path, dxFile) => path -> dxFile.asUri
      }

    // Replace the local paths in the output values with URIs. For files that
    // were inputs, we can resolve them using a mapping of input values to URIs;
    // for files that were generated on the worker, this requires two look-ups:
    // first to get the absoulte Path associated with the file value (which may
    // be relative or absolute), and second to get the URI associated with the
    // Path. Returns an Optional[String] because optional outputs may be null.
    val inputValueToUri = fileSourceToPath
      .collect {
        case (fs: AddressableFileSource, path) =>
          Map(fs.address -> fs.address, path.toString -> fs.address)
      }
      .flatten
      .toMap
    def resolveFileValue(value: String): Option[String] = {
      inputValueToUri
        .get(value)
        .orElse(delocalizingValueToPath.get(value) match {
          case Some(path) => delocalizedPathToUri.get(path)
          case _          => None
        })
    }

    def pathTranslator(v: V): Option[V] = {
      v match {
        case V_File(value) =>
          resolveFileValue(value) match {
            case Some(uri) => Some(V_File(uri))
            case None =>
              throw new Exception(s"Did not delocalize file ${value}")
          }
        case V_Optional(V_File(value)) =>
          resolveFileValue(value) match {
            case Some(uri) => Some(V_Optional(V_File(uri)))
            case None      => Some(V_Null)
          }
        case _ => None
      }
    }

    val delocalizedOutputs = localizedOutputs.bindings.map {
      case (name, value) =>
        val wdlType = outputDefs(name).wdlType
        val irType = WdlUtils.toIRType(wdlType)
        val wdlValue = EvalUtils.transform(value, pathTranslator)
        val irValue = WdlUtils.toIRValue(wdlValue, wdlType)
        name -> (irType, irValue)
    }

    // serialize the outputs to the job output file
    jobMeta.writeOutputs(delocalizedOutputs)
  }

  override def linkOutputs(subjob: DxJob): Unit = {
    val irOutputFields = task.outputs.map { outputDef: TAT.OutputParameter =>
      outputDef.name -> WdlUtils.toIRType(outputDef.wdlType)
    }.toMap
    jobMeta.writeOutputLinks(subjob, irOutputFields)
  }
}

case class WdlTaskSupportFactory() extends TaskSupportFactory {
  override def create(jobMeta: JobMeta, fileUploader: FileUploader): Option[WdlTaskSupport] = {
    val (task, typeAliases, doc) =
      try {
        WdlUtils.parseSingleTask(jobMeta.sourceCode, jobMeta.fileResolver)
      } catch {
        case ex: Throwable =>
          Logger.error(s"error parsing ${jobMeta.sourceCode}", Some(ex))
          return None
      }
    Some(
        WdlTaskSupport(task, doc.version.value, typeAliases, jobMeta, fileUploader)
    )
  }
}
