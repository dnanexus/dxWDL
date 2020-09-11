package dx.executor

import java.nio.file.Path

import dx.{AppException, AppInternalException}
import dx.api.{
  DxApi,
  DxExecutable,
  DxExecution,
  DxJobDescribe,
  DxProject,
  DxProjectDescribe,
  Field,
  InstanceTypeDB
}
import dx.core.Native
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache, DxWorkerPaths}
import dx.core.ir.Value.VNull
import dx.core.ir.{
  ParameterLink,
  ParameterLinkDeserializer,
  ParameterLinkExec,
  ParameterLinkSerializer,
  Type,
  Value,
  ValueSerde
}
import dx.core.languages.Language
import dx.core.languages.Language.Language
import dx.core.util.CompressionUtils
import spray.json._
import wdlTools.util.{FileSourceResolver, FileUtils, JsUtils, Logger, TraceLevel}

case class JobMeta(homeDir: Path = DxWorkerPaths.HomeDir,
                   dxApi: DxApi = DxApi.get,
                   logger: Logger = Logger.get) {

  lazy val project: DxProject = dxApi.currentProject
  lazy val projectDesc: DxProjectDescribe = project.describe()

  private val inputPath = homeDir.resolve(JobMeta.inputFile)
  private val outputPath = homeDir.resolve(JobMeta.outputFile)
  private val infoPath = homeDir.resolve(JobMeta.infoFile)

  lazy val jsInputs: Map[String, JsValue] =
    FileUtils.readFileContent(inputPath).parseJson.asJsObject.fields

  lazy val inputDeserializer: ParameterLinkDeserializer =
    ParameterLinkDeserializer(dxFileDescCache, dxApi)
  lazy val inputs: Map[String, Value] = inputDeserializer.deserializeInputMap(jsInputs)

  lazy val dxFileDescCache: DxFileDescCache = {
    val allFilesReferenced = jsInputs.flatMap {
      case (_, jsElem) => dxApi.findFiles(jsElem)
    }.toVector
    // Describe all the files and build a lookup cache
    DxFileDescCache(dxApi.fileBulkDescribe(allFilesReferenced))
  }

  lazy val fileResolver: FileSourceResolver = {
    val dxProtocol = DxFileAccessProtocol(dxApi, dxFileDescCache)
    val fileResolver = FileSourceResolver.create(
        localDirectories = Vector(homeDir),
        userProtocols = Vector(dxProtocol),
        logger = logger
    )
    FileSourceResolver.set(fileResolver)
    fileResolver
  }

  def writeJsOutputs(outputJs: Map[String, JsValue]): Unit = {
    FileUtils.writeFileContent(outputPath, JsObject(outputJs).prettyPrint)
  }

  lazy val outputSerializer: ParameterLinkSerializer = ParameterLinkSerializer(fileResolver, dxApi)

  def writeOutputs(outputs: Map[String, (Type, Value)]): Unit = {
    // write outputs, ignore null values, these could occur for optional
    // values that were not specified.
    val outputJs = outputs
      .collect {
        case (name, (t, v)) if v != VNull =>
          outputSerializer.createFields(name, t, v)
      }
      .flatten
      .toMap
    writeJsOutputs(outputJs)
  }

  def createOutputLinks(outputs: Map[String, (Type, Value)]): Map[String, ParameterLink] = {
    outputs.collect {
      case (name, (t, v)) if v != VNull =>
        name -> outputSerializer.createLink(t, v)
    }
  }

  def serializeOutputLinks(outputs: Map[String, ParameterLink]): Map[String, JsValue] = {
    outputs
      .map {
        case (name, link) => outputSerializer.createFieldsFromLink(link, name)
      }
      .flatten
      .toMap
  }

  def writeOutputLinks(outputs: Map[String, ParameterLink]): Unit = {
    writeJsOutputs(serializeOutputLinks(outputs))
  }

  def createOutputLinks(execution: DxExecution,
                        irOutputFields: Map[String, Type],
                        prefix: Option[String] = None): Map[String, ParameterLink] = {
    irOutputFields.map {
      case (name, t) =>
        val fqn = prefix.map(p => s"${p}.${name}").getOrElse(name)
        fqn -> ParameterLinkExec(execution, name, t)
    }
  }

  def serializeOutputLinks(execution: DxExecution,
                           irOutputFields: Map[String, Type]): Map[String, JsValue] = {
    createOutputLinks(execution, irOutputFields)
      .flatMap {
        case (name, link) => outputSerializer.createFieldsFromLink(link, name)
      }
      .filter {
        case (_, null | JsNull) => false
        case _                  => true
      }
  }

  def writeOutputLinks(execution: DxExecution, irOutputFields: Map[String, Type]): Unit = {
    writeJsOutputs(serializeOutputLinks(execution, irOutputFields))
  }

  private lazy val info: Map[String, JsValue] =
    FileUtils.readFileContent(infoPath).parseJson.asJsObject.fields

  lazy val jobDesc: DxJobDescribe = DxApi.get.currentJob.describe(
      Set(Field.Executable, Field.Details, Field.InstanceType)
  )

  lazy val executable: DxExecutable = info.get("executable") match {
    case None =>
      dxApi.logger.trace(
          "executable field not found locally, performing an API call.",
          minLevel = TraceLevel.None
      )
      jobDesc.executable.get
    case Some(JsString(x)) if x.startsWith("app-") =>
      dxApi.app(x)
    case Some(JsString(x)) if x.startsWith("applet-") =>
      dxApi.applet(x)
    case Some(other) =>
      throw new Exception(s"Malformed executable field ${other} in job info")
  }

  lazy val executableDetails: Map[String, JsValue] =
    executable.describe(Set(Field.Details)).details.get.asJsObject.fields

  lazy val language: Option[Language] = executableDetails.get(Native.Language) match {
    case Some(JsString(lang)) => Some(Language.withName(lang))
    case None =>
      logger.warning("This applet ws built with an old version of dxWDL - please rebuild")
      // we will attempt to detect the language/version later
      None
    case other =>
      throw new Exception(s"unexpected ${Native.Language} value ${other}")
  }

  lazy val sourceCode: String = {
    val sourceCodeEncoded = executableDetails.get(Native.SourceCode) match {
      case Some(JsString(s)) => s
      case None =>
        logger.warning("This applet ws built with an old version of dxWDL - please rebuild")
        val JsString(s) =
          executableDetails.getOrElse("wdlSourceCode", executableDetails("womSourceCode"))
        s
      case other =>
        throw new Exception(s"unexpected ${Native.SourceCode} value ${other}")
    }
    CompressionUtils.base64DecodeAndGunzip(sourceCodeEncoded)
  }

  lazy val instanceTypeDb: InstanceTypeDB = executableDetails(Native.InstanceTypeDb) match {
    case JsString(s) =>
      val js = CompressionUtils.base64DecodeAndGunzip(s)
      js.parseJson.convertTo[InstanceTypeDB]
    case other =>
      throw new Exception(s"unexpected ${Native.InstanceTypeDb} value ${other}")
  }

  lazy val defaultRuntimeAttrs: Map[String, Value] =
    executableDetails.get(Native.RuntimeAttributes) match {
      case Some(JsObject(fields)) => ValueSerde.deserializeMap(fields)
      case Some(JsNull) | None    => Map.empty
      case other =>
        throw new Exception(s"unexpected ${Native.RuntimeAttributes} value ${other}")
    }

  lazy val delayWorkspaceDestruction: Option[Boolean] =
    executableDetails.get("delayWorkspaceDestruction") match {
      case Some(JsBoolean(flag)) => Some(flag)
      case None                  => None
    }

  def error(e: Throwable): Unit = {
    JobMeta.writeError(homeDir, e)
  }
}

object JobMeta {
  val inputFile = "job_input.json"
  val outputFile = "job_output.json"
  val errorFile = "job_error.json"
  val infoFile = "dnanexus-job.json"

  /**
    * Report an error, since this is called from a bash script, we can't simply
    * raise an exception. Instead, we write the error to a standard JSON file.
    * @param e the exception
    */
  def writeError(homeDir: Path, e: Throwable): Unit = {
    val jobErrorPath = homeDir.resolve(errorFile)
    val errType = e match {
      case _: AppException         => "AppError"
      case _: AppInternalException => "AppInternalError"
      case _: Throwable            => "AppInternalError"
    }
    // We are limited in what characters can be written to json, so we
    // provide a short description for json.
    //
    // Note: we sanitize this string, to be absolutely sure that
    // it does not contain problematic JSON characters.
    val errMsg = JsObject(
        "error" -> JsObject(
            "type" -> JsString(errType),
            "message" -> JsUtils.sanitizedString(e.getMessage)
        )
    ).prettyPrint
    FileUtils.writeFileContent(jobErrorPath, errMsg)
  }
}
