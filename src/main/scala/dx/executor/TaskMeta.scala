package dx.executor

import java.nio.file.Path

import dx.api.{DxApi, DxJob, InstanceTypeDB}
import dx.core.Native
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache, DxWorkerPaths}
import dx.core.ir.Value.VNull
import dx.core.ir.{
  ParameterLinkDeserializer,
  ParameterLinkExec,
  ParameterLinkSerializer,
  Type,
  Value
}
import dx.core.ir.ValueSerde.valueMapFormat
import dx.core.languages.Language
import dx.core.languages.Language.Language
import dx.core.util.CompressionUtils
import spray.json._
import wdlTools.util.{FileSourceResolver, FileUtils, Logger}

/**
  * Encapsulates information from the metadata files that are
  * loaded onto the worker.
  * @param homeDir worker home dir
  * @param dxApi DxApi
  * @param logger Logger
  */
case class TaskMeta(homeDir: Path = DxWorkerPaths.HomeDir,
                    dxApi: DxApi = DxApi.get,
                    logger: Logger = Logger.get)
    extends JobMeta(homeDir, dxApi, logger) {

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

  private lazy val inputDeserializer = ParameterLinkDeserializer(dxFileDescCache, dxApi)
  lazy val inputs: Map[String, Value] = inputDeserializer.deserializeInputMap(jsInputs)

  private lazy val outputSerializer = ParameterLinkSerializer(fileResolver, dxApi)

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
    FileUtils.writeFileContent(outputPath, JsObject(outputJs).prettyPrint)
  }

  def writeOutputLinks(subjob: DxJob, irOutputFields: Vector[(String, Type)]): Unit = {
    val outputJs: Map[String, JsValue] = irOutputFields
      .flatMap {
        case (name, t) =>
          val link = ParameterLinkExec(subjob, name, t)
          outputSerializer.createFields(link, name)
      }
      .filter {
        case (_, null | JsNull) => false
        case _                  => true
      }
      .toMap
    FileUtils.writeFileContent(outputPath, JsObject(outputJs).prettyPrint)
  }

  lazy val language: Option[Language] = executableDetails.get(Native.Language) match {
    case Some(JsString(lang)) => Some(Language.withName(lang))
    case None =>
      logger.warning("This applet ws built with an old version of dxWDL - please rebuild")
      // we will attempt to detect the language/version later
      None
  }

  lazy val sourceCode: String = {
    val sourceCodeEncoded = executableDetails.get(Native.SourceCode) match {
      case Some(JsString(s)) => s
      case None =>
        logger.warning("This applet ws built with an old version of dxWDL - please rebuild")
        val JsString(s) =
          executableDetails.getOrElse("wdlSourceCode", executableDetails("womSourceCode"))
        s
    }
    CompressionUtils.base64DecodeAndGunzip(sourceCodeEncoded)
  }

  lazy val instanceTypeDb: InstanceTypeDB = executableDetails(Native.InstanceTypeDb) match {
    case JsString(s) =>
      val js = CompressionUtils.base64DecodeAndGunzip(s)
      js.parseJson.convertTo[InstanceTypeDB]
  }

  lazy val defaultRuntimeAttrs: Map[String, Value] =
    executableDetails.get("runtimeAttrs") match {
      case Some(x)             => x.convertTo[Map[String, Value]]
      case Some(JsNull) | None => Map.empty
    }

  lazy val delayWorkspaceDestruction: Option[Boolean] =
    executableDetails.get("delayWorkspaceDestruction") match {
      case Some(JsBoolean(flag)) => Some(flag)
      case None                  => None
    }
}
