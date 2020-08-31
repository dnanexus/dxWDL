package dx.translator

import java.nio.file.Path

import dx.core.ir.{Bundle, Parameter, Value}
import dx.core.languages.Language.Language
import spray.json.JsValue
import wdlTools.util.{FileSourceResolver, Logger}

abstract class InputFile(fields: Map[String, JsValue],
                         fileResolver: FileSourceResolver,
                         logger: Logger) {
  private var retrievedKeys: Set[String] = Set.empty
  private var irFields: Map[String, Value] = Map.empty

  private def getExactlyOnce(fqn: String): Option[JsValue] = {
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

  protected def translateInput(parameter: Parameter,
                               jsv: JsValue,
                               dxName: String,
                               fileResolver: FileSourceResolver,
                               encodeName: Boolean): Map[String, Value]

  // If WDL variable fully qualified name [fqn] was provided in the
  // input file, set [stage.cvar] to its JSON value
  def checkAndBind(fqn: String, dxName: String, parameter: Parameter): Unit = {
    getExactlyOnce(fqn) match {
      case None      => ()
      case Some(jsv) =>
        // Do not assign the value to any later stages.
        // We found the variable declaration, the others
        // are variable uses.
        logger.trace(s"checkAndBind, found: ${fqn} -> ${dxName}")
        irFields ++= translateInput(parameter, jsv, dxName, fileResolver, encodeName = false)
    }
  }

  def checkAllUsed(): Unit = {
    val unused = fields.keySet -- retrievedKeys
    if (unused.nonEmpty) {
      throw new Exception(s"""|Could not map all input fields.
                              |These were left: ${unused}""".stripMargin)

    }
  }
}

abstract class LanguageTranslator {

  /**
    * Translates a document in a supported workflow language to a Bundle.
    *
    * @param source the source file
    * @param locked whether to lock generated workflows
    * @param reorgEnabled whether output reorg is enabled
    * @return the generated Bundle
    */
  def translateDocument(source: Path, locked: Boolean, reorgEnabled: Option[Boolean] = None): Bundle

  /**
    * Translates a document in a supported workflow language to a Bundle.
    * Also uses the provided inputs to
    *
    * @param source the source file.
    * @param locked whether to lock generated workflows
    * @param defaults default values to embed in generated Bundle
    * @param inputs inputs to use when resolving defaults
    * @param reorgEnabled whether output reorg is enabled
    * @return (bundle, fileCache), where bundle is the generated Bundle and fileCache
    *         is a cache of the translated values for all the DxFiles in `defaults`
    *         and `inputs`
    */
  def translateDocumentWithDefaults(
      source: Path,
      locked: Boolean,
      defaults: Map[String, JsValue],
      inputs: Map[Path, Map[String, JsValue]] = Map.empty,
      reorgEnabled: Option[Boolean] = None
  ): (Bundle, Map[Path, InputFile])
}
/*


    val (pathToDxFile, dxFiles) = filterFiles(inputAndDefaultFields, bundle, project)
    // lookup platform files in bulk
    val allFiles = dxApi.fileBulkDescribe(dxFiles)
    val dxFileDescCache = DxFileDescCache(allFiles)
    // update bundle with default values
    val dxProtocol = DxFileAccessProtocol(dxApi, dxFileDescCache)
    val fileResolverWithCache = fileResolver.replaceProtocol[DxFileAccessProtocol](dxProtocol)
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
 */
trait LanguageTranslatorFactory {
  def create(language: Language,
             extras: Option[Extras],
             fileResolver: FileSourceResolver): Option[LanguageTranslator]

  def create(sourceFile: Path,
             extras: Option[Extras],
             fileResolver: FileSourceResolver): Option[LanguageTranslator]
}
