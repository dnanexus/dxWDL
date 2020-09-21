package dx.translator

import java.nio.file.Path

import dx.api.{DxApi, DxProject}
import dx.core.ir._
import dx.core.languages.Language.Language
import dx.translator.wdl.WdlTranslatorFactory
import wdlTools.util.{FileSourceResolver, FileUtils, Logger}

trait Translator {
  def apply: Bundle

  def translateInputs(bundle: Bundle,
                      inputs: Vector[Path],
                      defaults: Option[Path],
                      project: DxProject): (Bundle, FileSourceResolver)
}

trait TranslatorFactory {
  def create(sourceFile: Path,
             language: Option[Language],
             locked: Boolean,
             defaultRuntimeAttrs: Map[String, Value],
             reorgAttrs: ReorgAttributes,
             fileResolver: FileSourceResolver,
             dxApi: DxApi = DxApi.get,
             logger: Logger = Logger.get): Option[Translator]
}

object TranslatorFactory {
  private val translatorFactories: Vector[TranslatorFactory] = Vector(
      WdlTranslatorFactory()
  )

  def create(source: Path,
             language: Option[Language] = None,
             extras: Option[Extras] = None,
             locked: Boolean = false,
             reorgEnabled: Option[Boolean] = None,
             baseFileResolver: FileSourceResolver = FileSourceResolver.get,
             dxApi: DxApi = DxApi.get,
             logger: Logger = Logger.get): Translator = {
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
    translatorFactories
      .collectFirst { factory =>
        factory.create(sourceAbsPath,
                       language,
                       locked,
                       defaultRuntimeAttrs,
                       reorgAttrs,
                       fileResolver,
                       dxApi,
                       logger) match {
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
  }
}
