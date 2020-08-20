package dx.compiler.ir

import java.nio.file.Path

import dx.compiler.DxNativeInterface
import dx.core.languages.Language.Language

/**
  * Provide support for language-specific features.
  * @tparam T language-specific value type
  */
trait LanguageSupport[T] {
  val language: Language

  def getExtras: Option[Extras[T]]

  def getDxNativeInterface: DxNativeInterface

  def getTranslator: Translator
}

trait LanguageSupportFactory[T] {
  def create(language: Language, extrasPath: Option[Path]): Option[LanguageSupport[T]]

  def create(sourceFile: Path, extrasPath: Option[Path]): Option[LanguageSupport[T]]
}
