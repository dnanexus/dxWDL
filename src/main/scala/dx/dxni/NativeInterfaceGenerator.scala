package dx.dxni

import dx.api.{DxApp, DxApplet}
import dx.core.languages.Language.Language

trait NativeInterfaceGenerator {
  def generate(apps: Vector[DxApp] = Vector.empty,
               applets: Vector[DxApplet] = Vector.empty,
               headerLines: Vector[String]): Vector[String]
}

trait NativeInterfaceGeneratorFactory {
  def create(language: Language): Option[NativeInterfaceGenerator]
}
