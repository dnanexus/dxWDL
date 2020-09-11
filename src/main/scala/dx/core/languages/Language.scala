package dx.core.languages

import wdlTools.syntax.WdlVersion
import wdlTools.util.Enum

object Language extends Enum {
  type Language = Value
  val WdlVDraft2, WdlV1_0, WdlV2_0, CwlV1_2 = Value
  val WdlDefault: Language = WdlV1_0
  val CwlDefault: Language = CwlV1_2

  def toWdlVersion(value: Value): WdlVersion = {
    value match {
      case WdlVDraft2 => WdlVersion.Draft_2
      case WdlV1_0    => WdlVersion.V1
      case WdlV2_0    => WdlVersion.V2
      case other      => throw new Exception(s"${other} is not a wdl version")
    }
  }

  def fromWdlVersion(version: WdlVersion): Value = {
    version match {
      case WdlVersion.Draft_2 => Language.WdlVDraft2
      case WdlVersion.V1      => Language.WdlV1_0
      case WdlVersion.V2      => Language.WdlV2_0
      case other              => throw new Exception(s"Unsupported dielect ${other}")
    }
  }

  private def normalizeVersion(version: String): String = {
    version.trim.toLowerCase.replaceAll("[._-]", "")
  }

  /**
    * Parse a version string.
    * @param version The version specifier. Should be "<language> <version>", but the language may be omitted if
    *                `hint` is specified.
    * @param hint Hint about which language is being used, when `version` does not include the language specifier.
    * @return
    */
  def parse(version: Option[String], hint: Option[String] = None): Value = {
    (version.map(normalizeVersion), hint.map(normalizeVersion)) match {
      case (None, Some("wdl"))                            => WdlDefault
      case (None, Some("cwl"))                            => CwlDefault
      case (Some("wdl"), _)                               => WdlDefault
      case (Some("cwl"), _)                               => CwlDefault
      case (Some("wdl draft2"), _)                        => Language.WdlVDraft2
      case (Some("draft2"), Some("wdl"))                  => Language.WdlVDraft2
      case (Some("wdl draft3" | "wdl 10"), _)             => Language.WdlV1_0
      case (Some("draft3" | "10"), Some("wdl"))           => Language.WdlV1_0
      case (Some("wdl development" | "wdl 20"), _)        => Language.WdlV2_0
      case (Some("development" | "20"), Some("wdl"))      => Language.WdlV2_0
      case (Some(v), _) if v.startsWith("cwl v120")       => Language.CwlV1_2
      case (Some(v), Some("cwl")) if v.startsWith("v120") => Language.CwlV1_2
      case other =>
        throw new Exception(s"Unrecognized/unsupported language ${other}")
    }
  }
}
