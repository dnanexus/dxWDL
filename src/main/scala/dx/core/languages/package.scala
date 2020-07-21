package dx.core.languages

import wdlTools.syntax.WdlVersion

object Language extends Enumeration {
  type Language = Value
  val WDLvDraft2, WDLv1_0, WDLv2_0, CWLv1_2 = Value
  val Default: Language = WDLv1_0

  def toWdlVersion(value: Value): WdlVersion = {
    value match {
      case WDLvDraft2 => WdlVersion.Draft_2
      case WDLv1_0    => WdlVersion.V1
      case WDLv2_0    => WdlVersion.V2
      case other      => throw new Exception(s"${other} is not a wdl version")
    }
  }

  def fromWdlVersion(version: WdlVersion): Value = {
    version match {
      case WdlVersion.Draft_2 => Language.WDLvDraft2
      case WdlVersion.V1      => Language.WDLv1_0
      case WdlVersion.V2      => Language.WDLv2_0
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
    version match {
      case None => Default
      case Some(buf) =>
        (normalizeVersion(buf), hint.map(normalizeVersion)) match {
          case ("wdl draft2", _)                        => Language.WDLvDraft2
          case ("draft2", Some("wdl"))                  => Language.WDLvDraft2
          case ("wdl draft3" | "wdl 10", _)             => Language.WDLv1_0
          case ("draft3" | "10", Some("wdl"))           => Language.WDLv1_0
          case ("wdl development" | "wdl 20", _)        => Language.WDLv2_0
          case ("development" | "20", Some("wdl"))      => Language.WDLv2_0
          case ("cwl v120dev4", _)                      => Language.CWLv1_2
          case (v, _) if v.startsWith("cwl v120")       => Language.CWLv1_2
          case (v, Some("cwl")) if v.startsWith("v120") => Language.CWLv1_2
        }
      case other => throw new Exception(s"Unrecognized/unsupported language ${other}")
    }
  }
}

object IORef extends Enumeration {
  type IORef = Value
  val Input, Output = Value
}
