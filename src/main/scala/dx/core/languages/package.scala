package dx.core.languages

import wdlTools.syntax.WdlVersion

object Language extends Enumeration {
  type Language = Value
  val WDLvDraft2, WDLv1_0, WDLv2_0, CWLv1_0 = Value

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
}

object IORef extends Enumeration {
  type IORef = Value
  val Input, Output = Value
}
