package dx.core

/**
  * Keys used in the details of a generated native executable.
  */
object NativeDetails {
  val ExecLinkInfo: String = "execLinkInfo"
  val BlockPath: String = "blockPath"
  val WfFragmentInputs: String = "fqnDictTypes"
  val InstanceTypeDb: String = "instanceTypeDB"
  val RuntimeAttributes: String = "runtimeAttrs"
  // TODO: these should be changed to a non-WDL-specific values
  val CompilerTag = "dxWDL"
  val SourceCode: String = "wdlSourceCode"
}
