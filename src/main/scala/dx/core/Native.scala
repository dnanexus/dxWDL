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
  val CompilerTag = "dxCompiler"
  val SourceCode: String = "sourceCode"
  val Language: String = "language"
}

/**
  * Common property names.
  */
object NativeProperties {
  val Checksum = "dxCompiler_checksum"
  val Version = "dxCompiler_version"
}

object ReorgOutputs {
  val ReorgStatus = "reorg_status___"
  val ReorgStatusCompleted = "completed"
}
