package dx.core

object Native {
  // keys used in details of native applets
  val ExecLinkInfo: String = "execLinkInfo"
  val BlockPath: String = "blockPath"
  val WfFragmentInputs: String = "fqnDictTypes"
  val InstanceTypeDb: String = "instanceTypeDB"
  val RuntimeAttributes: String = "runtimeAttrs"
  val CompilerTag = "dxCompiler"
  val SourceCode: String = "sourceCode"
  val Language: String = "language"
  val ScatterChunkSize = "scatterChunkSize"
  val Checksum = "checksum"
  val Version = "version"

  // parameter names used in "special" native applets
  val ReorgStatus = "reorg_status___"
  val ReorgStatusCompleted = "completed"
  val ContinueStart = "continue_start___"

  // Limits imposed on native apps.
  val JobsPerScatterLimit = 1000
  val JobPerScatterDefault = 500
  // Very long strings cause problems with bash and the UI, so we set
  // a max limit of 32k characters
  val StringLengthLimit: Int = 32 * 1024
}
