package dx.compiler.wdl

/**
  * A unification of WDL Runtime and Hints, with version-specific support.
  */
object Runtime {
  val HINT_ACCESS = "dx_access"
  val HINT_IGNORE_REUSE = "dx_ignore_reuse"
  val HINT_INSTANCE_TYPE = "dx_instance_type"
  //val HINT_REGIONS = "dx_regions"
  val HINT_RESTART = "dx_restart"
  val HINT_TIMEOUT = "dx_timeout"
  val HINT_APP_TYPE = "type"
  val HINT_APP_ID = "id"
  // This key is used in the restart object value to represent "*"
  val ALL_KEY = "All"
}
