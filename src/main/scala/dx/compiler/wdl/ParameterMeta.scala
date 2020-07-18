package dx.compiler.wdl

object ParameterMeta {
  // Keywords for string pattern matching in WDL parameter_meta
  val PARAM_META_CHOICES = "choices"
  val PARAM_META_DEFAULT = "default"
  val PARAM_META_DESCRIPTION = "description" // accepted as a synonym to 'help'
  val PARAM_META_GROUP = "group"
  val PARAM_META_HELP = "help"
  val PARAM_META_LABEL = "label"
  val PARAM_META_PATTERNS = "patterns"
  val PARAM_META_SUGGESTIONS = "suggestions"
  val PARAM_META_TYPE = "dx_type"

  val PARAM_META_CONSTRAINT_AND = "and"
  val PARAM_META_CONSTRAINT_OR = "or"
}
