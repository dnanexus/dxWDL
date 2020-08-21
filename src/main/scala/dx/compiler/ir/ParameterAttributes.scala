package dx.compiler.ir

import dx.api.ConstraintOper
import dx.core.ir.{ParameterAttribute, Value}

/**
  * These types correspond to attributes of inputSpec/outputSpec from dxapp.json.
  * These attributes can be used in the parameter_meta section of task WDL, and
  * will be parsed out and used when generating the native app.
  * Example:
  *
  * task {
  *   inputs {
  *     File sorted_bams
  *   }
  *   parameter_meta {
  *     sorted_bams: {
  *       label: "Sorted mappings",
  *       help: "A set of coordinate-sorted BAM files to be merged.",
  *       patterns: ["*.bam"]
  *     }
  *   }
  * }
  *
  * will be turned into:
  *
  * "inputSpec": {
  *   "myparam": {
  *     "name": "sorted_bams",
  *     "label": "Sorted mappings",
  *     "help": "A set of coordinate-sorted BAM files to be merged.",
  *     "class": "array:file",
  *     "patterns": ["*.bam"]
  *   }
  * }
  */
object ParameterAttributes {

  /**
    * Compile time representation of the dxapp IO spec patterns.
    * Example:
    *  'patterns': { // PatternsReprObj
    *    'name': ['*.sam', '*.bam'],
    *    'class': 'file',
    *    'tag': ['foo', 'bar']
    *  }
    *   OR
    * 'patterns': ['*.sam', '*.bam'] // PatternsReprArray
    *
   **/
  sealed abstract class Patterns
  final case class PatternsArray(patterns: Vector[String]) extends Patterns
  final case class PatternsObject(name: Vector[String] = Vector.empty,
                                  klass: Option[String] = None,
                                  tag: Vector[String] = Vector.empty)
      extends Patterns

  /**
    * Compile time representation of the dxapp IO spec choices.
    * Choices is an array of suggested values, where each value can be raw (a primitive type)
    * or, for file parameters, an annotated value (a hash with optional 'name' key and required
    * 'value' key).
    *  Examples:
    *   choices: [
    *     {
    *       name: "file1", value: "dx://file-XXX"
    *     },
    *     {
    *       name: "file2", value: "dx://file-YYY"
    *     }
    *   ]
    *
    *   choices: [true, false]  # => [true, false]
   **/
  sealed abstract class Choice
  final case class IntChoice(value: Long) extends Choice
  final case class FloatChoice(value: Double) extends Choice
  final case class StringChoice(value: String) extends Choice
  final case class BooleanChoice(value: Boolean) extends Choice
  final case class FileChoice(value: String, name: Option[String]) extends Choice
  final case class DirectoryChoice(value: String, name: Option[String]) extends Choice

  /**
    * Compile time representation of the dxapp IO spec suggestions
    * Suggestions is an array of suggested values, where each value can be raw (a primitive type)
    * or, for file parameters, an annotated value (a hash with optional 'name', 'value',
    * 'project', and 'path' keys).
    *  Examples:
    *   suggestions: [
    *     {
    *       name: "file1", value: "dx://file-XXX"
    *     },
    *     {
    *       name: "file2", project: "project-XXX", path: "/foo/bar.txt"
    *     }
    *   ]
    *
    *   suggestions: [1, 2, 3]
   **/
  sealed abstract class Suggestion
  sealed case class IntSuggestion(value: Long) extends Suggestion
  sealed case class FloatSuggestion(value: Double) extends Suggestion
  sealed case class StringSuggestion(value: String) extends Suggestion
  sealed case class BooleanSuggestion(value: Boolean) extends Suggestion
  sealed case class FileSuggestion(
      value: Option[String],
      name: Option[String],
      project: Option[String],
      path: Option[String]
  ) extends Suggestion
  sealed case class DirectorySuggestion(
      value: Option[String],
      name: Option[String],
      project: Option[String],
      path: Option[String]
  ) extends Suggestion

  /**
    * Compile-time representation of the dxapp IO spec 'type' value.
    * Type is either a string or a boolean "expression" represented as a hash value with a single
    * key ('$and' or '$or') and an array value that contains two or more expressions.
    *  Examples:
    *   type: "fastq"
    *
    *   type: { and: [ "fastq", { or: ["Read1", "Read2"] } ] }
   **/
  sealed abstract class Constraint
  sealed case class StringConstraint(constraint: String) extends Constraint
  sealed case class CompoundConstraint(oper: ConstraintOper.Value, constraints: Vector[Constraint])
      extends Constraint

  /**
    * Compile time representaiton of supported parameter_meta section
    * information for the dxapp IO spec.
    */
  final case class GroupAttribute(text: String) extends ParameterAttribute
  final case class HelpAttribute(text: String) extends ParameterAttribute
  final case class LabelAttribute(text: String) extends ParameterAttribute
  final case class PatternsAttribute(patternRepr: Patterns) extends ParameterAttribute
  final case class ChoicesAttribute(choices: Vector[Choice]) extends ParameterAttribute
  final case class SuggestionsAttribute(suggestions: Vector[Suggestion]) extends ParameterAttribute
  final case class TypeAttribute(constraint: Constraint) extends ParameterAttribute
  final case class DefaultAttribute(value: Value) extends ParameterAttribute
}
