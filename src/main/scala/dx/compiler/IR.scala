package dx.compiler

import dx.api.{ConstraintOper, DxFile, DxWorkflowStage}
import dx.core.languages.wdl.WdlVarLinksConverter
import wdlTools.eval.WdlValues
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}

/**
  * Intermediate Representation (IR)
  *
  * Representation the compiler front end generates from a WDL workflow. The compiler back end uses it to generate
  * DNAnexus native applet(s), and ususally also workflow(s) except in the case of stand-alone tasks.
  */
object IR {
  // stages that the compiler uses in generated DNAx workflows
  val COMMON = "common"
  val EVAL_STAGE = "eval"
  val OUTPUT_SECTION = "outputs"
  val REORG = "reorg"
  val CUSTOM_REORG_CONFIG = "reorg_config"

  // The following keywords/types correspond to app(let) and workflow attributes from dxapp.json
  // (except for inputSpec/outputSpec attributes, which are defined separately). These attributes
  // can be used in the meta section of task WDL, and will be parsed out and used when generating
  // the native app.

  val META_CATEGORIES = "categories"
  val META_DESCRIPTION = "description"
  val META_DETAILS = "details"
  val META_DEVELOPER_NOTES = "developer_notes"
  val META_OPEN_SOURCE = "open_source"
  val META_PROPERTIES = "properties"
  val META_SUMMARY = "summary"
  val META_TAGS = "tags"
  val META_TITLE = "title"
  val META_TYPES = "types"
  val META_VERSION = "version"
  val META_CALL_NAMES = "call_names"
  val META_RUN_ON_SINGLE_NODE = "run_on_single_node"

  // This is equivalent to the MetaValue type from the typed-abstract-syntax. It
  // strips the text-source token so it would be easier to compare maps
  // and complex structures.
  sealed abstract class MetaValue
  final case object MetaValueNull extends MetaValue
  final case class MetaValueBoolean(value: Boolean) extends MetaValue
  final case class MetaValueInt(value: Int) extends MetaValue
  final case class MetaValueFloat(value: Double) extends MetaValue
  final case class MetaValueString(value: String) extends MetaValue
  final case class MetaValueObject(value: Map[String, MetaValue]) extends MetaValue
  final case class MetaValueArray(value: Vector[MetaValue]) extends MetaValue

  sealed abstract class TaskAttr
  final case class TaskAttrTitle(text: String) extends TaskAttr
  final case class TaskAttrDescription(text: String) extends TaskAttr
  final case class TaskAttrSummary(text: String) extends TaskAttr
  final case class TaskAttrDeveloperNotes(text: String) extends TaskAttr
  final case class TaskAttrVersion(text: String) extends TaskAttr
  final case class TaskAttrDetails(details: Map[String, MetaValue]) extends TaskAttr
  final case class TaskAttrOpenSource(isOpenSource: Boolean) extends TaskAttr
  final case class TaskAttrCategories(categories: Vector[String]) extends TaskAttr
  final case class TaskAttrTypes(types: Vector[String]) extends TaskAttr
  final case class TaskAttrTags(tags: Vector[String]) extends TaskAttr
  final case class TaskAttrProperties(properties: Map[String, String]) extends TaskAttr

  sealed abstract class WorkflowAttr
  final case class WorkflowAttrTitle(text: String) extends WorkflowAttr
  final case class WorkflowAttrDescription(text: String) extends WorkflowAttr
  final case class WorkflowAttrSummary(text: String) extends WorkflowAttr
  final case class WorkflowAttrVersion(text: String) extends WorkflowAttr
  final case class WorkflowAttrDetails(details: Map[String, MetaValue]) extends WorkflowAttr
  final case class WorkflowAttrTypes(types: Vector[String]) extends WorkflowAttr
  final case class WorkflowAttrTags(tags: Vector[String]) extends WorkflowAttr
  final case class WorkflowAttrProperties(properties: Map[String, String]) extends WorkflowAttr
  final case class WorkflowAttrCallNames(mapping: Map[String, String]) extends WorkflowAttr
  final case class WorkflowAttrRunOnSingleNode(value: Boolean) extends WorkflowAttr

  // The following keywords/types correspond to runtime-related attributes from dxapp.json.
  // Currently, these are searched for in the runtime section of WDL, but will move to hints in
  // WDL 2.0.

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

  sealed abstract class RuntimeHint
  final case class RuntimeHintRestart(max: Option[Int] = None,
                                      default: Option[Int] = None,
                                      errors: Option[Map[String, Int]] = None)
      extends RuntimeHint
  final case class RuntimeHintTimeout(days: Option[Int] = None,
                                      hours: Option[Int] = None,
                                      minutes: Option[Int] = None)
      extends RuntimeHint
  final case class RuntimeHintIgnoreReuse(value: Boolean) extends RuntimeHint
  final case class RuntimeHintAccess(network: Option[Vector[String]] = None,
                                     project: Option[String] = None,
                                     allProjects: Option[String] = None,
                                     developer: Option[Boolean] = None,
                                     projectCreation: Option[Boolean] = None)
      extends RuntimeHint

  // The following keywords/types correspond to attributes of inputSpec/outputSpec from
  // dxapp.json. These attributes can be used in the parameter_meta section of task WDL, and
  // will be parsed out and used when generating the native app.
  //  Example:
  //
  //  task {
  //    inputs {
  //      File sorted_bams
  //    }
  //    parameter_meta {
  //      sorted_bams: {
  //        label: "Sorted mappings",
  //        help: "A set of coordinate-sorted BAM files to be merged.",
  //        patterns: ["*.bam"]
  //      }
  //    }
  //  }
  //
  //  will be turned into:
  //
  //  "inputSpec": {
  //    "myparam": {
  //      "name": "sorted_bams",
  //      "label": "Sorted mappings",
  //      "help": "A set of coordinate-sorted BAM files to be merged.",
  //      "class": "array:file",
  //      "patterns": ["*.bam"]
  //    }
  //  }

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

  /** Compile time representation of the dxapp IO spec patterns
    *  Example:
    *  'patterns': { // PatternsReprObj
    *    'name': ['*.sam', '*.bam'],
    *    'class': 'file',
    *    'tag': ['foo', 'bar']
    *  }
    *   OR
    * 'patterns': ['*.sam', '*.bam'] // PatternsReprArray
    *
   **/
  sealed abstract class PatternsRepr
  final case class PatternsReprArray(patterns: Vector[String]) extends PatternsRepr
  final case class PatternsReprObj(name: Option[Vector[String]],
                                   klass: Option[String],
                                   tag: Option[Vector[String]])
      extends PatternsRepr

  // TODO: we can probably get rid of some of the repr types and just leave them as
  // MetaValue

  /** Compile time representation of the dxapp IO spec choices
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
  sealed abstract class ChoiceRepr
  final case class ChoiceReprString(value: String) extends ChoiceRepr
  final case class ChoiceReprInteger(value: Int) extends ChoiceRepr
  final case class ChoiceReprFloat(value: Double) extends ChoiceRepr
  final case class ChoiceReprBoolean(value: Boolean) extends ChoiceRepr
  final case class ChoiceReprFile(value: String, name: Option[String]) extends ChoiceRepr

  /** Compile time representation of the dxapp IO spec suggestions
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
  sealed abstract class SuggestionRepr
  sealed case class SuggestionReprString(value: String) extends SuggestionRepr
  sealed case class SuggestionReprInteger(value: Int) extends SuggestionRepr
  sealed case class SuggestionReprFloat(value: Double) extends SuggestionRepr
  sealed case class SuggestionReprBoolean(value: Boolean) extends SuggestionRepr
  sealed case class SuggestionReprFile(
      value: Option[String],
      name: Option[String],
      project: Option[String],
      path: Option[String]
  ) extends SuggestionRepr

  /** Compile-time representation of the dxapp IO spec 'type' value.
    * Type is either a string or a boolean "expression" represented as a hash value with a single
    * key ('$and' or '$or') and an array value that contains two or more expressions.
    *  Examples:
    *   type: "fastq"
    *
    *   type: { and: [ "fastq", { or: ["Read1", "Read2"] } ] }
  **/
  sealed abstract class ConstraintRepr
  sealed case class ConstraintReprString(constraint: String) extends ConstraintRepr
  sealed case class ConstraintReprOper(oper: ConstraintOper.Value,
                                       constraints: Vector[ConstraintRepr])
      extends ConstraintRepr

  /** Compile-time representation of the dxapp IO spec 'default' value.
    * The default value can be specified when defining the parameter. If it is not (for example,
    * if the parameter is optional and a separate variable is defined using select_first()), then
    * the default value can be specified in paramter_meta and will be used when the dxapp.json
    * is generated.
  **/
  sealed abstract class DefaultRepr
  final case class DefaultReprString(value: String) extends DefaultRepr
  final case class DefaultReprInteger(value: Int) extends DefaultRepr
  final case class DefaultReprFloat(value: Double) extends DefaultRepr
  final case class DefaultReprBoolean(value: Boolean) extends DefaultRepr
  final case class DefaultReprFile(value: String) extends DefaultRepr
  final case class DefaultReprArray(array: Vector[DefaultRepr]) extends DefaultRepr

  // Compile time representaiton of supported parameter_meta section
  // information for the dxapp IO spec.
  sealed abstract class IOAttr
  final case class IOAttrGroup(text: String) extends IOAttr
  final case class IOAttrHelp(text: String) extends IOAttr
  final case class IOAttrLabel(text: String) extends IOAttr
  final case class IOAttrPatterns(patternRepr: PatternsRepr) extends IOAttr
  final case class IOAttrChoices(choices: Vector[ChoiceRepr]) extends IOAttr
  final case class IOAttrSuggestions(suggestions: Vector[SuggestionRepr]) extends IOAttr
  final case class IOAttrType(constraint: ConstraintRepr) extends IOAttr
  final case class IOAttrDefault(value: DefaultRepr) extends IOAttr

  // Compile time representation of a variable. Used also as
  // an applet argument.
  //
  // The fullyQualifiedName could contains dots. However dx does not allow
  // dots in applet/workflow arugment names, this requires some kind
  // of transform.
  //
  // The attributes are used to encode DNAx applet input/output
  // specification fields, such as {help, suggestions, patterns}.
  //
  case class CVar(
      name: String,
      wdlType: WdlTypes.T,
      default: Option[WdlValues.V],
      attrs: Option[Vector[IOAttr]] = None
  ) {
    // dx does not allow dots in variable names, so we
    // convert them to underscores.
    //
    // TODO: check for collisions that are created this way.
    def dxVarName: String = {
      val nameNoDots = WdlVarLinksConverter.transformVarName(name)
      assert(!(nameNoDots contains "."))
      nameNoDots
    }
  }

  /** Specification of instance type.
    *
    *  An instance could be:
    *  Default: the platform default, useful for auxiliary calculations.
    *  Const:   instance type is known at compile time. We can start the
    *           job directly on the correct instance type.
    *  Runtime: WDL specifies a calculation for the instance type, based
    *           on information known only at runtime. The generated applet
    *           will need to evalulate the expressions at runtime, and then
    *           start another job on the correct instance type.
    */
  sealed trait InstanceType
  case object InstanceTypeDefault extends InstanceType
  case class InstanceTypeConst(
      dxInstanceType: Option[String],
      memoryMB: Option[Int],
      diskGB: Option[Int],
      cpu: Option[Int],
      gpu: Option[Boolean]
  ) extends InstanceType
  case object InstanceTypeRuntime extends InstanceType

  // A task may specify a docker image to run under. There are three
  // supported options:
  //  None:    no image
  //  Network: the image resides on a network site and requires download
  //  DxAsset: the image is a platform asset
  //
  sealed trait DockerImage
  case object DockerImageNone extends DockerImage
  case object DockerImageNetwork extends DockerImage
  case class DockerImageDxFile(url: String, tarball: DxFile) extends DockerImage

  // A unified type representing a WDL workflow or a WDL applet.
  // This is useful when compiling WDL workflows, because they can
  // call other WDL workflows and applets. This is done using the
  // same syntax.
  sealed trait Callable {
    def name: String
    def inputVars: Vector[CVar]
    def outputVars: Vector[CVar]
  }

  // There are several kinds of applets
  //   Native:     a native platform applet
  //   Task:       call a task, execute a shell command (usually)
  //   WfFragment: WDL workflow fragment, can included nested if/scatter blocks
  //   WfInputs:   handle workflow inputs for unlocked workflows
  //   WfOutputs:  evaluate workflow outputs
  //   WorkflowOutputReorg: move intermediate result files to a subdirectory.
  sealed trait AppletKind
  case class AppletKindNative(id: String) extends AppletKind
  case class AppletKindTask(task: TAT.Task) extends AppletKind
  case class AppletKindWfFragment(calls: Vector[String],
                                  blockPath: Vector[Int],
                                  fqnDictTypes: Map[String, WdlTypes.T])
      extends AppletKind
  case object AppletKindWfInputs extends AppletKind

  // Output - default and custom reorg
  case object AppletKindWfOutputs extends AppletKind
  case object AppletKindWfCustomReorgOutputs extends AppletKind

  // Reorg - default and custom reorg
  case object AppletKindWorkflowOutputReorg extends AppletKind
  case class AppletKindWorkflowCustomReorg(id: String) extends AppletKind

  /** @param name          Name of applet
    * @param inputs        input arguments
    * @param outputs       output arguments
    * @param instanceType  a platform instance name
    * @param docker        is docker used? if so, what image
    * @param kind          Kind of applet: task, scatter, ...
    * @param document          Task definition
    * @param meta          Additional applet metadata
    * @param runtimeHints  Runtime hints
    */
  case class Applet(name: String,
                    inputs: Vector[CVar],
                    outputs: Vector[CVar],
                    instanceType: InstanceType,
                    docker: DockerImage,
                    kind: AppletKind,
                    document: TAT.Document,
                    meta: Option[Vector[TaskAttr]] = None,
                    runtimeHints: Option[Vector[RuntimeHint]] = None)
      extends Callable {
    def inputVars: Vector[CVar] = inputs
    def outputVars: Vector[CVar] = outputs
  }

  /**
    * An input to a stage. Could be empty, a wdl constant,
    * a link to an output variable from another stage,
    * or a workflow input. The workflow input may have a
    * default value that is a complex expression that must
    * be evaluated at runtime.
    */
  sealed trait SArg
  case object SArgEmpty extends SArg
  case class SArgConst(wdlValue: WdlValues.V) extends SArg
  case class SArgLink(stageId: DxWorkflowStage, argName: CVar) extends SArg
  case class SArgWorkflowInput(arg: CVar, dynamicDefault: Boolean = false) extends SArg

  // A stage can call an applet or a workflow.
  //
  // Note: the description may contain dots, parentheses, and other special
  // symbols. It is shown to the user on the UI. The [id] is unique
  // across the workflow.
  case class Stage(description: String,
                   id: DxWorkflowStage,
                   calleeName: String,
                   inputs: Vector[SArg],
                   outputs: Vector[CVar])

  /** A workflow output is linked to the stage that
    * generated it.
    *
    * If [level] is SubWorkflow, then a workflow matches part of a
    * WDL workflow, it is not a first class citizen. It is compiled
    * into a hidden dx:workflow.
    */
  object Level extends Enumeration {
    type Level = Value
    val Top, Sub = Value
  }

  case class Workflow(name: String,
                      inputs: Vector[(CVar, SArg)],
                      outputs: Vector[(CVar, SArg)],
                      stages: Vector[Stage],
                      document: TAT.Workflow,
                      locked: Boolean,
                      level: Level.Value,
                      meta: Option[Vector[WorkflowAttr]] = None)
      extends Callable {
    def inputVars: Vector[CVar] = inputs.map { case (cVar, _)   => cVar }
    def outputVars: Vector[CVar] = outputs.map { case (cVar, _) => cVar }
  }

  // dependencies: the order in which to compile the workflows and tasks.
  // The first element in the vector depends on nothing else. Each other
  // element (may) depend on all previous elements.
  case class Bundle(primaryCallable: Option[Callable],
                    allCallables: Map[String, Callable],
                    dependencies: Vector[String],
                    typeAliases: Map[String, WdlTypes.T])
}
