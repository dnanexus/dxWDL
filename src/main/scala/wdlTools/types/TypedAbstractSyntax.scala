package wdlTools.types

import java.net.URL
import wdlTools.syntax.{CommentMap, TextSource, WdlVersion}
import wdlTools.types.WdlTypes

// A tree representing a WDL program with all of the types in place.
object TypedAbstractSyntax {
  type WdlType = WdlTypes.T

  trait Element {
    val text: TextSource // where in the source program does this element belong
  }
  sealed trait WorkflowElement extends Element
  sealed trait DocumentElement extends Element
  sealed trait Callable {
    val name: String
    val wdlType: WdlTypes.T_Callable
  }

  // expressions
  sealed trait Expr extends Element {
    val wdlType: WdlType
  }

  // values
  case class ValueNull(wdlType: WdlType, text: TextSource) extends Expr
  case class ValueBoolean(value: Boolean, wdlType: WdlType, text: TextSource) extends Expr
  case class ValueInt(value: Int, wdlType: WdlType, text: TextSource) extends Expr
  case class ValueFloat(value: Double, wdlType: WdlType, text: TextSource) extends Expr
  case class ValueString(value: String, wdlType: WdlType, text: TextSource) extends Expr
  case class ValueFile(value: String, wdlType: WdlType, text: TextSource) extends Expr

  case class ExprIdentifier(id: String, wdlType: WdlType, text: TextSource) extends Expr

  // represents strings with interpolation. These occur only in command blocks.
  // For example:
  //  "some string part ~{ident + ident} some string part after"
  case class ExprCompoundString(value: Vector[Expr], wdlType: WdlType, text: TextSource)
      extends Expr

  case class ExprPair(l: Expr, r: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprArray(value: Vector[Expr], wdlType: WdlType, text: TextSource) extends Expr
  case class ExprMap(value: Map[Expr, Expr], wdlType: WdlType, text: TextSource) extends Expr
  case class ExprObject(value: Map[String, Expr], wdlType: WdlType, text: TextSource) extends Expr

  // These are expressions of kind:
  //
  // ~{true="--yes" false="--no" boolean_value}
  // ~{default="foo" optional_value}
  // ~{sep=", " array_value}
  //
  case class ExprPlaceholderEqual(t: Expr, f: Expr, value: Expr, wdlType: WdlType, text: TextSource)
      extends Expr
  case class ExprPlaceholderDefault(default: Expr, value: Expr, wdlType: WdlType, text: TextSource)
      extends Expr
  case class ExprPlaceholderSep(sep: Expr, value: Expr, wdlType: WdlType, text: TextSource)
      extends Expr

  // operators on one argument
  case class ExprUniraryPlus(value: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprUniraryMinus(value: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprNegate(value: Expr, wdlType: WdlType, text: TextSource) extends Expr

  // operators on two arguments
  case class ExprLor(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprLand(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprEqeq(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprLt(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprGte(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprNeq(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprLte(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprGt(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprAdd(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprSub(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprMod(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprMul(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr
  case class ExprDivide(a: Expr, b: Expr, wdlType: WdlType, text: TextSource) extends Expr

  // Access an array element at [index]
  case class ExprAt(array: Expr, index: Expr, wdlType: WdlType, text: TextSource) extends Expr

  // conditional:
  // if (x == 1) then "Sunday" else "Weekday"
  case class ExprIfThenElse(cond: Expr,
                            tBranch: Expr,
                            fBranch: Expr,
                            wdlType: WdlType,
                            text: TextSource)
      extends Expr

  // Apply a standard library function to arguments. For example:
  //   read_int("4")
  case class ExprApply(funcName: String, elements: Vector[Expr], wdlType: WdlType, text: TextSource)
      extends Expr

  // Access a field in a struct or an object. For example:
  //   Int z = x.a
  case class ExprGetName(e: Expr, id: String, wdlType: WdlType, text: TextSource) extends Expr

  case class Declaration(name: String, wdlType: WdlType, expr: Option[Expr], text: TextSource)
      extends WorkflowElement

  // sections
  /** In draft-2 there is no `input {}` block. Bound and unbound declarations may be mixed together
    * and bound declarations that require evaluation cannot be treated as inputs. Thus, the draft-2
    * `InputSection` `TextSource` may overlap with other elements.
    */
  case class InputSection(declarations: Vector[Declaration], text: TextSource) extends Element
  case class OutputSection(declarations: Vector[Declaration], text: TextSource) extends Element

  // A command can be simple, with just one continuous string:
  //
  // command {
  //     ls
  // }
  //
  // It can also include several embedded expressions. For example:
  //
  // command <<<
  //     echo "hello world"
  //     ls ~{input_file}
  //     echo ~{input_string}
  // >>>
  case class CommandSection(parts: Vector[Expr], text: TextSource) extends Element
  case class RuntimeSection(kvs: Map[String, Expr], text: TextSource) extends Element

  // A specialized JSON-like object language for meta values only.
  sealed trait MetaValue
  case object MetaNull extends MetaValue
  case class MetaBoolean(value: Boolean) extends MetaValue
  case class MetaInt(value: Int) extends MetaValue
  case class MetaFloat(value: Double) extends MetaValue
  case class MetaString(value: String) extends MetaValue
  case class MetaObject(value: Map[String, MetaValue]) extends MetaValue
  case class MetaArray(value: Vector[MetaValue]) extends MetaValue

  // the parameter sections have mappings from keys to json-like objects
  case class ParameterMetaSection(kvs: Map[String, MetaValue], text: TextSource) extends Element
  case class MetaSection(kvs: Map[String, MetaValue], text: TextSource) extends Element

  case class Version(value: WdlVersion, text: TextSource) extends DocumentElement

  // import statement with the typed-AST for the referenced document
  case class ImportAlias(id1: String, id2: String, text: TextSource) extends Element
  case class ImportDoc(name: Option[String],
                       aliases: Vector[ImportAlias],
                       addr: String,
                       doc: Document,
                       text: TextSource)
      extends DocumentElement

  // A task
  case class Task(name: String,
                  wdlType: WdlTypes.T_Task,
                  input: Option[InputSection],
                  output: Option[OutputSection],
                  command: CommandSection, // the command section is required
                  declarations: Vector[Declaration],
                  meta: Option[MetaSection],
                  parameterMeta: Option[ParameterMetaSection],
                  runtime: Option[RuntimeSection],
                  text: TextSource)
      extends DocumentElement
      with Callable

  // The name of the call may not contain dots. Examples:
  //
  // fully-qualified-name          actual-name
  // -----                         ---
  // call lib.concat as concat     concat
  // call add                      add
  // call a.b.c                    c
  //
  case class Call(fullyQualifiedName: String,
                  wdlType: WdlTypes.T_Call,
                  alias: Option[String],
                  actualName: String,
                  inputs: Map[String, Expr],
                  text: TextSource)
      extends WorkflowElement

  case class Scatter(identifier: String,
                     expr: Expr,
                     body: Vector[WorkflowElement],
                     text: TextSource)
      extends WorkflowElement

  case class Conditional(expr: Expr, body: Vector[WorkflowElement], text: TextSource)
      extends WorkflowElement

  // A workflow
  case class Workflow(name: String,
                      wdlType: WdlTypes.T_Workflow,
                      input: Option[InputSection],
                      output: Option[OutputSection],
                      meta: Option[MetaSection],
                      parameterMeta: Option[ParameterMetaSection],
                      body: Vector[WorkflowElement],
                      text: TextSource)
      extends Element
      with Callable

  case class Document(docSourceUrl: URL,
                      sourceCode: String,
                      version: Version,
                      elements: Vector[DocumentElement],
                      workflow: Option[Workflow],
                      text: TextSource,
                      comments: CommentMap)
      extends Element
}
