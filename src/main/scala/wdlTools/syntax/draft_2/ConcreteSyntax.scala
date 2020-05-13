package wdlTools.syntax.draft_2

import java.net.URL

import wdlTools.syntax.{CommentMap, TextSource}

// A parser based on a WDL grammar written by Patrick Magee. The tool
// underlying the grammar is Antlr4.
//

// A concrete syntax for draft-2 of the Workflow Description Language (WDL). This shouldn't be used
// outside this package. Please use the abstract syntax instead.
object ConcreteSyntax {

  sealed trait Element {
    val text: TextSource // where in the source program does this element belong
  }
  sealed trait WorkflowElement extends Element
  sealed trait DocumentElement extends Element

  // type system
  sealed trait Type extends Element
  case class TypeOptional(t: Type, text: TextSource) extends Type
  case class TypeArray(t: Type, nonEmpty: Boolean, text: TextSource) extends Type
  case class TypeMap(k: Type, v: Type, text: TextSource) extends Type
  case class TypePair(l: Type, r: Type, text: TextSource) extends Type
  case class TypeString(text: TextSource) extends Type
  case class TypeFile(text: TextSource) extends Type
  case class TypeBoolean(text: TextSource) extends Type
  case class TypeInt(text: TextSource) extends Type
  case class TypeFloat(text: TextSource) extends Type
  case class TypeIdentifier(id: String, text: TextSource) extends Type
  case class TypeObject(text: TextSource) extends Type

  // expressions
  sealed trait Expr extends Element
  case class ExprString(value: String, text: TextSource) extends Expr
  case class ExprBoolean(value: Boolean, text: TextSource) extends Expr
  case class ExprInt(value: Int, text: TextSource) extends Expr
  case class ExprFloat(value: Double, text: TextSource) extends Expr

  // represents strings with interpolation.
  // For example:
  //  "some string part ~{ident + ident} some string part after"
  case class ExprCompoundString(value: Vector[Expr], text: TextSource) extends Expr
  case class ExprMapItem(key: Expr, value: Expr, text: TextSource) extends Expr
  case class ExprMapLiteral(value: Vector[ExprMapItem], text: TextSource) extends Expr
  case class ExprObjectMember(key: String, value: Expr, text: TextSource) extends Expr
  case class ExprObjectLiteral(value: Vector[ExprObjectMember], text: TextSource) extends Expr
  case class ExprArrayLiteral(value: Vector[Expr], text: TextSource) extends Expr

  case class ExprIdentifier(id: String, text: TextSource) extends Expr

  // These are parts of string interpolation expressions like:
  //
  // ${true="--yes" false="--no" boolean_value}
  // ${default="foo" optional_value}
  // ${sep=", " array_value}
  //
  trait PlaceHolderPart extends Element
  // true="--yes"    false="--no"
  case class ExprPlaceholderPartEqual(b: Boolean, value: Expr, text: TextSource)
      extends PlaceHolderPart
  // default="foo"
  case class ExprPlaceholderPartDefault(value: Expr, text: TextSource) extends PlaceHolderPart
  // sep=", "
  case class ExprPlaceholderPartSep(value: Expr, text: TextSource) extends PlaceHolderPart

  // These are full expressions of the same kind
  //
  // ${true="--yes" false="--no" boolean_value}
  // ${default="foo" optional_value}
  // ${sep=", " array_value}
  case class ExprPlaceholderEqual(t: Expr, f: Expr, value: Expr, text: TextSource) extends Expr
  case class ExprPlaceholderDefault(default: Expr, value: Expr, text: TextSource) extends Expr
  case class ExprPlaceholderSep(sep: Expr, value: Expr, text: TextSource) extends Expr

  case class ExprUniraryPlus(value: Expr, text: TextSource) extends Expr
  case class ExprUniraryMinus(value: Expr, text: TextSource) extends Expr
  case class ExprLor(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprLand(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprNegate(value: Expr, text: TextSource) extends Expr
  case class ExprEqeq(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprLt(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprGte(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprNeq(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprLte(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprGt(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprAdd(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprSub(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprMod(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprMul(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprDivide(a: Expr, b: Expr, text: TextSource) extends Expr
  case class ExprPair(l: Expr, r: Expr, text: TextSource) extends Expr
  case class ExprAt(array: Expr, index: Expr, text: TextSource) extends Expr
  case class ExprApply(funcName: String, elements: Vector[Expr], text: TextSource) extends Expr
  case class ExprIfThenElse(cond: Expr, tBranch: Expr, fBranch: Expr, text: TextSource) extends Expr
  case class ExprGetName(e: Expr, id: String, text: TextSource) extends Expr

  case class Declaration(name: String, wdlType: Type, expr: Option[Expr], text: TextSource)
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

  case class RuntimeKV(id: String, expr: Expr, text: TextSource) extends Element
  case class RuntimeSection(kvs: Vector[RuntimeKV], text: TextSource) extends Element

  // meta section
  case class MetaKV(id: String, value: String, text: TextSource) extends Element
  case class ParameterMetaSection(kvs: Vector[MetaKV], text: TextSource) extends Element
  case class MetaSection(kvs: Vector[MetaKV], text: TextSource) extends Element

  // imports
  case class ImportAddr(value: String, text: TextSource) extends Element
  case class ImportName(value: String, text: TextSource) extends Element

  // import statement as read from the document
  case class ImportDoc(name: Option[ImportName], addr: ImportAddr, text: TextSource)
      extends DocumentElement

  // top level definitions
  case class Task(name: String,
                  input: Option[InputSection],
                  output: Option[OutputSection],
                  command: CommandSection, // the command section is required
                  declarations: Vector[Declaration],
                  meta: Option[MetaSection],
                  parameterMeta: Option[ParameterMetaSection],
                  runtime: Option[RuntimeSection],
                  text: TextSource)
      extends DocumentElement

  case class CallAlias(name: String, text: TextSource) extends Element
  case class CallInput(name: String, expr: Expr, text: TextSource) extends Element
  case class CallInputs(value: Vector[CallInput], text: TextSource) extends Element
  case class Call(name: String,
                  alias: Option[CallAlias],
                  inputs: Option[CallInputs],
                  text: TextSource)
      extends WorkflowElement
  case class Scatter(identifier: String,
                     expr: Expr,
                     body: Vector[WorkflowElement],
                     text: TextSource)
      extends WorkflowElement
  case class Conditional(expr: Expr, body: Vector[WorkflowElement], text: TextSource)
      extends WorkflowElement

  case class Workflow(name: String,
                      input: Option[InputSection],
                      output: Option[OutputSection],
                      meta: Option[MetaSection],
                      parameterMeta: Option[ParameterMetaSection],
                      body: Vector[WorkflowElement],
                      text: TextSource)
      extends Element

  case class Document(docSourceUrl: URL,
                      docSource: String,
                      elements: Vector[DocumentElement],
                      workflow: Option[Workflow],
                      text: TextSource,
                      comments: CommentMap)
      extends Element
}
