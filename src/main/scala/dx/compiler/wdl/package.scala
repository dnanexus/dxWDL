package dx.compiler.wdl

import dx.compiler.{ApplicationSource, DocumentSource, WorkflowSource}
import dx.core.ir.{Parameter, ParameterAttribute, Type, Value}
import dx.core.languages.wdl.ParameterLinkSerde
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.util.Adjuncts

case class WdlBundle(version: WdlVersion,
                     primaryCallable: Option[TAT.Callable],
                     tasks: Map[String, TAT.Task],
                     workflows: Map[String, TAT.Workflow],
                     callableNames: Set[String],
                     sources: Map[String, TAT.Document],
                     adjunctFiles: Map[String, Vector[Adjuncts.AdjunctFile]])

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
case class WdlParameter(
    name: String,
    dxType: Type,
    defaultValue: Option[Value] = None,
    attributes: Vector[ParameterAttribute] = Vector.empty
) extends Parameter {
  // dx does not allow dots in variable names, so we
  // convert them to underscores.
  //
  // TODO: check for collisions that are created this way.
  def dxName: String = {
    val nameNoDots = ParameterLinkSerde.encodeDots(name)
    assert(!nameNoDots.contains("."))
    nameNoDots
  }
}

// wrappers around WDL-specific document elements, used by Native
// when generating apps/workflows
case class WdlDocumentSource(doc: TAT.Document) extends DocumentSource
case class WdlApplicationSource(task: TAT.Task) extends ApplicationSource
case class WdlWorkflowSource(workflow: TAT.Workflow) extends WorkflowSource
