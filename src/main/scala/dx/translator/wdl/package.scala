/**
  * Wrappers around WDL-specific document elements, used by the Compiler
  * when generating apps/workflows.
  */
package dx.translator.wdl

import dx.core.ir.{ApplicationSource, DocumentSource, WorkflowSource}
import dx.core.languages.wdl.WdlUtils
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypedAbstractSyntax => TAT}

case class WdlDocumentSource(doc: TAT.Document) extends DocumentSource {
  override def toString: String = WdlUtils.generateDocument(doc)
}

case class WdlApplicationSource(task: TAT.Task, wdlVersion: WdlVersion) extends ApplicationSource {
  override def toString: String = WdlUtils.generateElement(task, wdlVersion)
}

case class WdlWorkflowSource(workflow: TAT.Workflow, wdlVersion: WdlVersion)
    extends WorkflowSource {
  override def toString: String = WdlUtils.generateElement(workflow, wdlVersion)
}
