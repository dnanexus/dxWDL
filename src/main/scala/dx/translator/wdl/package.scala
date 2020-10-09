/**
  * Wrappers around WDL-specific document elements, used by the Compiler
  * when generating apps/workflows.
  */
package dx.translator.wdl

import dx.core.ir.{DocumentSource, WorkflowSource}
import dx.core.languages.wdl.VersionSupport
import wdlTools.types.{TypedAbstractSyntax => TAT}

case class WdlDocumentSource(doc: TAT.Document, versionSupport: VersionSupport)
    extends DocumentSource {
  override def toString: String = versionSupport.generateDocument(doc)
}

case class WdlWorkflowSource(workflow: TAT.Workflow, versionSupport: VersionSupport)
    extends WorkflowSource {
  override def toString: String = versionSupport.generateElement(workflow)
}
