package dx.translator.wdl

import dx.core.ir.{ApplicationSource, DocumentSource, WorkflowSource}
import wdlTools.generators.code.WdlV1Generator
import wdlTools.types.{TypedAbstractSyntax => TAT}

// wrappers around WDL-specific document elements, used by Native
// when generating apps/workflows
case class WdlDocumentSource(doc: TAT.Document) extends DocumentSource {
  override def toString: String = {
    val generator = WdlV1Generator()
    val sourceLines = generator.generateDocument(doc)
    sourceLines.mkString("\n")
  }
}
case class WdlApplicationSource(task: TAT.Task) extends ApplicationSource {
  override def toString: String = {
    val generator = WdlV1Generator()
    val sourceLines = generator.generateElement(task)
    sourceLines.mkString("\n")
  }
}
case class WdlWorkflowSource(workflow: TAT.Workflow) extends WorkflowSource {
  override def toString: String = {
    val generator = WdlV1Generator()
    val sourceLines = generator.generateElement(workflow)
    sourceLines.mkString("\n")
  }
}
