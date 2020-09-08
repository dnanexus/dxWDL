package dx.executor.wdl

import dx.core.ir.Type
import dx.core.languages.wdl.{Utils => WdlUtils}
import dx.executor.{JobMeta, WorkflowSupport, WorkflowSupportFactory}
import wdlTools.syntax.WdlVersion
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}

case class WdlWorkflowSupport(workflow: TAT.Workflow,
                              wdlVersion: WdlVersion,
                              tasks: Map[String, TAT.Task],
                              wdlTypeAliases: Map[String, WdlTypes.T_Struct],
                              jobMeta: JobMeta)
    extends WorkflowSupport(jobMeta) {
  override def typeAliases: Map[String, Type] = WdlUtils.toIRTypeMap(wdlTypeAliases)
}

case class WdlWorkflowSupportFactory() extends WorkflowSupportFactory {
  override def create(jobMeta: JobMeta): Option[WdlWorkflowSupport] = {
    val (doc, typeAliases) =
      try {
        WdlUtils.parseSource(jobMeta.sourceCode, jobMeta.fileResolver)
      } catch {
        case _: Throwable =>
          return None
      }
    val workflow = doc.workflow.getOrElse(
        throw new RuntimeException("This document should have a workflow")
    )
    val tasks = doc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    Some(WdlWorkflowSupport(workflow, doc.version.value, tasks, typeAliases.bindings, jobMeta))
  }
}
