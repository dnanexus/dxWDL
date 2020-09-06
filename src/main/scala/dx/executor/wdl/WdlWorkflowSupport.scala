package dx.executor.wdl

import dx.executor.WorkflowSupportFactory

case class WdlWorkflowSupport() {}

case class WdlWorkflowSupportFactory() extends WorkflowSupportFactory {
  def create: WdlWorkflowSupport = {}
}
