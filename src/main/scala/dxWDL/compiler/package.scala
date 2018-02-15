package dxWDL.compiler

case class CompiledNamespace(workflow: Option[DXWorkflow],
                             applets: Vector[DXApplet],
                             subWorkflows: Vector[DXWorkflow])
