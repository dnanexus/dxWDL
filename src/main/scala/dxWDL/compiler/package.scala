package dxWDL.compiler

import com.dnanexus.{DXApplet, DXWorkflow}

// WDL namespace, compiled to dnanexus
sealed trait DxWdlNamespace {
    def name : String
    def importedAs: Option[String]
    def applets: Map[String, (IR.Applet, DXApplet)]
}

case class DxWdlNamespaceLeaf(name: String,
                              importedAs: Option[String],
                              applets: Map[String, (IR.Applet, DXApplet)]) extends DxWdlNamespace
case class DxWdlNamespaceNode(name: String,
                              importedAs: Option[String],
                              applets: Map[String, (IR.Applet, DXApplet)],
                              workflow: (IR.Workflow, DXWorkflow),
                              children: Vector[DxWdlNamespace]) extends DxWdlNamespace
