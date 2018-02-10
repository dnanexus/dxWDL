package dxWDL.compiler

//
//

import wdl.{WdlNamespace, WdlTask}

object NamespaceOps {

    sealed trait NsTree {
        def name : String
        def prettyPrint : String
        def transform(f: WdlWorkflow => WdlWorkflow) : NsTree
        def whitewash : WdlNamespace
    }

    // A namespace that is a library of tasks; it has no workflow
    case class NsTreeLeaf(ns: WdlNamespaceWithWorkflow) {
        def name = {
            ns.importUri match {
                case None => "Unknown filename"
                case Some(x) => x
            }
        }

        def prettyPrint : String = {
            WdlPrettyPrinter.apply(ns, 0)
        }

        // There is nothing to do here, there is no workflow
        def transform(f: WdlWorkflow => WdlWorkflow) : NsTree = {
            this
        }

        def whitewash : WdlNamespace = {
            ns
        }
    }

    case class NsTreeNode(nswf: WdlNamespaceWithWorkflow,
                          children: Vector[NsTree]) {
        def name = {
            ns.importUri match {
                case None => "Unknown filename"
                case Some(x) => x
            }
        }

        def prettyPrint : String = {
            val topWf = this.name + "\n" + WdlPrettyPrinter.apply(nswf, 0)

            val childrenStrings = children.map{ child =>
                child.name + "\n" + child.prettyPrint
            }.toVector

            (topWf +: childrenString).mkString("\n\n")
        }

        def transform(f: WdlWorkflow => WdlWorkflow) : NsTree = ???
        def whitewash : WdlNamespace = ???
    }


    def load(ns: WdlNamespace,
             wdlSourceFile: Path) : NsTree = {
    }
}
