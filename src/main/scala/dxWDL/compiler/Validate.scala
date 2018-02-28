package dxWDL.compiler

// Validate the correctness of the WDL files before trying to actually compile.
// Try to put all of the exceptions here, because then matching the error
// with the source WDL is straight-forward
//
import dxWDL.{CompilerErrorFormatter, Utils, Verbose}
import wdl._
import wdl4s.parser.WdlParser.Ast

case class Validate(cef: CompilerErrorFormatter,
                    verbose: Verbose) {

    // Make a pass on all declarations amd calls. Make sure no reserved words or prefixes
    // are used.
    private def checkReservedWords(ns: WdlNamespace) : Unit = {
        def checkVarName(varName: String, ast: Ast) : Unit = {
            if (Utils.isGeneratedVar(varName))
                throw new Exception(cef.illegalVariableName(ast))
            Utils.reservedSubstrings.foreach{ s =>
                if (varName contains s)
                    throw new Exception(cef.illegalVariableName(ast))
            }
        }
        def checkCallName(call : WdlCall) : Unit = {
            val nm = call.unqualifiedName
            Utils.reservedAppletPrefixes.foreach{ prefix =>
                if (nm.startsWith(prefix))
                    throw new Exception(cef.illegalCallName(call))
            }
            Utils.reservedSubstrings.foreach{ sb =>
                if (nm contains sb)
                    throw new Exception(cef.illegalCallName(call))
            }
            if (nm == Utils.LAST_STAGE)
                throw new Exception(cef.illegalCallName(call))
        }
        def deepCheck(children: Seq[Scope]) : Unit = {
            children.foreach {
                case ssc:Scatter =>
                    checkVarName(ssc.item, ssc.ast)
                    deepCheck(ssc.children)
                case decl:DeclarationInterface =>
                    checkVarName(decl.unqualifiedName, decl.ast)
                case call:WdlCall =>
                    checkCallName(call)
                case _ => ()
            }
        }
        ns match {
            case nswf: WdlNamespaceWithWorkflow => deepCheck(nswf.workflow.children)
            case _ => ()
        }
        ns.tasks.map{ task =>
            // check task inputs and outputs
            deepCheck(task.outputs)
            deepCheck(task.declarations)
        }
    }

    private def validateTask(task: WdlTask) : Unit = {
        // validate runtime attributes
        val validAttrNames:Set[String] = Set(Utils.DX_INSTANCE_TYPE_ATTR,
                                             "memory", "disks", "cpu", "docker")
        task.runtimeAttributes.attrs.foreach{ case (attrName,_) =>
            if (!(validAttrNames contains attrName))
                Utils.warning(verbose,
                              s"Runtime attribute ${attrName} for task ${task.name} is unknown")
        }
    }

    def apply(ns: WdlNamespace) : Unit = {
        checkReservedWords(ns)
        ns.tasks.foreach(t => validateTask(t))
    }
}

object Validate {
    // We need to map the WDL namespace hierarchy to a flat space of
    // dx:applets and dx:workflows in the project and folder.
    //
    // A task, similarly workflow, can be defined more than once in a
    // complex namespace with imports. We choose to compile it to an
    // applet with its unqualified name.
    //
    // This method makes sure each applet and workflow are
    // defined exactly once, and is uniquely named by its
    // unqualified name.
    private def checkFlatNamespace(ns: WdlNamespace,
                                   verbose: Verbose) : Unit = {
        // make a flat list of all referenced namespaces and sub-namespaces
        val allNs: Vector[WdlNamespace] = ns.allNamespacesRecursively.toVector

        // make sure tasks are unique
        val allTasks: Set[WdlTask] = allNs.map{ ns => ns.tasks }.flatten.toSet
        val allTaskNames : Set[String] = allTasks.map(_.unqualifiedName)
        val taskCounts: Map[String, Int] = allTaskNames.groupBy(x => x).mapValues(_.size)
        taskCounts.foreach{ case (taskName, nAppear) =>
            if (nAppear > 1)
                throw new Exception(s"""|Task ${taskName} appears ${nAppear} times. It has to
                                        |be unique in order to be compiled to a single
                                        |dnanexus applet""".stripMargin.trim)
        }

        // make sure workflow names are unique
        val allWorkflows: Set[WdlWorkflow] = allNs.map{ ns => ns.workflows }.flatten.toSet
        val allWorkflowNames : Set[String] = allWorkflows.map(_.unqualifiedName)
        val wfCounts: Map[String, Int] = allWorkflowNames.groupBy(x => x).mapValues(_.size)
        wfCounts.foreach{ case (wfName, nAppear) =>
            if (nAppear > 1)
                throw new Exception(s"""|Workflow ${wfName} appears ${nAppear} times. It has to
                                        |be unique in order to be compiled to a single
                                        |dnanexus workflow""".stripMargin.trim)
        }

        // make sure there is no intersection between applet and workflow names
        val allNames = allTaskNames ++ allWorkflowNames
        val counts: Map[String, Int] = allNames.groupBy(x => x).mapValues(_.size)
        counts.foreach{ case (name, nAppear) =>
            if (nAppear > 1)
                throw new Exception(s"Name ${name} is used for an applet and a workflow.")
        }
    }

    def validateNamespace(ns: WdlNamespace,
                          verbose: Verbose) : Unit = {
        // check this namespace
        val cef = new CompilerErrorFormatter(ns.resource, ns.terminalMap)
        val v = new Validate(cef, verbose)
        v.apply(ns)

        // recurse into all the sub-namespaces
        ns.namespaces.foreach(xNs => validateNamespace(xNs, verbose))
    }

    def apply(ns: WdlNamespace,
              verbose: Verbose) : Unit = {
        checkFlatNamespace(ns, verbose)
        validateNamespace(ns, verbose)
    }
}
