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

        // recurse into all the sub-namespaces
        ns.namespaces.foreach(xNs => apply(xNs))
    }
}

object Validate {
    def apply(ns: WdlNamespace, verbose: Verbose) : Unit = {
        val cef = new CompilerErrorFormatter(ns.terminalMap)
        val v = new Validate(cef, verbose)
        v.apply(ns)
    }
}
