package dxWDL.compiler

// Validate the correctness of the WDL files before trying to actually compile.
// Try to put all of the exceptions here, because then matching the error
// with the source WDL is straight-forward
//
import dxWDL._
import scala.collection.mutable.Queue
import wdl.draft2.model._
import wdl.draft2.parser.WdlParser.Ast
import wom.types._

case class Validate(allErrors :  Queue[String],
                    allWarnings : Queue[String],
                    cef: CompilerErrorFormatter,
                    verbose: Verbose) {
    val reservedWords = List(Utils.COMMON,
                             Utils.OUTPUT_SECTION,
                             Utils.REORG)

    // Make a pass on all declarations amd calls. Make sure no reserved words or prefixes
    // are used.
    private def checkReservedWords(ns: WdlNamespace) : Unit = {
        def checkVarName(varName: String, ast: Ast) : Unit = {
            /*for (word <- reservedWords) {
                if (varName == word)
                    recordError(cef.illegalVariableName(ast))
            }*/
        }
        def checkCallName(call : WdlCall) : Unit = {
            for (word <- reservedWords) {
                if (call.unqualifiedName == word)
                    allErrors += (cef.illegalCallName(call))
            }
        }
        def deepCheck(children: Seq[Scope]) : Unit = {
            children.foreach {
                case decl:DeclarationInterface =>
                    checkVarName(decl.unqualifiedName, decl.ast)
                case call:WdlCall =>
                    checkCallName(call)
                case ssc:Scatter =>
                    checkVarName(ssc.item, ssc.ast)
                    deepCheck(ssc.children)
                case ifStmt:If =>
                    deepCheck(ifStmt.children)
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
        task.runtimeAttributes.attrs.foreach{ case (attrName,_) =>
            if (!(Extras.RUNTIME_ATTRS contains attrName))
                allWarnings += (s"Runtime attribute ${attrName} for task ${task.name} is unknown")
        }
    }

    // Make sure we don't have partial output expressions like:
    //   output {
    //     Add.result
    //   }
    //
    // We only deal with fully specified outputs, like:
    //   output {
    //     File Add_result = Add.result
    //   }
    private def validateWorkflowOutputs(workflow: WdlWorkflow) : Unit = {
        val wfOutputs: Vector[WorkflowOutput] =
            workflow.children.filter(x => x.isInstanceOf[WorkflowOutput])
                .map(_.asInstanceOf[WorkflowOutput])
                .toVector
        try {
            // Try to use the native wdl4s method for getting outputs from
            // a workflow. This sometimes fails, hence the try/catch.
            //
            // This check catches cases of partial definitions, such as:
            // output {
            //    Add.result
            // }
            val wfOutputsWithExpansion = workflow.outputs
            if (wfOutputs.size < wfOutputsWithExpansion.size) {
                def nrPartialDefs = wfOutputsWithExpansion.size - wfOutputs.size
                allWarnings += (s"""|The output section for workflow [${workflow.unqualifiedName}]
                                    |contains ${nrPartialDefs} partial definitions"""
                                    .stripMargin.replaceAll("\n", " "))
                }
        } catch {
            case e: Throwable => ()
        }

        if (workflow.noWorkflowOutputs) {
            // Empty output section. Unlike Cromwell, we generate no outputs
            allWarnings += ("Empty output section, no outputs will be generated")
        }

        // check for duplicate names. WDL is supposed to check for this, but this
        // did not work in Cromwell 30.2.
        val names = wfOutputs.map(_.unqualifiedName)
        if (names.toSet.size != names.toVector.size)
            allErrors += ("Duplicate names in the workflow output section")

        // Only trivial expressions are supported in the output section
        val van = VarAnalysis(Set.empty, Map.empty, cef, verbose)
        wfOutputs.foreach{ wot =>
            if (!van.isTrivialExpression(wot.requiredExpression)) {
                allErrors += (cef.notCurrentlySupported(
                                wot.ast,
                                "expressions in the output section"))
            }

            // check if a cast is needed, that requires a job.
            if (wot.womType !=
                    Utils.evalType(wot.requiredExpression, workflow, cef, verbose)) {
                allErrors += (cef.notCurrentlySupported(
                                wot.ast,
                                "coercion in the output section"))
            }
        }
    }


    // validate that the call is providing all the compulsory arguments
    // to the callee (task/workflow).
    private def validateCall(call: WdlCall) : Unit = {
        val calleeInputs: Seq[Declaration] = call match {
            case tc: WdlTaskCall => tc.task.declarations
            case wfc: WdlWorkflowCall => wfc.calledWorkflow.declarations.filter(_.upstream.isEmpty)
        }
        val compulsoryInputs = calleeInputs.filter{ decl =>
            (decl.womType, decl.expression) match {
                case (WomOptionalType(_), _) =>
                    // optional value
                    false
                case (_, Some(expr)) =>
                    // input has a default, caller does not need to specify it
                    false
                case (_,_) =>
                    // compulsory input with no default
                    true
            }
        }
        compulsoryInputs.foreach{ decl =>
            val inputOpt = call.inputMappings.find{
                case (iName, _) => iName == decl.unqualifiedName
            }
            inputOpt match {
                case None =>
                    val msg = s"""|Workflow doesn't supply required input ${decl.unqualifiedName}
                                  |to call ${call.unqualifiedName}
                                  |""".stripMargin.replaceAll("\n", " ")
                    allWarnings += (cef.missingCallArgument(call.ast, msg))
                case _ => ()
            }
        }
    }

    // A calls must specify all the compulsory arguments. Check
    // that this is true for all calls
    //
    private def checkMissingArguments(statements: Seq[Scope]) : Unit = {
        statements.foreach{
            case decl:Declaration => ()
            case call:WdlCall => validateCall(call)
            case ssc:Scatter => checkMissingArguments(ssc.children)
            case ifStmt:If => checkMissingArguments(ifStmt.children)
            case _:WorkflowOutput => ()
            case x =>
                allErrors += (cef.notCurrentlySupported(
                                x.ast,
                                s"unimplemented workflow element"))
        }
    }

    def apply(ns: WdlNamespace) : Unit = {
        checkReservedWords(ns)
        ns.tasks.foreach(t => validateTask(t))

        // Make sure there are well defined outputs
        ns match {
            case nswf:WdlNamespaceWithWorkflow =>
                validateWorkflowOutputs(nswf.workflow)
                checkMissingArguments(nswf.workflow.children)
            case _ => ()
        }
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
                                   allErrors :  Queue[String],
                                   allWarnings : Queue[String],
                                   verbose: Verbose) : Unit = {
        // make a flat list of all referenced namespaces and sub-namespaces
        val allNs: Vector[WdlNamespace] = ns.allNamespacesRecursively.toVector

        // make sure tasks are unique
        val allTasks: Set[WdlTask] = allNs.map{ ns => ns.tasks }.flatten.toSet
        val allTaskNames : Set[String] = allTasks.map(_.unqualifiedName)
        val taskCounts: Map[String, Int] = allTaskNames.groupBy(x => x).mapValues(_.size)
        taskCounts.foreach{ case (taskName, nAppear) =>
            if (nAppear > 1)
                allErrors += (s"""|Task ${taskName} appears ${nAppear} times. It has to
                                  |be unique in order to be compiled to a single
                                  |dnanexus applet""".stripMargin.trim)
        }

        // make sure workflow names are unique
        val allWorkflows: Set[WdlWorkflow] = allNs.map{ ns => ns.workflows }.flatten.toSet
        val allWorkflowNames : Set[String] = allWorkflows.map(_.unqualifiedName)
        val wfCounts: Map[String, Int] = allWorkflowNames.groupBy(x => x).mapValues(_.size)
        wfCounts.foreach{ case (wfName, nAppear) =>
            if (nAppear > 1)
                allErrors += (s"""|Workflow ${wfName} appears ${nAppear} times. It has to
                                  |be unique in order to be compiled to a single
                                  |dnanexus workflow""".stripMargin.trim)
        }

        // make sure there is no intersection between applet and workflow names
        val allNames = allTaskNames ++ allWorkflowNames
        val counts: Map[String, Int] = allNames.groupBy(x => x).mapValues(_.size)
        counts.foreach{ case (name, nAppear) =>
            if (nAppear > 1)
                allErrors += (s"Name ${name} is used for an applet and a workflow.")
        }
    }

    private def validateNamespace(ns: WdlNamespace,
                                  allErrors :  Queue[String],
                                  allWarnings : Queue[String],
                                  verbose: Verbose) : Unit = {
        // check this namespace
        val cef = new CompilerErrorFormatter(ns.resource, ns.terminalMap)
        val v = new Validate(allErrors, allWarnings, cef, verbose)
        v.apply(ns)

        // recurse into all the sub-namespaces
        ns.namespaces.foreach(xNs => validateNamespace(xNs, allErrors, allWarnings, verbose))
    }

    def apply(ns: WdlNamespace,
              fatalValidationWarnings: Boolean,
              verbose: Verbose) : Unit = {
        // Keep track of warnings and errors, instead of throwing them immediately
        val allErrors = Queue.empty[String]
        val allWarnings = Queue.empty[String]

        checkFlatNamespace(ns, allErrors, allWarnings, verbose)
        validateNamespace(ns, allErrors, allWarnings, verbose)

        if (fatalValidationWarnings) {
            allErrors ++= allWarnings
            allWarnings.clear
        }
        if (!allWarnings.isEmpty) {
            val aggregatedWarningMsg = allWarnings.mkString("\n\n")
            Utils.warning(verbose, aggregatedWarningMsg)
        }
        if (!allErrors.isEmpty) {
            val aggregatedErrorMsg = allErrors.mkString("\n\n")
            throw new NamespaceValidationException(aggregatedErrorMsg)
        }
    }
}
