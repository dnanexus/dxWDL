/**
  *  Simplify the original WDL by rewriting it. Instead of handling
  *  expressions in calls directly, we lift the expressions, generate
  *  auxiliary variables, and call the task with values or variables
  *  (no expressions).
  */
package dxWDL

import scala.collection.mutable.Queue
import scala.util.{Failure, Success}
import Utils.{genTmpVarName, nonInterpolation, trace, Verbose, warning}
import wdl4s.wdl._
import wdl4s.wdl.expression._
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wdl4s.wdl.types._

case class CompilerSimplifyExpr(wf: WdlWorkflow,
                                cef: CompilerErrorFormatter,
                                verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "simplify"

    private def isMemberAccess(a: Ast) = {
        wdl4s.wdl.WdlExpression.AstForExpressions(a).isMemberAccess
    }

    // A member access expression such as [A.x]. Check if
    // A is a call.
    private def isCallOutputAccess(ast: Ast) : Boolean = {
        if (!isMemberAccess(ast)) {
            false
        } else {
            val lhs:String = WdlExpression.toString(ast.getAttribute("lhs"))
            try {
                val wdlType = Utils.lookupType(wf)(lhs)
                wdlType.isInstanceOf[WdlCallOutputsObjectType]
            } catch {
                case e:Throwable=> false
            }
        }
    }

    // Figure out the type of an expression
    private def evalType(expr: WdlExpression, parent: Scope) : WdlType = {
        dxWDL.TypeEvaluator(Utils.lookupType(parent),
                            new WdlStandardLibraryFunctionsType,
                            Some(parent)).evaluate(expr.ast) match {
            case Success(wdlType) => wdlType
            case Failure(f) =>
                System.err.println(cef.couldNotEvaluateType(expr))
                throw f
        }
    }

    // Make sure [srcType] can be coerced into [trgType]. wdl4s allows
    // certain conversions that we do not. We assume that wdl4s type checking
    // has already taken place; we don't need to repeat it.
    //
    // For example, in wdl4s it is legal to coerce a String into a
    // File in workflow context. This is not possible in dxWDL.
    private def typesMatch(srcType: WdlType, trgType: WdlType) : Boolean = {
        (srcType, trgType) match {
            // base cases
            case (WdlBooleanType, WdlBooleanType) => true
            case (WdlIntegerType, WdlIntegerType) => true
            case (WdlFloatType, WdlFloatType) => true
            case (WdlStringType, WdlStringType) => true
            case (WdlFileType, WdlFileType) => true

            // Files: it is legal to convert a file to a string, but not the other
            // way around.
            //case (WdlFileType, WdlStringType) => true

            // array
            case (WdlArrayType(WdlNothingType), WdlArrayType(_)) => true
            case (WdlArrayType(s), WdlArrayType(t)) => typesMatch(s,t)

            // strip optionals
            case (WdlOptionalType(s), WdlOptionalType(t)) => typesMatch(s,t)
            case (WdlOptionalType(s), t) => typesMatch(s,t)
            case (s, WdlOptionalType(t)) => typesMatch(s,t)

            // map
            case (WdlMapType(sk, sv), WdlMapType(tk, tv)) =>
                typesMatch(sk, tv) && typesMatch(sv, tv)

            case (WdlPairType(sLeft, sRight), WdlPairType(tLeft, tRight)) =>
                typesMatch(sLeft, tLeft) && typesMatch(sRight, tRight)

            // objects
            case (WdlObjectType, WdlObjectType) => true
            case (_,_) => false
        }
    }

    // Return true if an expression is a variable
    def isVar(expr: WdlExpression) : Boolean = {
        if (Utils.isExpressionConst(expr)) {
            false
        } else expr.ast match {
            case t: Terminal if nonInterpolation(t) => true
            case _ => false
        }
    }

    // Transform a call by lifting its non trivial expressions,
    // and converting them into declarations. For example:
    //
    //  call Multiply {
    //     input: a = Add.result + 10, b = 2
    //  }
    //
    // Would be transformed into:
    //  Int xtmp_1 = Add.result + 10
    //  call Multiply {
    //     input: a = xtmp_1, b = 2
    //  }
    //
    // Inside a scatter we can deal with field accesses,
    private def simplifyCall(call: WdlCall) : Vector[Scope] = {
        val tmpDecls = Queue[Scope]()

        // replace an expression with a temporary variable
        def replaceWithTempVar(t: WdlType, expr: WdlExpression) : WdlExpression = {
            val tmpVarName = genTmpVarName()
            tmpDecls += WdlRewrite.declaration(t, tmpVarName, Some(expr))
            WdlExpression.fromString(tmpVarName)
        }

        val inputs: Map[String, WdlExpression]  = call.inputMappings.map { case (key, expr) =>
            val calleeDecl: Declaration =
                call.declarations.find(decl => decl.unqualifiedName == key).get
            val calleeType = calleeDecl.wdlType
            val callerType = evalType(expr, call)
            val rhs:WdlExpression = expr.ast match {
                case _ if (!typesMatch(callerType, calleeType)) =>
                    // A coercion is required to convert the expression to the expected
                    // type
                    warning(verbose, cef.typeConversionRequired(expr, call,
                                                                callerType, calleeType))
                    replaceWithTempVar(calleeType, expr)
                case t: Terminal if nonInterpolation(t) => expr
                case a: Ast if isCallOutputAccess(a) =>
                    // Accessing an expression like A.B.C
                    // The expression could be:
                    // 1) Result from a previous call
                    // 2) Access to a field in a pair, or an object (pair.left, obj.name)
                    // Only the first case can be handled inline, the other requires
                    // a temporary variable.
                    expr
                case _ =>
                    replaceWithTempVar(calleeType, expr)
            }
            (key -> rhs)
        }

        val callModifiedInputs = call match {
            case tc: WdlTaskCall => WdlRewrite.taskCall(tc, inputs)
            case wfc: WdlWorkflowCall => throw new Exception(s"Unimplemented WorkflowCall")
        }
        tmpDecls += callModifiedInputs
        tmpDecls.toVector
    }

    // - Move the collection expression to a separate declaration
    // - Move expressions in calls into separate declarations.
    //
    // For example:
    //
    // scatter (i in [1,2,3]) {
    //    call Add { input: a=i*2 }
    // }
    // ->
    // Array[Int] xtmp5 = [1,2,3]
    // scatter (i in xtmp5) {
    //    Int xtmp6 = i*2
    //    call Add { input: a=xtmp6 }
    // }
    //
    def simplifyScatter(ssc: Scatter) : Vector[Scope] = {
        // simplify expressions in the children
        val children: Vector[Scope] = ssc.children
            .map(x => simplify(x))
            .toVector
            .flatten
        // Figure out the expression type for a collection we loop over in a scatter
        val collType : WdlType = evalType(ssc.collection, ssc)
        if (isVar(ssc.collection)) {
            // The collection is a simple variable, there is no need
            // to create an additional declaration
            val ssc1 = WdlRewrite.scatter(ssc, children)
            Vector(ssc1)
        } else {
            // separate declaration for collection expression
            val colDecl = WdlRewrite.declaration(collType,
                                                 genTmpVarName(),
                                                 Some(ssc.collection))
            val collVar = WdlExpression.fromString(colDecl.unqualifiedName)
            val ssc1 = WdlRewrite.scatter(ssc, children, collVar)
            Vector(colDecl, ssc1)
        }
    }

    def simplifyIf(cond: If) : Vector[Scope] = {
        // simplify expressions in the children
        val children: Vector[Scope] = cond.children
            .map(x => simplify(x))
            .toVector
            .flatten

        // Figure out the expression type for a collection we loop over in a scatter
        val exprType : WdlType = evalType(cond.condition, cond)
        if (isVar(cond.condition)) {
            // The condition is a simple variable, there is no need
            // to create an additional declaration
            Vector(WdlRewrite.cond(cond, children, cond.condition))
        } else {
            // separate declaration for condition expression
            val condDecl = WdlRewrite.declaration(exprType,
                                                  genTmpVarName(),
                                                  Some(cond.condition))
            val condVar = WdlExpression.fromString(condDecl.unqualifiedName)
            val cond1 = WdlRewrite.cond(cond, children, condVar)
            Vector(condDecl, cond1)
        }
    }

    // If the workflow output has a complex expression, split that into
    // a temporary variable.
    def simplifyWorkflowOutput(wot: WorkflowOutput) : (Option[Declaration], WorkflowOutput) = {
        val wdlType = wot.wdlType
        val expr = wot.requiredExpression
        val maybeCoercion:Boolean =
            try {
                val exprType = evalType(wot.requiredExpression, wot)
                exprType != wdlType
            } catch {
                case e:Throwable=>
                    // We can't evaluate the type of the expressions, so
                    // we suspect that coercion may be required.
                    true
            }

        val tempVarRequired = expr.ast match {
            case _ if maybeCoercion =>
                // Coercion maybe needed, this requires calculation
                true
            case t: Terminal if nonInterpolation(t) => false
            case a: Ast if isCallOutputAccess(a) =>
                // Accessing an expression like A.B.C
                // The expression could be:
                // 1) Result from a previous call
                // 2) Access to a field in a pair, or an object (pair.left, obj.name)
                // Only the first case can be handled inline, the other requires
                // a temporary variable.
                false
            case _ =>
                true
        }
        if (tempVarRequired) {
            trace(verbose.on, s"building temporary variable for ${wot.toWdlString}")

            // separate declaration for expression
            val tmpVarName = genTmpVarName()
            val tmpDecl:Declaration = WdlRewrite.declaration(wdlType, tmpVarName, Some(expr))
            val wot1 = new WorkflowOutput(wot.unqualifiedName, wot.wdlType,
                                          WdlExpression.fromString(tmpVarName),
                                          WdlRewrite.INVALID_AST,
                                          Some(wot))
            (Some(tmpDecl), wot1)
        } else {
            (None, wot)
        }
    }

    // Extract all the complex expressions from the output section,
    // so it will be easier for downstream passes to deal with.
    def simplifyOutputSection(wfOutputs: Vector[WorkflowOutput])
            : (Vector[Declaration], Vector[WorkflowOutput]) = {
        val tmpDecls = Queue.empty[Declaration]

        val outputs: Vector[WorkflowOutput] = wfOutputs.map{ wot =>
            val (declOpt, wot1) = simplifyWorkflowOutput(wot)
            declOpt match {
                case None => ()
                case Some(decl) => tmpDecls += decl
            }
            wot1
        }

        (tmpDecls.toVector, outputs)
    }


    // Convert complex expressions to independent declarations
    def simplify(scope: Scope): Vector[Scope] = {
        scope match {
            case ssc:Scatter => simplifyScatter(ssc)
            case call:WdlCall => simplifyCall(call)
            case cond:If => simplifyIf(cond)
            case decl:Declaration => Vector(decl)
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    def simplifyWorkflow(wf: WdlWorkflow) : WdlWorkflow = {
        val wfProper = wf.children.filter(x => !x.isInstanceOf[WorkflowOutput])
        val wfProperSmpl: Vector[Scope] = wfProper.map(x => simplify(x)).toVector.flatten

        val outputs :Vector[WorkflowOutput] = wf.outputs.toVector
        val (tmpDecls, outputsSmpl) = simplifyOutputSection(outputs)

        val allChildren = wfProperSmpl ++ tmpDecls ++ outputsSmpl
        WdlRewrite.workflow(wf, allChildren)
    }
}

object CompilerSimplifyExpr {

    // Make a pass on all declarations, and make sure no reserved words or prefixes
    // are used.
    private def checkReservedWords(ns: WdlNamespace, cef: CompilerErrorFormatter) : Unit = {
        def checkVarName(varName: String, ast: Ast) : Unit = {
            if (Utils.isGeneratedVar(varName))
                throw new Exception(cef.illegalVariableName(ast))
            Utils.reservedSubstrings.foreach{ s =>
                if (varName contains s)
                    throw new Exception(cef.illegalVariableName(ast))
            }
        }
        def deepCheck(children: Seq[Scope]) : Unit = {
            children.foreach {
                case ssc:Scatter =>
                    checkVarName(ssc.item, ssc.ast)
                    deepCheck(ssc.children)
                case decl:DeclarationInterface =>
                    checkVarName(decl.unqualifiedName, decl.ast)
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

    private def validateTask(task: WdlTask, verbose: Verbose) : Unit = {
        // validate runtime attributes
        val validAttrNames:Set[String] = Set(Utils.DX_INSTANCE_TYPE_ATTR, "memory", "disks", "cpu", "docker")
        task.runtimeAttributes.attrs.foreach{ case (attrName,_) =>
            if (!(validAttrNames contains attrName))
                warning(verbose, s"Runtime attribute ${attrName} for task ${task.name} is unknown")
        }
    }

    def apply(ns: WdlNamespace, verbose: Verbose) : WdlNamespace = {
        trace(verbose.on, "simplifying workflow expressions")
        val cef = new CompilerErrorFormatter(ns.terminalMap)
        checkReservedWords(ns, cef)
        ns.tasks.foreach(t => validateTask(t, verbose))

        // Process the original WDL file,
        // Do not modify the tasks
        ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                val cse = new CompilerSimplifyExpr(nswf.workflow, cef, verbose)
                val wf2 = cse.simplifyWorkflow(nswf.workflow)
                WdlRewrite.namespace(nswf, wf2)
            case _ => ns
        }
    }
}
