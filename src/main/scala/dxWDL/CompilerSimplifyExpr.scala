/**
  *  Simplify the original WDL by rewriting it. Instead of handling
  *  expressions in calls directly, we lift the expressions, generate
  *  auxiliary variables, and call the task with values or variables
  *  (no expressions).
  */
package dxWDL

import scala.collection.mutable.Queue
import scala.util.{Failure, Success}
import Utils.{genTmpVarName, stripOptional, trace, Verbose}
import wdl4s.wdl._
import wdl4s.wdl.expression._
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wdl4s.wdl.types._

case class CompilerSimplifyExpr(wf: WdlWorkflow,
                                cef: CompilerErrorFormatter,
                                verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "simplify"

    // A member access expression such as [A.x]. Check if
    // A is a call.
    private def isCallOutputAccess(expr: WdlExpression,
                                   ast: Ast,
                                   call: WdlCall) : Boolean = {
        val lhs:String = WdlExpression.toString(ast.getAttribute("lhs"))
        try {
            val wdlType = WdlNamespace.lookupType(wf)(lhs)
            wdlType.isInstanceOf[WdlCallOutputsObjectType]
        } catch {
            case e:Throwable=> false
        }
    }

    // Coercion is not required to move from source to target types
    def callTypesMatch(a: Ast,
                       expr: expr,
                       call: WdlCall,
                       calleeType: WdlType) : Boolean = {
        if  (!isCallOutputAccess(expr, a, call))
            return False
        val callerType:WdlType = callOutputFieldType(expr, a)

        val srcType: WdlType = stripOptional(callerType)
        val trgType: WdlType = stripOptional(calleeType)
        if (srcType == trgType ||
                trgType == WdlArrayType(srcType)) {
            return True
        } else {
            trace(verbose.on, s"coercion required ${srcType.toWdlString} -> ${trgType.toWdlString}")
            trace(verbose.on, cef.traceExpression(ast))
            return False
        }
    }


    // A member access expression such as [A.x]. Check if
    // A is a call.
    private def callOutputFieldType(expr: WdlExpression,
                                    ast: Ast) : WdlType = {
        val lhs:String = WdlExpression.toString(ast.getAttribute("lhs"))
        val rhs:String = WdlExpression.toString(ast.getAttribute("rhs"))
        val call = wf.findCallByName(lhs) match {
            case None =>
                throw new Exception(s"lhs of ${expr.toWdlString} is not a call")
            case Some(call) => call
        }
        val cOutOpt:Option[CallOutput] = call.outputs.find { cOutput =>
            cOutput.taskOutput.unqualifiedName == rhs
        }
        cOutOpt match {
            case None =>
                throw new Exception(s"Could not find field ${rhs} in expression ${expr.toWdlString}")
            case Some(cOut) =>
                cOut.taskOutput.wdlType
        }
    }

    // Return true if we are certain there is no interpolation in this string.
    //
    // A literal can include an interpolation expression, for example:
    //   "${filename}.vcf.gz"
    // Interpolation requires evaluation. This check is an approximation,
    // it may cause us to create an unnecessary declaration.
    private def nonInterpolation(t: Terminal) : Boolean = {
        !(t.getSourceString contains "${")
    }

    // Return true if an expression is a constant or a variable
    def isConstOrVar(expr: WdlExpression) : Boolean = {
        expr.ast match {
            case t: Terminal if nonInterpolation(t) => true
            case _ => false
        }
    }

    def isMemberAccess(a: Ast) = {
        wdl4s.wdl.WdlExpression.AstForExpressions(a).isMemberAccess
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
            val rhs = expr.ast match {
                case t: Terminal if nonInterpolation(t) => expr
                case a: Ast if (isMemberAccess(a) && callTypesMatch(expr, a, call, calleeType)) =>
                    // Accessing an expression like A.B.C
                    // The expression could be:
                    // 1) Result from a previous call
                    // 2) Access to a field in a pair, or an object (pair.left, obj.name)
                    // Only the first case can be handled inline, the other requires
                    // a temporary variable. Any coercion operation requires a separate
                    // expression.
                    val callerType:WdlType = callOutputFieldType(expr, a)
                    if (noCoercionNeeded(callerType, calleeType, a))
                        expr
                    else
                        replaceWithTempVar(calleeType, expr)
                case _ =>
                    replaceWithTempVar(calleeWdlType, expr)
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
        val lookupType: (String => WdlType) = WdlNamespace.lookupType(ssc)
        val collType : WdlType =
            ssc.collection.evaluateType(lookupType,
                                        new WdlStandardLibraryFunctionsType, Some(ssc)) match {
                case Success(wdlType) => wdlType
                case Failure(f) =>
                    val astCollection = ssc.ast.getAttribute("collection").asInstanceOf[Ast]
                    System.err.println(cef.couldNotEvaluateType(astCollection))
                    throw f
            }

        if (isConstOrVar(ssc.collection)) {
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
        val exprType : WdlType =
            cond.condition.evaluateType(WdlNamespace.lookupType(cond),
                                       new WdlStandardLibraryFunctionsType,
                                       Some(cond)) match {
                case Success(wdlType) => wdlType
                case _ => throw new Exception(cef.couldNotEvaluateType(cond.ast))
            }

        if (isConstOrVar(cond.condition)) {
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

    // Convert complex expressions to independent declarations
    def simplify(scope: Scope): Vector[Scope] = {
        scope match {
            case ssc:Scatter => simplifyScatter(ssc)
            case call:WdlCall => simplifyCall(call)
            case cond:If => simplifyIf(cond)
            case decl:Declaration => Vector(decl)
            case wfo:WorkflowOutput => Vector(wfo)
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    def simplifyWorkflow(wf: WdlWorkflow) : WdlWorkflow = {
        val children: Vector[Scope] = wf.children.map(x => simplify(x)).toVector.flatten
        WdlRewrite.workflow(wf, children)
    }
}

object CompilerSimplifyExpr {

    // Make a pass on all declarations, and make sure no reserved words or prefixes
    // are used.
    private def checkReservedWords(ns: WdlNamespace, cef: CompilerErrorFormatter) : Unit = {
        def checkVarName(varName: String, ast: Ast) : Unit = {
            if (Utils.isGeneratedVar(varName))
                throw new Exception(cef.illegalVariableName(ast))
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

    def apply(ns: WdlNamespace, verbose: Verbose) : WdlNamespace = {
        trace(verbose.on, "simplifying workflow expressions")
        val cef = new CompilerErrorFormatter(ns.terminalMap)
        checkReservedWords(ns, cef)

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
