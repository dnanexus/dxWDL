/**
  *  Simplify the original WDL by rewriting it. Instead of handling
  *  expressions in calls directly, we lift the expressions, generate
  *  auxiliary variables, and call the task with values or variables
  *  (no expressions).
  */
package dxWDL.compiler

import dxWDL.{CompilerErrorFormatter, Utils, Verbose}
import java.nio.file.Path
import scala.collection.mutable.Queue
import scala.util.{Failure, Success}
import wdl._
import wdl.expression._
import wdl.types._
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wom.types._

case class SimplifyExpr(cef: CompilerErrorFormatter,
                        verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "simplify"

    private def isMemberAccess(a: Ast) : Boolean = {
        wdl.WdlExpression.AstForExpressions(a).isMemberAccess
    }

    // A member access expression such as [A.x]. Check if
    // A is a call.
    private def isCallOutputAccess(ast: Ast, wf: WdlWorkflow) : Boolean = {
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
    private def evalType(expr: WdlExpression, parent: Scope) : WomType = {
        TypeEvaluator(Utils.lookupType(parent),
                      new WdlStandardLibraryFunctionsType,
                      Some(parent)).evaluate(expr.ast) match {
            case Success(wdlType) => wdlType
            case Failure(f) =>
                Utils.warning(verbose, cef.couldNotEvaluateType(expr))
                throw f
        }
    }

    // Make sure [srcType] can be coerced into [trgType]. wdl4s allows
    // certain conversions that we do not. We assume that wdl4s type checking
    // has already taken place; we don't need to repeat it.
    //
    // For example, in wdl4s it is legal to coerce a String into a
    // File in workflow context. This is not possible in dxWDL.
    private def typesMatch(srcType: WomType, trgType: WomType) : Boolean = {
        (srcType, trgType) match {
            // base cases
            case (WomBooleanType, WomBooleanType) => true
            case (WomIntegerType, WomIntegerType) => true
            case (WomFloatType, WomFloatType) => true
            case (WomStringType, WomStringType) => true
            case (WomFileType, WomFileType) => true

            // Files: it is legal to convert a file to a string, but not the other
            // way around.
            //case (WomFileType, WomStringType) => true

            // array
            case (WomArrayType(WomNothingType), WomArrayType(_)) => true
            case (WomArrayType(s), WomArrayType(t)) => typesMatch(s,t)

            // strip optionals
            case (WomOptionalType(s), WomOptionalType(t)) => typesMatch(s,t)
            case (WomOptionalType(s), t) => typesMatch(s,t)
            case (s, WomOptionalType(t)) => typesMatch(s,t)

            // map
            case (WomMapType(sk, sv), WomMapType(tk, tv)) =>
                typesMatch(sk, tv) && typesMatch(sv, tv)

            case (WomPairType(sLeft, sRight), WomPairType(tLeft, tRight)) =>
                typesMatch(sLeft, tLeft) && typesMatch(sRight, tRight)

            // objects
            case (WomObjectType, WomObjectType) => true
            case (_,_) => false
        }
    }

    // Return true if an expression is a variable
    def isVar(expr: WdlExpression) : Boolean = {
        if (Utils.isExpressionConst(expr)) {
            false
        } else expr.ast match {
            case t: Terminal if Utils.nonInterpolation(t) => true
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
    private def simplifyCall(call: WdlCall, wf: WdlWorkflow) : Vector[Scope] = {
        val tmpDecls = Queue[Scope]()

        // replace an expression with a temporary variable
        def replaceWithTempVar(t: WomType, expr: WdlExpression) : WdlExpression = {
            val tmpVarName = Utils.genTmpVarName()
            tmpDecls += WdlRewrite.declaration(t, tmpVarName, Some(expr))
            WdlExpression.fromString(tmpVarName)
        }

        val inputs: Map[String, WdlExpression]  = call.inputMappings.map { case (key, expr) =>
            val calleeDecl: Declaration =
                call.declarations.find(decl => decl.unqualifiedName == key).get
            val calleeType = calleeDecl.womType
            val callerType = evalType(expr, call)
            val rhs:WdlExpression = expr.ast match {
                case _ if (!typesMatch(callerType, calleeType)) =>
                    // A coercion is required to convert the expression to the expected
                    // type
                    Utils.warning(verbose, cef.typeConversionRequired(expr, call,
                                                                      callerType, calleeType))
                    replaceWithTempVar(calleeType, expr)
                case t: Terminal if Utils.nonInterpolation(t) => expr
                case a: Ast if isCallOutputAccess(a, wf) =>
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
            case wfc: WdlWorkflowCall => WdlRewrite.workflowCall(wfc, inputs)
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
    def simplifyScatter(ssc: Scatter, wf: WdlWorkflow) : Vector[Scope] = {
        // simplify expressions in the children
        val children: Vector[Scope] = ssc.children
            .map(x => simplify(x, wf))
            .toVector
            .flatten
        // Figure out the expression type for a collection we loop over in a scatter
        val collType : WomType = evalType(ssc.collection, ssc)
        if (isVar(ssc.collection)) {
            // The collection is a simple variable, there is no need
            // to create an additional declaration
            val ssc1 = WdlRewrite.scatter(ssc, children)
            Vector(ssc1)
        } else {
            // separate declaration for collection expression
            val colDecl = WdlRewrite.declaration(collType,
                                                 Utils.genTmpVarName(),
                                                 Some(ssc.collection))
            val collVar = WdlExpression.fromString(colDecl.unqualifiedName)
            val ssc1 = WdlRewrite.scatter(ssc, children, collVar)
            Vector(colDecl, ssc1)
        }
    }

    def simplifyIf(cond: If, wf: WdlWorkflow) : Vector[Scope] = {
        // simplify expressions in the children
        val children: Vector[Scope] = cond.children
            .map(x => simplify(x, wf))
            .toVector
            .flatten

        // Figure out the expression type for a collection we loop over in a scatter
        val exprType : WomType = evalType(cond.condition, cond)
        if (isVar(cond.condition)) {
            // The condition is a simple variable, there is no need
            // to create an additional declaration
            Vector(WdlRewrite.cond(cond, children, cond.condition))
        } else {
            // separate declaration for condition expression
            val condDecl = WdlRewrite.declaration(exprType,
                                                  Utils.genTmpVarName(),
                                                  Some(cond.condition))
            val condVar = WdlExpression.fromString(condDecl.unqualifiedName)
            val cond1 = WdlRewrite.cond(cond, children, condVar)
            Vector(condDecl, cond1)
        }
    }

    // If the workflow output has a complex expression, split that into
    // a temporary variable.
    def simplifyWorkflowOutput(wot: WorkflowOutput,
                               wf: WdlWorkflow) : (Option[Declaration], WorkflowOutput) = {
        val wdlType = wot.womType
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
            case t: Terminal if Utils.nonInterpolation(t) => false
            case a: Ast if isCallOutputAccess(a, wf) =>
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
            Utils.trace(verbose.on, s"building temporary variable for ${wot.unqualifiedName}")

            // separate declaration for expression
            val tmpVarName = Utils.genTmpVarName()
            val tmpDecl:Declaration = WdlRewrite.declaration(wdlType, tmpVarName, Some(expr))
            val wot1 = new WorkflowOutput(wot.unqualifiedName, wot.womType,
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
    def simplifyOutputSection(wfOutputs: Vector[WorkflowOutput],
                              wf: WdlWorkflow)
            : (Vector[Declaration], Vector[WorkflowOutput]) = {
        val tmpDecls = Queue.empty[Declaration]

        val outputs: Vector[WorkflowOutput] = wfOutputs.map{ wot =>
            val (declOpt, wot1) = simplifyWorkflowOutput(wot, wf)
            declOpt match {
                case None => ()
                case Some(decl) => tmpDecls += decl
            }
            wot1
        }

        (tmpDecls.toVector, outputs)
    }


    // Convert complex expressions to independent declarations
    private def simplify(scope: Scope, wf: WdlWorkflow): Vector[Scope] = {
        scope match {
            case ssc:Scatter => simplifyScatter(ssc, wf)
            case call:WdlCall => simplifyCall(call, wf)
            case cond:If => simplifyIf(cond, wf)
            case decl:Declaration => Vector(decl)
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    def simplifyWorkflow(wf: WdlWorkflow) : WdlWorkflow = {
        Utils.trace(verbose.on, s"simplifying workflow ${wf.unqualifiedName}")
        val wfProper = wf.children.filter(x => !x.isInstanceOf[WorkflowOutput])
        val wfProperSmpl: Vector[Scope] = wfProper.map(x => simplify(x, wf)).toVector.flatten

        val outputs :Vector[WorkflowOutput] = wf.outputs.toVector
        val (tmpDecls, outputsSmpl) = simplifyOutputSection(outputs, wf)

        val allChildren = wfProperSmpl ++ tmpDecls ++ outputsSmpl
        WdlRewrite.workflow(wf, allChildren)
    }

}

object SimplifyExpr {
    def apply(nsTree: NamespaceOps.Tree,
              wdlSourceFile: Path,
              verbose: Verbose) : NamespaceOps.Tree = {
        // validate the namespace
        val nsTree1 = nsTree.transform{ case (wf, cef) =>
            val simpExpr = new SimplifyExpr(cef, verbose)
            simpExpr.simplifyWorkflow(wf)
        }
        if (verbose.on)
            NamespaceOps.prettyPrint(wdlSourceFile, nsTree1, "simple", verbose)
        nsTree1
    }
}
