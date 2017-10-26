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

    // Convert the source to a singleton array. It is used in
    // the GATK best practices pipeline. This conversion may not be a
    // good idea, but it is used.
    //
    //  https://github.com/openwdl/wdl/blob/develop/scripts/broad_pipelines/germline-short-variant-discovery/gvcf-generation-per-sample/0.2.0/PublicPairedSingleSampleWf_170412.wdl#L1256
    //    input_bams = SortAndFixSampleBam.output_bam
    //    input_bams is of type Array[File]
    //    output_bam is of type File
    private def typesArrayDiff(expr: WdlExpression,
                               callerType:WdlType,
                               calleeType:WdlType) : Boolean = {
        val srcType: WdlType = stripOptional(callerType)
        val trgType: WdlType = stripOptional(calleeType)
        (srcType, trgType) match {
            case (WdlArrayType(_), WdlArrayType(_)) => false
            case (_, WdlArrayType(_)) =>
                System.err.println(s"Warning: converting ${expr.toWdlString} from ${srcType.toWdlString} to ${trgType.toWdlString}")
                //System.err.println(cef.traceExpression(expr.ast.asInstanceOf[Ast]))
                true
            case (_, _) => false
        }
    }

    // Convert a T to Array[T]
    //   1  -> [1]
    //   "a" -> ["a"]
    private def augmentExprToArray(expr: WdlExpression) : WdlExpression = {
        // Convert the source to a singleton array. It is used in
        // the GATK best practices pipeline. This conversion may not be a
        // good idea, but it is used.
        WdlExpression.fromString(s"[ ${expr.toWdlString} ]")
    }

    // Figure out the type of an expression
    private def evalType(expr: WdlExpression, parent: Scope) : WdlType = {
/*        expr.evaluateType(Utils.lookupType(parent),
                          new WdlStandardLibraryFunctionsType,
                          Some(parent)) match {*/
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
            case (WdlFileType, WdlStringType) => true

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
                case _ if typesArrayDiff(expr, callerType, calleeType) =>
                    // A coercion is required to convert the expression to the expected
                    // type
                    val augExpr = augmentExprToArray(expr)
                    replaceWithTempVar(calleeType, augExpr)
                case _ if (!typesMatch(callerType, calleeType)) =>
                    System.err.println(cef.typeConversionRequired(expr, call,
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
        val exprType : WdlType = evalType(cond.condition, cond)
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
