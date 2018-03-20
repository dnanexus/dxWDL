/*
 * Break large sub-blocks into separate sub-workflows. Simplistically,
 * a large subblock is one that has more than two calls. For example
 * the scatter below is "large".
 *
 * task add {
 *    Int a
 *    Int b
 *    command {}
 *    output {
 *       Int result = a + b
 *    }
 * }
 *
 * task mul {
 *    Int a
 *    Int n
 *    command {}
 *    output {
 *       Int result = a ** b
 *    }
 * }
 *
 * scatter (x in ax) {
 *   Int y = x + 4
 *   call add { input: a=y, b=x }
 *   Int base = add.result
 *   call mul { input: a=base, n=x}
 * }
 *
 *
 * It will be broken into a subworkflow, and a scatter that calls it.
 *
 * scatter (x in ax) {
 *   Int y = x + 4
 *   call sub_wf_add { x=x, y=y }
 * }
 *
 * workflow sub_wf_add {
 *   Int x
 *   Int y
 *
 *   call add { input: a=y, b=x }
 *   Int base = add.result
 *   call mul { input: a=base, n=x}
 *
 *   output {
 *     Int out_add_result = add.result
 *     Int out_base = base
 *     Int out_mul_result = mul.result
 *   }
 * }
 *
 * Downstream references to 'add.result' will be renamed: sub_wf_add.out_add_result
 */

package dxWDL.compiler

import dxWDL.{CompilerErrorFormatter, Utils, Verbose}
import java.nio.file.Path
import wom.types.WomType
import wdl._
import wdl.AstTools.EnhancedAstNode
import wdl.WdlExpression.AstForExpressions
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}

case class DecomposeBlocks(cef: CompilerErrorFormatter,
                           verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "subwf"

    // The name field can include dots ("add.result", "mul.result").
    // The sanitized name has no dots, and can be placed in a WDL script (or dx).
    case class DVar(name: String,
                    womType: WomType,
                    sanitizedName: String)
    object DVar {
        def apply(name: String, womType: WomType) : DVar =
            new DVar(name, womType, Utils.transformVarName(name))
    }

    // keep track of variables defined inside the block, and outside
    // the block.
    case class VarTracker(local: Map[String, DVar],   // defined inside the block
                          outside: Map[String, DVar])  // defined outside the block
    {
        // A statmement accessed a bunch of variables. Add any newly
        // discovered outside variables.
        def findNewIn(refs: Seq[DVar]) : VarTracker = {
            val localVarNames = local.keys.toSet
            val discovered =
                refs.filter{ dVar => (!(localVarNames contains dVar.name)) }
                    .map{ dVar => dVar.name -> dVar}.toMap
            VarTracker(local,
                       outside ++ discovered)
        }

        // Add a binding for a local variable
        def addLocal(dVar: DVar) : VarTracker = {
            VarTracker(local + (dVar.name -> dVar),
                       outside)
        }
        def addLocal(dVars: Seq[DVar]) : VarTracker = {
            val newLocals = dVars.map{ dVar => dVar.name -> dVar}
            VarTracker(local ++ newLocals,
                       outside)
        }
    }

    object VarTracker {
        val empty = VarTracker(Map.empty, Map.empty)
    }

    // Is this a subblock? Declarations aren't subblocks, scatters and if's are.
    private def isSubBlock(scope:Scope) : Boolean = {
        scope match {
            case _:Scatter => true
            case _:If => true
            case _ => false
        }
    }

    // A block is large if it has more than a bunch of declarations, followed by
    // at most one call.
    private def isLargeSubBlock(scope: Scope) : Boolean = {
        if (isSubBlock(scope)) {
            val (topDecls, rest) = Utils.splitBlockDeclarations(scope.children.toList)
            (rest.length >= 2)
        } else {
            false
        }
    }


    // Figure out all the variables used to calculate this expression.
    private def dependencies(aNode : AstNode) : Set[String] = {
        // recurse into list of subexpressions
        def subExpr(exprVec: Seq[AstNode]) : Set[String] = {
            val setList = exprVec.map(a => dependencies(a))
            setList.foldLeft(Set.empty[String]){case (accu, st) =>
                accu ++ st
            }
        }
        aNode match {
            case t: Terminal =>
                val srcStr = t.getSourceString
                t.getTerminalStr match {
                    case "identifier" => Set(srcStr)
                    case "string" if Utils.isInterpolation(srcStr) =>
                        // From a string like: "Tamara likes ${fruit}s and ${ice}s"
                        // extract the variables {fruit, ice}. In general, they
                        // could be full fledged expressions
                        //
                        // from: wdl4s.wdl.expression.ValueEvaluator.InterpolationTagPattern
                        val iPattern = "\\$\\{\\s*([^\\}]*)\\s*\\}".r
                        val subExprRefs: List[String] = iPattern.findAllIn(srcStr).toList
                        val subAsts:List[AstNode] = subExprRefs.map{ tag =>
                            // ${fruit} ---> fruit
                            val expr = WdlExpression.fromString(tag.substring(2, tag.length - 1))
                            expr.ast
                        }
                        subExpr(subAsts)
                    case _ =>
                        // A literal
                        Set.empty
                }
            case a: Ast if a.isMemberAccess =>
                // This is a case of accessing something like A.B.C.
                val exprSrcStr = AstTools.findTerminals(a).map{
                    t => t.getSourceString
                }.mkString(".")
                Set(exprSrcStr)
            case a: Ast if a.isBinaryOperator =>
                val lhs = a.getAttribute("lhs")
                val rhs = a.getAttribute("rhs")
                subExpr(List(lhs, rhs))
            case a: Ast if a.isUnaryOperator =>
                val expression = a.getAttribute("expression")
                dependencies(expression)
            case TernaryIf(condition, ifTrue, ifFalse) =>
                subExpr(List(condition, ifTrue, ifFalse))
            case a: Ast if a.isArrayLiteral =>
                subExpr(a.getAttribute("values").astListAsVector)
            case a: Ast if a.isTupleLiteral =>
                subExpr(a.getAttribute("values").astListAsVector)
            case a: Ast if a.isMapLiteral =>
                val kvMap = a.getAttribute("map").astListAsVector.map { kv =>
                    val key = kv.asInstanceOf[Ast].getAttribute("key")
                    val value = kv.asInstanceOf[Ast].getAttribute("value")
                    key -> value
                }.toMap
                val elems: Vector[AstNode] = kvMap.keys.toVector ++ kvMap.values.toVector
                subExpr(elems)
            case a: Ast if a.isArrayOrMapLookup =>
                val index = a.getAttribute("rhs")
                val mapOrArray = a.getAttribute("lhs")
                subExpr(List(index, mapOrArray))
            case a: Ast if a.isFunctionCall =>
                subExpr(a.params)
            case _ =>
                throw new Exception(s"unhandled expression ${aNode}")
        }
    }

    private def exprDeps(expr: WdlExpression,
                         scope: Scope): Vector[DVar] = {
        val variables = dependencies(expr.ast)
        variables.map{ varName =>
            val womType = Utils.lookupType(scope)(varName)
            DVar(varName, womType, Utils.transformVarName(varName))
        }.toVector
    }

    // Find all the free variables in a block of statements. For example,
    // for the following block:
    //
    //   call add { input: a=y, b=x }
    //   Int base = add.result
    //   call mul { input: a=base, n=x}
    //
    // The free variables are {x, y}.
    //
    private def freeVars(statements: Seq[Scope]) : Vector[DVar] = {
        val varTracker = statements.foldLeft(VarTracker.empty) {
            case (accu, decl:Declaration) =>
                val xtrnVars = decl.expression match {
                    case None => Vector.empty
                    case Some(expr) => exprDeps(expr, decl)
                }
                val declVar = DVar(decl.unqualifiedName, decl.womType)
                accu.findNewIn(xtrnVars).addLocal(declVar)

            case (accu, call:WdlCall) =>
                val xtrnVars = call.inputMappings.map{
                    case (_, expr) => exprDeps(expr, call)
                }.flatten.toSeq
                val callOutputs = call.outputs.map { cOut: CallOutput =>
                    DVar(call.unqualifiedName + "." ++ cOut.unqualifiedName, cOut.womType)
                }
                accu.findNewIn(xtrnVars).addLocal(callOutputs)

            case (_, x) =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
        varTracker.outside.values.toVector
    }

    // Figure out all the outputs from a block of statements
    private def blockOutputs(statements: Vector[Scope]) : Vector[DVar] = {
        statements.map{
            case decl:Declaration =>
                Vector(DVar(decl.unqualifiedName, decl.womType))
            case call:WdlCall =>
                call.outputs.map { cOut: CallOutput =>
                    DVar(call.unqualifiedName + "." ++ cOut.unqualifiedName, cOut.womType)
                }
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }.flatten
    }

    // Rename member accesses inside an expression, from the form A.x
    // to A_x. This is used inside an applet of WDL generated code.
    //
    // Here, we take a shortcut, and just replace strings, instead of
    // doing a recursive syntax analysis (see ValueEvaluator wdl4s
    // module).
    private def exprRenameVars(expr: WdlExpression,
                               allVars: Vector[DVar]) : WdlExpression = {
        var sExpr: String = expr.toWomString
        for (dVar <- allVars) {
            // A.x => A_x
            sExpr = sExpr.replaceAll(dVar.name, dVar.sanitizedName)
        }
        WdlExpression.fromString(sExpr)
    }

    // rename all the variables from [allVars] used inside a statement
    private def statementRenameVars(stmt: Scope,
                                    allVars: Vector[DVar]) : Scope = {
        stmt match {
            case decl:Declaration =>
                WdlRewrite.declaration(decl.womType,
                                       decl.unqualifiedName,
                                       decl.expression.map(e => exprRenameVars(e, allVars)))

            case tc:WdlTaskCall =>
                val inputsRn = tc.inputMappings.map{
                    case (name, expr) =>  name -> exprRenameVars(expr, allVars)
                }.toMap
                WdlRewrite.taskCall(tc, inputsRn)

            case wfc:WdlWorkflowCall =>
                val inputsRn = wfc.inputMappings.map{
                    case (name, expr) =>  name -> exprRenameVars(expr, allVars)
                }.toMap
                WdlRewrite.workflowCall(wfc, inputsRn)

            case ssc:Scatter =>
                val children = ssc.children.map {
                    case child:Scope => statementRenameVars(child, allVars)
                }.toVector
                WdlRewrite.scatter(ssc,
                                   children,
                                   exprRenameVars(ssc.collection, allVars))

            case cond:If =>
                val children = cond.children.map {
                    case child:Scope => statementRenameVars(child, allVars)
                }.toVector
                WdlRewrite.cond(cond,
                                children,
                                exprRenameVars(cond.condition, allVars))

            case wo:WorkflowOutput =>
                new WorkflowOutput(wo.unqualifiedName,
                                   wo.womType,
                                   exprRenameVars(wo.requiredExpression, allVars),
                                   wo.ast,
                                   wo.parent)
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        "Unimplemented workflow element"))
        }
    }

    // Build a valid WDL workflow with [inputs] and [outputs].
    // The body is made up of [statements].
    // 1) Rename the input variables (A.B -> A_B)
    // 2) Setup the output variables
    //
    // Return the subworkflow, and the new names of all the outputs. We will
    // need to rename all the references to them.
    private def buildSubWorkflow(subWfName: String,
                                 inputs: Vector[DVar],
                                 outputs: Vector[DVar],
                                 statements: Vector[Scope]) : (WdlWorkflow, Vector[DVar]) = {
        val inputDecls = inputs.map{ dVar =>
            WdlRewrite.declaration(dVar.womType, dVar.sanitizedName, None)
        }

        // rename usages of the input variables in the statment block
        val statementsRn = statements.map{
            case stmt => statementRenameVars(stmt, inputs)
        }.toVector

        // output variables, and how to references them outside this workflow.
        val (outputDecls, xtrnRefs) = outputs.map { dVar =>
            val srcExpr = WdlExpression.fromString(dVar.name)
            val wo = WdlRewrite.workflowOutput(dVar.sanitizedName, dVar.womType, srcExpr)
            val xtrnRef = DVar(dVar.name,   // original name
                               dVar.womType,
                               subWfName + "." + wo.unqualifiedName)
            (wo, xtrnRef)
        }.unzip

        val emptyWf = WdlRewrite.workflowGenEmpty(subWfName)
        val wf = WdlRewrite.workflow(emptyWf,
                                     inputDecls ++ statementsRn ++ outputDecls)
        (wf, xtrnRefs)
    }

    // Replace the bottom statments with a call to a subworkflow
    private def rewriteBlock(oldStmt: Scope,
                             topDecls: Seq[Declaration],
                             subWf: WdlWorkflow,
                             inputs: Vector[DVar]) : Scope = {
        val subWfInputs = inputs.map{ dVar =>
            Utils.transformVarName(dVar.name) -> WdlExpression.fromString(dVar.name)
        }.toMap
        val wfc = WdlRewrite.workflowCall(subWf,
                                          subWfInputs)
        val children = topDecls :+ wfc

        oldStmt match {
            case ssc: Scatter =>
                WdlRewrite.scatter(ssc, children)
            case cond: If =>
                WdlRewrite.cond(cond, children, cond.condition)
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    private def decompose(scope: Scope, subWfNamePrefix: String) : (Scope, WdlWorkflow, Vector[DVar]) = {
        val (topDecls, bottom: Seq[Scope]) = Utils.splitBlockDeclarations(scope.children.toList)
        assert(bottom.length > 1)
        System.err.println(s"split scope=${scope.fullyQualifiedName}  #topDecl=${topDecls.length} numBottom=${bottom.length}")

        // Figure out the free variables in [bottom]
        val btmInputs = freeVars(bottom)

        // Figure out the outputs from the [bottom] statements
        val btmOutputs: Vector[DVar] = blockOutputs(bottom.toVector)

        // create a subworkflow from the bottom statements. Name
        // it based on the first call.
        var subWfName = s"${subWfNamePrefix}_sub_wf_${bottom.head.unqualifiedName}"
        if (subWfName.length > Utils.MAX_STAGE_NAME_LEN)
            subWfName = subWfName.substring(0, Utils.MAX_STAGE_NAME_LEN)
        val (subWf,xtrnVarRefs) = buildSubWorkflow(subWfName, btmInputs, btmOutputs, bottom.toVector)

        // replace the bottom statements with a call to the subworkflow
        val scope2 = rewriteBlock(scope, topDecls, subWf, btmInputs)
/*        System.err.println(s"decompose, convert:")
        System.err.println(WdlPrettyPrinter(true, None, None).apply(scope, 0).mkString("\n"))
        System.err.println(s"decompose, into:")
        System.err.println(WdlPrettyPrinter(true, None, None).apply(scope2, 0).mkString("\n"))
        System.err.println() */
        (scope2, subWf, xtrnVarRefs)
    }



    case class Partition(before: Vector[Scope],
                         lrgBlock: Option[Scope],
                         after: Vector[Scope])

    private def splitWithLargeBlockAsPivot(wf: WdlWorkflow) : Partition = {
        var before = Vector.empty[Scope]
        var lrgBlock: Option[Scope] = None
        var after = Vector.empty[Scope]

        for (stmt <- wf.children) {
            lrgBlock match {
                case None =>
                    if (isLargeSubBlock(stmt))
                        lrgBlock = Some(stmt)
                    else
                        before = before :+ stmt
                case Some(blk) =>
                    after = after :+ stmt
            }
        }
        assert (before.length + lrgBlock.size + after.length == wf.children.length)
        Partition(before, lrgBlock, after)
    }

    // Iterate through the workflow children.
    //  1) Find the first a large scatter/if block, If none exists, we are done.
    //  2) Split the children to those before and after the large-block.
    //     (beforeList, largeBlock, afterList)
    //  3) The beforeList doesn't change
    //  4) The largeBlock is decomposed into a sub-workflow and small block
    //  5) All references in afterList to results from the sub-workflow are
    //     renamed.
    def apply(wf: WdlWorkflow) : (WdlWorkflow, Option[WdlWorkflow]) = {
        // There is at least one large block.
        val Partition(before, lrgBlock, after) = splitWithLargeBlockAsPivot(wf)
        lrgBlock match {
            case None =>
                // All blocks are small, there is nothing to do
                (wf, None)

            case Some(scope) =>
                val (scope2, scope2SubWf, xtrnVarRefs) = decompose(scope, wf.unqualifiedName)
                val afterRn = after.map{ stmt => statementRenameVars(stmt, xtrnVarRefs) }
                val wf2 : WdlWorkflow = WdlRewrite.workflow(wf,
                                                            (before :+ scope2) ++ afterRn)
                (wf2, Some(scope2SubWf))
        }
    }
}

object DecomposeBlocks {
    def apply(nsTree: NamespaceOps.Tree,
              wdlSourceFile: Path,
              verbose: Verbose) : NamespaceOps.Tree = {
        Utils.trace(verbose.on, "Breaking sub-blocks into sub-workflows")

        // Process the original WDL files. Break all large
        // blocks into sub-workflows. Continue until all blocks
        // contain one call (at most).
        var done = false
        var tree = nsTree
        var iter = 0
        while (!done) {
            System.err.println(s"Decompose iteration ${iter}")
            iter = iter + 1
            tree = tree.transform{ case (wf, cef) =>
                val sbw = new DecomposeBlocks(cef, verbose)
                val (wf2, subWf) = sbw.apply(wf)
                subWf match {
                    case None => done = true
                    case _ => done = false
                }
                (wf2, subWf)
            }
        }
        if (verbose.on)
            NamespaceOps.prettyPrint(wdlSourceFile, tree, "subblocks", verbose)
        tree
    }
}
