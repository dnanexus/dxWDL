/*
 * Break large sub-blocks into separate sub-workflows. Simplistically,
 * a large subblock is one that has more than two calls. For example
 * the scatter below is "large".
 *
 * scatter (x in ax) {
 *   Int y = x + 4
 *   call add { input: a=y, b=x }
 *   Int base = add.result
 *   call pow { input: a=base, n=x}
 * }
 *
 *
 * It will be broken into a subworkflow, and a scatter that calls it.
 *
 * scatter (x in ax) {
 *   Int y = x + 4
 *   call wf_add_pow { x=x, y=y }
 * }
 *
 * workflow wf_add_pow {
 *   Int x
 *   Int y
 *   call add { input: a=y, b=x }
 *   Int base = add.result
 *   call pow { input: a=base, n=x}
 *   output {
 *     ...
 *   }
 * }
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

    case class DVar(name: String, womType: WomType)

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
            DVar(varName, womType)
        }.toVector
    }

    // Find all the free variables in a block of statements. For example,
    // for the following block:
    //
    //   call add { input: a=y, b=x }
    //   Int base = add.result
    //   call pow { input: a=base, n=x}
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

    // Build a valid WDL workflow with [inputs] and [outputs].
    // The body is made up of [statements].
    private def buildSubWorkflow(subWfName: String,
                                 inputs: Vector[DVar],
                                 outputs: Vector[DVar],
                                 statements: Vector[Scope]) : WdlWorkflow = {
        val emptyWf = WdlRewrite.workflowGenEmpty(subWfName)
        val inputDecls = inputs.map{ dVar =>
            val normalizedName = Utils.transformVarName(dVar.name)
            WdlRewrite.declaration(dVar.womType, normalizedName, None)
        }

        WdlRewrite.workflow(emptyWf,
                            inputDecls ++ statements)
    }

    private def rewriteBlock(oldStmt, topDecls, subWf, inputs) : Scope = {
        val inputMapping = inputs.map{ dVar =>
            Utils.transformVarName(dVar.name) -> WdlExpression.fromString(dVar.name)
        }
        val wfc = WdlWorkflowCall(None,
                                  subWf,
                                  inputMapping,
                                  WdlRewrite.INVALID_AST)
        val children = topDecls :+ wfc
        oldStmt.copy(children = children)
    }

    private def decompose(scope: Scope, wf: WdlWorkflow) : (Scope, WdlWorkflow) = {
        val (topDecls, bottom: Seq[Scope]) = Utils.splitBlockDeclarations(scope.children.toList)
        assert(bottom.length > 1)

        // Figure out the free variables in [bottom]
        val btmInputs = freeVars(bottom)

        // Figure out the outputs from the [bottom] statements
        val btmOutputs: Vector[DVar] = blockOutputs(bottom.toVector)

        // create a subworkflow from the bottom statements
        val subWfName = "subWf_" ++ scope.fullyQualifiedName
        val subWf = buildSubWorkflow(subWfName, btmInputs, btmOutputs, bottom)

        // replace the bottom statmenets with a call to the subworkflow
        val scope2 = rewriteBlock(scope, topDecls, subWf, btmInputs)
        (scope2, subWf)
    }


    // Iterate over all the workflow children. Go into the large scatter/if blocks,
    // and break them into block+subWorkflow.
    def apply(wf: WdlWorkflow) : (WdlWorkflow, Vector[WdlWorkflow]) = {
        case class Accu(children: Vector[Scope],
                        subWf: Vector[WdlWorkflow])
        val initAccu = Accu(Vector.empty, Vector.empty)

        val accu =
            wf.children.foldLeft(initAccu) {
                case (Accu(children, subWf), scope) if isLargeSubBlock(scope) =>
                    val (scope2, scope2SubWf) = decompose(scope, wf)
                    Accu(children :+ scope2,
                         subWf :+ scope2SubWf)

                case (Accu(children, subWf), scope) =>
                    Accu(children :+ scope, subWf)
            }
        val wf2 = WdlRewrite.workflow(wf, accu.children)
        (wf2, accu.subWf)
    }
}

object DecomposeBlocks {
    def apply(nsTree: NamespaceOps.Tree,
              wdlSourceFile: Path,
              verbose: Verbose) : NamespaceOps.Tree = {
        Utils.trace(verbose.on, "Breaking sub-blocks into sub-workflows")

        // Process the original WDL file, rewrite all the workflows.
        // Do not modify the tasks.
        val nsTree1 = nsTree.transform{ case (wf, cef) =>
            val sbw = new DecomposeBlocks(cef, verbose)
            sbw.apply(wf)
        }
        if (verbose.on)
            NamespaceOps.prettyPrint(wdlSourceFile, nsTree1, "subblocks", verbose)
        nsTree1
    }
}
