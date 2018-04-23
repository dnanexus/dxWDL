/*
Break large sub-blocks into separate sub-workflows. Simplistically,
a large subblock is one that has two calls or more. For example
the scatter below is "large". Schematically, the main cases are as
follows:

a) calls are inside the same block. This sitaution is termed *CallLine*.
    if (cond) {
      call A
      call B
    }
a1) same as (a), but the nesting could be deep
   if (cond) {
     scatter (x in xs) {
       call A
       call B
     }
   }

(b) Calls are not inside the same block. This is called a *Fragment*.
workflow w
   if (cond) {
      if (cond2) {
        call A
      }
      if (cond3) {
        call B
      }
   }

Will be decomposed into:
workflow w
  if (cond) {
     wf_A
     wf_B
  }
workflow wf_A
  if (cond2) {
     call A
  }
workflow wf_B
  if (cond3) {
    call B
}


Here are more detailed examples:

workflow w {
  scatter (x in ax) {
    Int y = x + 4
    call add { input: a=y, b=x }
    Int base = add.result
    call mul { input: a=base, n=x}
  }
}

*w* will be broken into a subworkflow, and a scatter that calls it.

workflow w {
  scatter (x in ax) {
    Int y = x + 4
    call wf_add { x=x, y=y }
  }
}

workflow wf_add {
  Int x
  Int y

  call add { input: a=y, b=x }
  Int base = add.result
  call mul { input: a=base, n=x}

  output {
    Int add_result = add.result
    Int out_base = base
    Int mul_result = mul.result
  }
}

Downstream references to 'add.result' will be renamed: wf_add.add_result


The workflow below requires two decomposition steps, the first is shown below, from w to w2.

workflow w {
    Int i
    Array[Int] xa

    if (i >= 0) {
      if (i == 2) {
        scatter (x in xa)
          call inc { input : a=x}
      }
      if (i == 3) {
        scatter (x in xa)
          call add { input: a=x, b=3 }
      }
    }
}

workflow w2 {
    Int i
    Array[Int] xa

    if (i >= 0) {
      call w2_inc { input: i=i, xa=xa }
      call w2_inc { input: i=i, xa=xa }
    }
}

workflow w2_inc {
    Int i
    Array[Int] xa

    if (i == 2) {
      scatter (x in xa)
        call inc { input : a=x}
    }
}

workflow w2_add {
    Int i
    Array[Int] xa

    if (i == 3) {
      scatter (x in xa)
        call add { input: a=x, b=3 }
    }
 }

 */

package dxWDL.compiler

import dxWDL.{CompilerErrorFormatter, Utils, Verbose, WdlPrettyPrinter}
import java.nio.file.Path
import scala.util.{Failure, Success}
import wdl._
import wdl.expression._
import wdl.AstTools.EnhancedAstNode
import wdl.WdlExpression.AstForExpressions
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wom.types._

case class DecomposeBlocks(subWorkflowPrefix: String,
                           cef: CompilerErrorFormatter,
                           verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "decompose"

    // The name field can include dots ("add.result", "mul.result").
    // The sanitized name has no dots, and can be placed in a WDL script (or dx).
    case class DVar(name: String,
                    womType: WomType,
                    sanitizedName: String)
    object DVar {
        // An additional default constructor. Normally,
        // the sanitized name is built by replacing dots with underscores.
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

    // Find all the free variables in a block of statements. For example,
    // for the following block:
    //
    //   call add { input: a=y, b=x }
    //   Int base = add.result
    //   call mul { input: a=base, n=x}
    //
    // The free variables are {x, y}.
    //
    private def freeVarsAccu(statements: Seq[Scope], accu: VarTracker) : VarTracker = {
        statements.foldLeft(accu) {
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

            case (accu, ssc:Scatter) =>
                // Update the tracker based on the scatter collection,
                // then recurse into the children.
                val xtrnVars = exprDeps(ssc.collection, ssc)
                val collectionType = evalType(ssc.collection, ssc)
                val item = DVar(ssc.item,
                                Utils.stripArray(collectionType))
                val accu2 = accu.findNewIn(xtrnVars).addLocal(item)
                freeVarsAccu(ssc.children, accu2)

            case (accu, condStmt:If) =>
                // add any free variables in the condition expression,
                // then recurse into the children.
                val xtrnVars = exprDeps(condStmt.condition, condStmt)
                val accu2 = accu.findNewIn(xtrnVars)
                freeVarsAccu(condStmt.children, accu2)

            case (_, x) =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    private def freeVars(statements: Seq[Scope]) : Vector[DVar] = {
        val varTracker = freeVarsAccu(statements, VarTracker.empty)
        varTracker.outside.values.toVector
    }

    // Figure out all the outputs from a block of statements
    private def blockOutputsAll(statements: Vector[Scope],
                                kind: Block.Kind.Value) : Vector[DVar] = {
        statements.map{
            case decl:Declaration =>
                Vector(DVar(decl.unqualifiedName, decl.womType))
            case call:WdlCall =>
                call.outputs.map { cOut: CallOutput =>
/*                    if (kind == Block.Kind.CallLine)
                        DVar(call.unqualifiedName + "." ++ cOut.unqualifiedName, cOut.womType)
                    else
 DVar(cOut.unqualifiedName, cOut.womType)*/
                    DVar(call.unqualifiedName + "." ++ cOut.unqualifiedName, cOut.womType)
                }
            case ssc:Scatter =>
                val innerVars = blockOutputsAll(ssc.children.toVector, kind)
                innerVars.map{ dVar => dVar.copy(womType = WomArrayType(dVar.womType)) }
            case condStmt:If =>
                val innerVars = blockOutputsAll(condStmt.children.toVector, kind)
                innerVars.map{ dVar => dVar.copy(womType = WomOptionalType(dVar.womType)) }

            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }.flatten
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
        val wordTranslations = inputs.map{ dVar =>
            dVar.name -> dVar.sanitizedName
        }.toMap
        val erv = StatementRenameVars(Set.empty, wordTranslations, cef, verbose)
        val statementsRn = statements.map{
            case stmt => erv.apply(stmt)
        }.toVector

        // output variables, and how to reference them outside this workflow.
        val (outputDecls, xtrnRefs) = outputs.map { dVar =>
            val srcExpr = WdlExpression.fromString(dVar.name)
            val wotName =
                if (dVar.sanitizedName == srcExpr.toWomString) {
                    // Corner case, where we export a local variable. For example:
                    //   k --> out_k
                    "out_" + dVar.sanitizedName
                } else {
                    // Normal case, export results of a call. For example:
                    //   Add.result --> Add_result
                    dVar.sanitizedName
                }
            val wot = new WorkflowOutput(wotName,
                                         dVar.womType,
                                         srcExpr,
                                         WdlRewrite.INVALID_AST,
                                         None)
            val xtrnRef = DVar(dVar.name,   // original name
                               dVar.womType,
                               subWfName + "." + wot.unqualifiedName)
            (wot, xtrnRef)
        }.unzip

        val emptyWf = WdlRewrite.workflowGenEmpty(subWfName)
        val wf = WdlRewrite.workflow(emptyWf,
                                     inputDecls ++ statementsRn ++ outputDecls)
        (wf, xtrnRefs)
    }

    // Excise a subtree from the workflow, replace it with a call
    // to a newly created subworkflow.
    //
    // The workflow is immutable, and we cannot change it in place. Therefore,
    // we write a new tree from the bottom up.
    //
    //    x
    //   / \
    //  y1  y2
    //     / \
    //    z1  z2
    //
    //    x'
    //   / \
    //  y1  y'
    //     / \
    //    z1  z'
    private def replaceStatement(orgStmt: Scope,
                                 freshStmt: Scope) : WdlWorkflow = {
        val parent = orgStmt.parent match {
            case None =>
                throw new Exception(cef.compilerInternalError(
                                        orgStmt.ast,
                                        s"statement without a parent"))
            case Some(x) => x
        }
        // Leave all the children intact, except the one we wish to replace
        val children2 = parent.children.map{
            case stmt if (stmt == orgStmt) => freshStmt
            case stmt => stmt
        }

        // rewrite the parent, and continue up the trail. Stop
        // when reaching the top workflow.
        parent match {
            case ssc: Scatter =>
                val ssc2 = WdlRewrite.scatter(ssc, children2)
                replaceStatement(parent, ssc2)
            case condStmt: If =>
                val condStmt2 = WdlRewrite.cond(condStmt, children2, condStmt.condition)
                replaceStatement(parent, condStmt2)
            case wf: WdlWorkflow =>
                WdlRewrite.workflow(wf, children2)
            case x =>
                throw new Exception(s"unexpected workflow element ${x.getClass.getSimpleName}")
        }
    }

    private def giveNameToCallLineSubworkflow(subtree: Scope) : String = {
        def sanitize(name:String) :String =
            if (name.startsWith(subWorkflowPrefix))
                name.substring(subWorkflowPrefix.length + 1)
            else
                name

        val calls = Block.findCalls(subtree)
        assert(calls.length >= 2)
        val hd = sanitize(calls.head.unqualifiedName)
        val tl = sanitize(calls.last.unqualifiedName)
        val fullName = s"${subWorkflowPrefix}_${hd}_${tl}"
        fullName.take(Utils.MAX_STAGE_NAME_LEN)
    }

    // The subtree looks like this:
    //   if (cond) {
    //      call A
    //      call B
    //      call C
    //   }
    //
    // Rewrite it as:
    //   if (cond)
    //      call subwf_A_B_C
    private def decomposeCallLine(wf: WdlWorkflow,
                                  subtree: Scope,
                                  kind: Block.Kind.Value)
            : (WdlWorkflow, WdlWorkflow, WdlWorkflowCall, Vector[DVar]) = {
        // Figure out the free variables in the subtree
        val sbtInputs0 = freeVars(subtree.children)

        // Some of the bottom variables are temporary. There is downstream code
        // thet gets confused by such a variable being also a workflow input.
        // Change these names.
        val sbtInputs = sbtInputs0.map{ dVar =>
            if (Utils.isGeneratedVar(dVar.name))
                DVar(dVar.name, dVar.womType, "in_" + dVar.name)
            else
                dVar
        }

        // Figure out the outputs from the subtree
        val sbtOutputs: Vector[DVar] = blockOutputsAll(subtree.children.toVector, kind)

        // create a separate subworkflow from the subtree. Name
        // it by concatenating the call names.
        val subWfName = giveNameToCallLineSubworkflow(subtree)
        val (subWf, xtrnVarRefs) = buildSubWorkflow(subWfName,
                                                    sbtInputs, sbtOutputs,
                                                    subtree.children.toVector)

        // replace the subtree with a call to the subworkflow
        val subWfInputs = sbtInputs.map{ dVar =>
            dVar.sanitizedName -> WdlExpression.fromString(dVar.name)
        }.toMap
        val wfc = WdlRewrite.workflowCall(subWf, subWfInputs)
        val freshSubtree = subtree match {
            case ssc: Scatter =>
                WdlRewrite.scatter(ssc, Vector(wfc))
            case cond: If =>
                WdlRewrite.cond(cond, Vector(wfc), cond.condition)
            case x =>
                throw new Exception(cef.compilerInternalError(
                                        x.ast,
                                        s"unexpected workflow element ${x.getClass.getSimpleName}"))
        }
        val wf2 = replaceStatement(subtree, freshSubtree)
        (wf2, subWf, wfc, xtrnVarRefs)
    }

    // Extract [subTree] from a workflow, make a subworkflow out of it
    private def decomposeFragment(wf: WdlWorkflow,
                                  subtree: Scope,
                                  kind: Block.Kind.Value)
            : (WdlWorkflow, WdlWorkflow, WdlWorkflowCall, Vector[DVar]) = {
        // Figure out the free variables in the subtree
        val sbtInputs0 = freeVars(Seq(subtree))

        // Some of the bottom variables are temporary. There is downstream code
        // thet gets confused by such a variable being also a workflow input.
        // Change these names.
        val sbtInputs = sbtInputs0.map{ dVar =>
            if (Utils.isGeneratedVar(dVar.name))
                DVar(dVar.name, dVar.womType, "in_" + dVar.name)
            else
                dVar
        }

        // Figure out the outputs from the subtree
        val sbtOutputs: Vector[DVar] = blockOutputsAll(Vector(subtree), kind)

        // create a separate subworkflow from the subtree. Create
        // a human readable name.
        val sbtCalls = Block.findCalls(subtree)
        assert(sbtCalls.length == 1)
        var subWfName = sbtCalls.head.unqualifiedName
        if (subWfName.startsWith(subWorkflowPrefix)) {
            // We are calling a generated sub-workflow
            //  1) strip the prefix (we are going to add it below)
            //  2) add short description of this new sub-workflow.
            subWfName = subWfName.substring(subWorkflowPrefix.length + 1)
            val summary = subtree match {
                case _:Scatter => "scatter"
                case _:If => "if"
                case _ => "frag"
            }
            subWfName = s"${summary}_${subWfName}"
        }

        // prefix the name, and make sure it doesn't go beyond a certain limit
        subWfName = s"${subWorkflowPrefix}_${subWfName}"
        subWfName = subWfName.take(Utils.MAX_STAGE_NAME_LEN)

        val (subWf, xtrnVarRefs) = buildSubWorkflow(subWfName,
                                                    sbtInputs, sbtOutputs, Vector(subtree))

        // replace the subtree with a call to the subworkflow
        val subWfInputs = sbtInputs.map{ dVar =>
            dVar.sanitizedName -> WdlExpression.fromString(dVar.name)
        }.toMap
        val wfc = WdlRewrite.workflowCall(subWf, subWfInputs)
        val wf2 = replaceStatement(subtree, wfc)
        (wf2, subWf, wfc, xtrnVarRefs)
    }

    // Replace [child], which is a subtree of the workflow, with a call to
    // a subworkflow. Rename usages of variables calculated inside the
    // newly created sub-workflow.
    def apply(wf: WdlWorkflow,
              child: Block.ReducibleChild) : (WdlWorkflow, WdlWorkflow) = {
        val (topWf, subWf, wfc, xtrnVarRefs) = child.kind match {
            case Block.Kind.CallLine =>
                decomposeCallLine(wf, child.scope, child.kind)
            case Block.Kind.Fragment =>
                decomposeFragment(wf, child.scope, child.kind)
        }

        if (verbose2) {
            val lines = WdlPrettyPrinter(true, None).apply(subWf, 0).mkString("\n")
            Utils.trace(verbose2, s"subwf = ${lines}")
            val topWfLines = WdlPrettyPrinter(true, None).apply(topWf, 0).mkString("\n")
            Utils.trace(verbose2, s"topWf = ${topWfLines}")
        }

        // Note: we are apply renaming to the entire toplevel workflow, including
        // the parts before the subtree that was actually rewritten. This is legal
        // because renamed elements only appear -after- the subtree. Nothing will
        // be renmaed -before- it.
        val xtrnUsageDict = xtrnVarRefs.map{ dVar =>
            dVar.name -> dVar.sanitizedName
        }.toMap
        //Utils.trace(verbose2, s"Renaming variable uses")
        val topWf2 = StatementRenameVars(Set(wfc), xtrnUsageDict, cef, verbose).apply(topWf)
        (topWf2.asInstanceOf[WdlWorkflow], subWf)
    }
}

// Process the original WDL files. Break all large
// blocks into sub-workflows. Continue until all blocks
// contain one call (at most).
//
// For each workflow, iterate through its children, and find subtrees to break off
// into subworkflows. The stopping conditions is:
//    Either there are no if/scatter blocks,
//    or, the remaining blocks contain a single call
//
object DecomposeBlocks {

    def apply(nsTree: NamespaceOps.Tree,
              wdlSourceFile: Path,
              ctx: NamespaceOps.Context,
              verbose: Verbose) : NamespaceOps.Tree = {
        Utils.trace(verbose.on, "Breaking sub-blocks into sub-workflows")

        // Prefix all the generated subworkflows with a consistent
        // string
        val subwfPrefix = nsTree.name

        var tree = nsTree
        var iter = 0
        var done = false
        while (!done) {
            done = tree match {
                case node: NamespaceOps.TreeNode if Block.countCalls(node.workflow.children) >= 2 =>
                    //  1) Find the first reducible scatter/if block, If none exists, we are done.
                    //  2) Split the children to those before and after the large-block.
                    //     (beforeList, largeBlock, afterList)
                    //  3) The beforeList doesn't change
                    //  4) The largeBlock is decomposed into a sub-workflow and small block
                    //  5) All references in afterList to results from the sub-workflow are
                    //     renamed.
                    Block.findReducibleChild(node.workflow.children.toVector, verbose) match {
                        case None => true
                        case Some(child) =>
                            Utils.trace(verbose.on, s"Decompose iteration ${iter}")
                            iter = iter + 1
                            def decomposeOp(wf: WdlWorkflow,
                                            cef: CompilerErrorFormatter,
                                            ctx: NamespaceOps.Context) = {
                                val sbw = new DecomposeBlocks(subwfPrefix, cef, verbose)
                                val (wf2, subWf) = sbw.apply(wf, child)
                                (wf2, Some(subWf))
                            }
                            tree = tree.transform(decomposeOp, ctx)
                            false
                    }
                case _ => true
            }
        }
        if (verbose.on)
            NamespaceOps.prettyPrint(wdlSourceFile, tree, "subblocks", verbose)
        tree
    }
}
