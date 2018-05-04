/*
Break large sub-blocks into separate sub-workflows. See more details
in doc/Internals.md.

Simplistically, a large subblock is one that has two calls or
more. For example the scatter below is "large". Schematically, the
main cases are as follows:

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
     wf2
  }

workflow wf2
  if (cond2) {
     call A
  }
  if (cond3) {
    call B
  }


The next example also shows variable renaming. Workflow *w* below,
is broken into a subworkflow, and a scatter that calls it.

workflow w {
  scatter (x in ax) {
    Int y = x + 4
    call add { input: a=y, b=x }
    Int base = add.result
    call mul { input: a=base, n=x}
  }
}


workflow w {
  scatter (x in ax) {
    Int y = x + 4
    call w_scatter_x_body { x=x, y=y }
  }
}

workflow w_scatter_x_body {
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

Downstream references to 'add.result' will be renamed: w_scatter_x_body.add_result
 */

package dxWDL.compiler

import dxWDL.{CompilerErrorFormatter, Utils, Verbose, WdlPrettyPrinter}
import java.nio.file.Path
import scala.util.matching.Regex.Match
import wdl._
import wom.types._

case class Decompose(subWorkflowPrefix: String,
                     subwfNames: NameBox,
                     cef: CompilerErrorFormatter,
                     verbose: Verbose) {
    private val verbose2:Boolean = verbose.keywords contains "decompose"
    private val freeVarAnalysis = new FreeVarAnalysis(cef, verbose)

    // A WDL source variable with a uniquely chosen name. The name
    // is:
    //  1) As much as possible, human readable
    //  2) Has no dots or special characters. Can be used as
    //     a WDL identifier, and appear well in the UI.
    case class DVarHr(fullyQualifiedName: String,
                      womType: WomType,
                      expr: WdlExpression,
                      conciseName: String)

    // Figure out all the outputs from a block of statements
    private def blockOutputsAll(statements: Vector[Scope]) : Vector[DVar] = {
        statements.map{
            case decl:Declaration =>
                Vector(DVar(decl.unqualifiedName, decl.womType))
            case call:WdlCall =>
                call.outputs.map { cOut: CallOutput =>
                    DVar(call.unqualifiedName + "." ++ cOut.unqualifiedName, cOut.womType)
                }
            case ssc:Scatter =>
                val innerVars = blockOutputsAll(ssc.children.toVector)
                innerVars.map{ dVar => dVar.copy(womType = WomArrayType(dVar.womType)) }
            case condStmt:If =>
                val innerVars = blockOutputsAll(condStmt.children.toVector)
                innerVars.map{ dVar => dVar.copy(womType = WomOptionalType(dVar.womType)) }

            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }.flatten
    }

    private def findAllDeclarations(statements: Vector[Scope]) : Vector[Declaration] = {
        statements.foldLeft(Vector.empty[Declaration]) {
            case (accu, decl:Declaration) =>
                accu :+ decl
            case (accu, call:WdlCall) =>
                accu
            case (accu, ssc:Scatter) =>
                accu ++ findAllDeclarations(ssc.children.toVector)
            case (accu, condStmt:If) =>
                accu ++ findAllDeclarations(condStmt.children.toVector)
            case (accu, x) =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    private def pickUnusedName(originalName: String,
                               _alreadyUsedNames: Set[String]) : String = {
        val alreadyUsedNames = Utils.RESERVED_WORDS ++ _alreadyUsedNames

        // Try the original name
        if (!(alreadyUsedNames contains originalName))
            return originalName

        // Add [1,2,3 ...] to the original name, until finding
        // an unused variable name
        for (i <- 1 to Utils.DECOMPOSE_MAX_NUM_RENAME_TRIES) {
            val tentative = s"${originalName}${i}"
            if (!(alreadyUsedNames contains tentative))
                return tentative
        }
        throw new Exception(s"Could not find a unique name for variable ${originalName}")
    }

    // Choose good names for the variables. The names:
    // 1) must be unique within this scope
    // 2) should be as close to the original WDL as possible
    // 3) should be succinct, and human readable
    private def chooseConciseNames(dVars: Vector[DVar],
                                   alreadyUsedNames: Set[String]) : Vector[DVarHr] = {
        val (allUsedNames, dvhrVec) = dVars.foldLeft((alreadyUsedNames, Vector.empty[DVarHr])) {
            case ((alreadyUsedNames, dvhrVec), dVar) =>
                val sanitizedName = Utils.transformVarName(dVar.unqualifiedName)
                val conciseName = pickUnusedName(sanitizedName, alreadyUsedNames)

                assert(!(alreadyUsedNames contains conciseName))
                assert(conciseName.indexOf('.') == -1)

                val dvhr = DVarHr(dVar.fullyQualifiedName,
                                  dVar.womType,
                                  WdlExpression.fromString(dVar.fullyQualifiedName),
                                  conciseName)
                (alreadyUsedNames + dvhr.conciseName,
                 dvhrVec :+ dvhr)
        }
        dvhrVec
    }


    // Build a valid WDL workflow with [inputs] and [outputs].
    // The body is made up of [statements].
    // 1) Rename the input variables (A.B -> A_B)
    // 2) Setup the output variables
    //
    // Return the subworkflow, and the new names of all the outputs. We will
    // need to rename all the references to them.
    private def buildSubWorkflow(subWfName: String,
                                 inputs: Vector[DVarHr],
                                 outputs: Vector[DVarHr],
                                 statements: Vector[Scope]) : (WdlWorkflow, Vector[DVarHr]) = {
        val inputDecls = inputs.map{ dvhr =>
            WdlRewrite.declaration(dvhr.womType, dvhr.conciseName, None)
        }

        // rename usages of the input variables in the statment block
        val wordTranslations = inputs.map{ dvhr =>
            dvhr.fullyQualifiedName -> dvhr.conciseName
        }.toMap
        val erv = VarAnalysis(Set.empty, wordTranslations, cef, verbose)
        val statementsRn = statements.map{
            case stmt => erv.rename(stmt)
        }.toVector

        // output variables, and how to reference them outside this workflow.
        val (outputDecls, xtrnRefs) = outputs.map { dvhr =>
            // Export results of a call. For example:
            //   Add.result --> Add_result
            val wot = new WorkflowOutput(dvhr.conciseName,
                                         dvhr.womType,
                                         dvhr.expr,
                                         WdlRewrite.INVALID_AST,
                                         None)
            val xtrnRef = DVarHr(dvhr.fullyQualifiedName,  // original name
                                 dvhr.womType,
                                 WdlExpression.fromString(dvhr.fullyQualifiedName),
                                 subWfName + "." + dvhr.conciseName)
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

    private def createNameForSubWorkflow(subtree: Scope) : String = {
        val shortBlockDesc = subtree match {
            case ssc: Scatter =>
                s"scatter_${ssc.item}"
            case cond: If =>
                val condRawString = cond.condition.toWomString
                // Leave only identifiers
                val fqnRegex = raw"""[a-zA-Z][a-zA-Z0-9_.]*""".r
                val matches: List[Match] = fqnRegex.findAllMatchIn(condRawString).toList
                matches.map{ _.toString}.mkString("_")
            case x =>
                throw new Exception(cef.compilerInternalError(
                                        x.ast,
                                        s"unexpected workflow element ${x.getClass.getSimpleName}"))
        }
        val baseName = s"${subWorkflowPrefix}_${shortBlockDesc}_body"
        subwfNames.chooseUniqueName(baseName)
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
    private def decompose(wf: WdlWorkflow,
                          subtree: Scope)
            : (WdlWorkflow, WdlWorkflow, WdlWorkflowCall, Vector[DVarHr]) = {
        val body = subtree.children.toVector

        // Figure out the free variables in the subtree
        val sbtInputs = freeVarAnalysis.apply(body)
        val sbtInputsHr = chooseConciseNames(sbtInputs, Set.empty)

        // Figure out the outputs from the subtree
        val sbtOutputs: Vector[DVar] = blockOutputsAll(body)
        val namesToAvoid =
            sbtInputsHr.map(_.conciseName) ++
                sbtInputsHr.map(_.fullyQualifiedName) ++
                findAllDeclarations(body).map(_.unqualifiedName)
        val sbtOutputsHr = chooseConciseNames(sbtOutputs, namesToAvoid.toSet)

        // create a separate subworkflow from the subtree.
        val subWfName = createNameForSubWorkflow(subtree)
        val (subWf, xtrnVarRefs) =
            buildSubWorkflow(subWfName, sbtInputsHr, sbtOutputsHr, body)

        // replace the subtree with a call to the subworkflow
        val subWfInputs = sbtInputsHr.map{ dvhr =>
            dvhr.conciseName -> dvhr.expr
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

    // Replace [child], which is a subtree of the workflow, with a call to
    // a subworkflow. Rename usages of variables calculated inside the
    // newly created sub-workflow.
    def apply(wf: WdlWorkflow, child: Scope) : (WdlWorkflow, WdlWorkflow) = {
        if (verbose2) {
            val childLines = WdlPrettyPrinter(true, None).apply(child, 0).mkString("\n")
            val wfLines = WdlPrettyPrinter(true, None).apply(wf, 0).mkString("\n")
            Utils.trace(verbose2,
                        s"""|=== apply1
                            |  topWf = ${wfLines}
                            |  child = ${childLines}
                            |""".stripMargin)
        }

        val (topWf, subWf, wfc, xtrnVarRefs) = decompose(wf, child)

        if (verbose2) {
            val lines = WdlPrettyPrinter(true, None).apply(subWf, 0).mkString("\n")
            val topWfLines = WdlPrettyPrinter(true, None).apply(topWf, 0).mkString("\n")
            Utils.trace(verbose2,
                        s"""|=== apply2
                            |  topWf = ${topWfLines}
                            |  subwf = ${lines}
                            |""".stripMargin)
        }

        // Note: we are apply renaming to the entire toplevel workflow, including
        // the parts before the subtree that was actually rewritten. This is legal
        // because renamed elements only appear -after- the subtree. Nothing will
        // be renmaed -before- it.

        val xtrnUsageDict = xtrnVarRefs.map{ dVar =>
            dVar.fullyQualifiedName -> dVar.conciseName
        }.toMap
        val topWf2 = VarAnalysis(Set(wfc), xtrnUsageDict, cef, verbose).rename(topWf)
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
object Decompose {
    case class Counter() {
        private var i = 0

        def get : Int = i

        def increment() : Unit = {
            if (i > Utils.MAX_NUM_REDUCE_ITERATIONS)
                throw new Exception(
                    s"""|Internal error: more than
                        |${Utils.MAX_NUM_REDUCE_ITERATIONS} reduce iterations"""
                        .stripMargin.replaceAll("\n", " "))
            i = i + 1
        }
    }

    // Reduce the top tree node until it becomes unreducible.
    private def reduceNode(orgNode: NamespaceOps.TreeNode,
                           wdlSourceFile: Path,
                           ctx: NamespaceOps.Context,
                           counter: Counter,
                           verbose: Verbose) : NamespaceOps.Tree = {
        val verbose2:Boolean = verbose.keywords contains "decompose"
        Utils.trace(verbose.on, "Breaking sub-blocks into sub-workflows")

        // Prefix all the generated subworkflows with a consistent
        // string
        val subwfPrefix = s"${orgNode.workflow.unqualifiedName}_"

        var node = orgNode
        var done = false
        val subwfNames = new NameBox(verbose)
        while (!done) {
            //  1) Find the first reducible scatter/if block, If none exists, we are done.
            //  2) Split the children to those before and after the large-block.
            //     (beforeList, largeBlock, afterList)
            //  3) The beforeList doesn't change
            //  4) The largeBlock is decomposed into a sub-workflow and small block
            //  5) All references in afterList to results from the sub-workflow are
            //     renamed.
            done = Block.findReducibleChild(node.workflow.children.toVector, verbose) match {
                case None => true
                case Some(child) =>
                    Utils.trace(verbose.on, s"Decompose iteration ${counter.get}")
                    val sbw = new Decompose(subwfPrefix, subwfNames, node.cef, verbose)
                    val (wf2, subWf) = sbw.apply(node.workflow, child)
                    node = node.cleanAfterRewrite(wf2, subWf, ctx, node.kind)
                    if (verbose2)
                        NamespaceOps.prettyPrint(wdlSourceFile, node, s"subblocks_${counter.get}", verbose)
                    counter.increment()
                    false
            }
        }

        // Mark the node as fully-reduced, so we do not try to reduce it again
        node.copy(fullyReduced = true)
    }

    // Perform one reduction operation on each node that is not already
    // fully reduced. Note that this can create children requiring
    // additional passes.
    private def reduceEntireTree(nsTree: NamespaceOps.Tree,
                                 wdlSourceFile: Path,
                                 ctx: NamespaceOps.Context,
                                 counter: Counter,
                                 verbose: Verbose) : NamespaceOps.Tree = {
        nsTree match {
            case leaf: NamespaceOps.TreeLeaf =>
                leaf
            case node: NamespaceOps.TreeNode =>
                // recurse into the children
                val children = node.children.map{ child =>
                    reduceEntireTree(child, wdlSourceFile, ctx, counter, verbose)
                }
                val node1 = node.copy(children = children)

                // reduce this node if needed
                if (node1.fullyReduced) {
                    node1
                } else {
                    // This operation leaves children that require reduction
                    reduceNode(node1,
                               wdlSourceFile,
                               ctx,
                               counter,
                               verbose)
                }
        }
    }

    // check if all the node in the tree have been reduced.
    private def isTreeFullyReduced(nsTree: NamespaceOps.Tree) : Boolean = {
        nsTree match {
            case _: NamespaceOps.TreeLeaf => true
            case node: NamespaceOps.TreeNode =>
                (node.fullyReduced &&
                     node.children.forall(child => isTreeFullyReduced(child)))
        }
    }

    def apply(orgTree: NamespaceOps.Tree,
              wdlSourceFile: Path,
              ctx: NamespaceOps.Context,
              verbose: Verbose) : NamespaceOps.Tree = {
        // iterate on the tree until it is fully reduced
        val counter = new Counter()
        var nsTree = orgTree
        while (!isTreeFullyReduced(nsTree)) {
            nsTree = reduceEntireTree(nsTree, wdlSourceFile, ctx, counter, verbose)
        }

        if (verbose.on)
            NamespaceOps.prettyPrint(wdlSourceFile, nsTree, "subblocks", verbose)

        nsTree
    }
}
