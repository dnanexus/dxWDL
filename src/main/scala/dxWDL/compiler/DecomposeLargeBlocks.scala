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
import IR.{CVar}
import java.nio.file.Path
import wdl._

case class DecomposeLargeBlocks(cef: CompilerErrorFormatter,
                            verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "subwf"

    // Environment (scope) where a call is made
    private type CallEnv = Map[String, LinkedVar]

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

    // Find all the free variables in a block of statements. For example,
    // for the following block:
    //
    //   call add { input: a=y, b=x }
    //   Int base = add.result
    //   call pow { input: a=base, n=x}
    //
    // The free variables are {x, y}.
    //
    // Figure out the closure for a block, and then build the input
    // definitions.
    //
    //  preDecls: Declarations that come immediately before the block.
    //            We pack them into the same applet.
    //  topBlockExpr: condition variable, scatter loop expression, ...
    //  topDecls: declarations inside the block, that come at the beginning
    //  env: environment outside the block
    private def blockInputs(topDecls: Vector[Declaration],
                            calls: Vector[WdlCall],
                            env : CallEnv) : (CallEnv, Vector[CVar]) = {
        var closure = Map.empty[String, LinkedVar]

        // Get closure dependencies from the top declarations
        topDecls.foreach { decl =>
            decl.expression match {
                case Some(expr) =>
                    closure = CUtil.updateClosure(closure, env, expr)
                case None => ()
            }
        }
        // Make a pass on the calls inside the block
        calls.foreach { call =>
            call.inputMappings.foreach { case (_, expr) =>
                closure = CUtil.updateClosure(closure, env, expr)
            }
        }

        val inputVars: Vector[CVar] = closure.map {
            case (varName, LinkedVar(cVar, _)) =>
                // a variable that must be passed to the scatter applet
                assert(env contains varName)
                Some(CVar(varName, cVar.womType, DeclAttrs.empty, cVar.ast))
        }.flatten.toVector

        (closure, inputVars)
    }

    private def decompose(scope: Scope, wf: WdlWorkflow) : (Scope, WdlWorkflow) = {
        val (topDecls, bottom:Vector[Scope]) = Utils.splitBlockDeclarations(scope.children.toList)
        assert(bottom.length > 1)

        // Figure out the free variables in [rest]
        // Figure out the input definitions
        val (closure, inputVars) = blockInputs(topDecls, bottom, env)

    }


    // Iterate over all the workflow children. Go into the large scatter/if blocks,
    // and break them into block+subWorkflow.
    def apply(wf: WdlWorkflow) : (WdlWorkflow, Vector[WdlWorkflow]) = {
        case class Accu(children: Vector[Scope],
                        subWf: Vector[WdlWorkflow],
                        env: CallEnv)
        val initAccu = Accu(Vector.empty, Vector.empty, Map.empty)

        val accu =
            wf.children.foldLeft(initAccu) {
                case (Accu(children, subWf, env), scope) if isLargeSubBlock(scope) =>
                    val (scope2, scope2SubWf, outputs) = decompose(scope, wf)
                    Accu(children :+ scope2,
                         subWf :+ scope2SubWf,
                         env ++ outputs)

                case (Accu(children, subWf), scope) =>
                    Accu(children :+ scope, subWf)
            }
        val wf2 = WdlRewrite.workflow(wf, accu.children)
        (wf2, accu.subWf)
    }
}

object DecomposeLargeBlocks {
    def apply(nsTree: NamespaceOps.Tree,
              wdlSourceFile: Path,
              verbose: Verbose) : NamespaceOps.Tree = {
        Utils.trace(verbose.on, "Breaking sub-blocks into sub-workflows")

        // Process the original WDL file, rewrite all the workflows.
        // Do not modify the tasks.
        val nsTree1 = nsTree.transform{ case (wf, cef) =>
            val sbw = new DecomposeLargeBlocks(cef, verbose)
            sbw.apply(wf)
        }
        if (verbose.on)
            NamespaceOps.prettyPrint(wdlSourceFile, nsTree1, "subblocks", verbose)
        nsTree1
    }
}
