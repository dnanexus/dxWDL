/**
  *  Reorganize the WDL file. Move the declarations, where possible,
  *  to minimize the number of applets and workflow stages.
  */
package dxWDL.compiler

import dxWDL.{CompilerErrorFormatter, Utils, Verbose}
import java.nio.file.Path
import wdl._

case class ReorgDecl(cef: CompilerErrorFormatter,
                     verbose: Verbose) {
    val MAX_NUM_COLLECT_ITER = 10
    val verbose2:Boolean = verbose.keywords contains "reorg"

    case class DeclReorgState(definedVars: Set[String],
                              top: Vector[Scope],
                              bottom: Vector[Scope])

    // Representation of a block of statements (scatter, if, loop, etc.)
    case class Block(children: Vector[Scope],
                     definedVars: Set[String])

    // Find all the variables defined in a statement
    def definitions(scope: Scope) : Set[String] = {
        scope match {
            case ssc:Scatter =>
                ssc.children.foldLeft(Set.empty[String]) {
                    (accu,child) => accu ++ definitions(child)
                }
            case cond:If =>
                cond.children.foldLeft(Set.empty[String]) {
                    (accu,child) => accu ++ definitions(child)
                }
            case call:WdlCall => Set(call.fullyQualifiedName)
            case decl:Declaration => Set(decl.fullyQualifiedName)
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    // Check if the expression can be evaluated based on a set
    // of variables.
    private def dependsOnlyOnVars(decl: Declaration,
                                  expr: WdlExpression,
                                  definedVars: Set[String]) : Boolean = {
        // Figure out everything that this expression depends on
        val nodeRefs:Set[String] = decl.upstream
            .collect{ case d:DeclarationInterface => d }
            .map(_.fullyQualifiedName)
            .toSet
        val memberAccesses:Set[String] = expr.topLevelMemberAccesses
            .map(ma => ma.lhs)
            .toSet
        val vars:Set[String] = expr.variableReferences(decl)
            .map(varRef => varRef.terminal.getSourceString)
            .toSet
        val refsWithPossibleDups:Set[String] = nodeRefs ++ memberAccesses ++ vars

        // The above may hold duplicates, such as x and A.B.x. Remove
        // duplicates by using the longest string, out of (x, B.x, A.B.x)
        val refs = refsWithPossibleDups.foldLeft(Set.empty[String]) { case (accu, fqn) =>
            if (accu.exists(_.endsWith(fqn)))
                accu
            else
                accu + fqn
        }

        // The memeber accesses and variable-references may not be
        // fully qualified, so try prefixing the FQN too.
        val fqn = decl.ancestry.head.fullyQualifiedName
        val retval = refs.forall{ x => definedVars contains x }
        Utils.trace(verbose2, s"""|dependsOnlyOnVars ${expr.toWomString}
                                  |  fqn = ${fqn}
                                  |  nodeRefs=${nodeRefs}
                                  |  upstream=${refs}
                                  |  defs=${definedVars}
                                  |  ${retval}""".stripMargin)
        retval
    }

    // Start from a split of the workflow elements into top and bottom blocks.
    // Move any declaration, whose dependencies are satisfied, to the top.
    //
    private def declMoveUp(drs: DeclReorgState) : DeclReorgState = {
        var definedVars : Set[String] = drs.definedVars
        var moved = Vector.empty[Scope]

        // Move up declarations
        var bottom : Vector[Scope] = drs.bottom.map {
            case decl: Declaration =>
                decl.expression match {
                    case None =>
                        // An input parameter, move to top
                        definedVars = definedVars ++ definitions(decl)
                        moved = moved :+ decl
                        None
                    case Some(expr) if dependsOnlyOnVars(decl, expr, definedVars)  =>
                        // Move the declaration to the top
                        definedVars = definedVars ++ definitions(decl)
                        moved = moved :+ decl
                        None
                    case _ =>
                        // some dependency is missing, can't move up
                        Some(decl)
                }
            case x => Some(x)
        }.flatten

        // If the next element is a declaration, move it up; it is "stuck"
        bottom = bottom match {
            case (Declaration(_,_,_,_,_)) +: tl =>
                moved = moved :+ bottom.head
                definedVars = definedVars ++ definitions(bottom.head)
                tl
            case _ => bottom
        }
        // Skip all consecutive non-declarations
        def skipNonDecl(t: Vector[Scope], b: Vector[Scope], defs: Set[String]) :
                (Vector[Scope], Vector[Scope], Set[String]) = {
            if (b.isEmpty) {
                (t, b, defs)
            } else {
                b.head match {
                    case decl: Declaration =>
                        // end of skip section
                        (t, b, defs)
                    case x =>
                        // dive in, and potentially reorganize the block
                        val (x1, x1defs) = reorg(x, definedVars)
                        skipNonDecl(t :+ x1, b.tail, defs ++ x1defs)
                }
            }
        }
        val (nonDeclBlock, remaining, nonDeclVars) = skipNonDecl(Vector.empty, bottom, Set.empty)
        if (!nonDeclVars.isEmpty)
            Utils.trace(verbose2, s"Not moving definitions ${nonDeclVars}")
        DeclReorgState(definedVars ++ nonDeclVars,
                       drs.top ++ moved ++ nonDeclBlock,
                       remaining)
    }

    // Attempt to collect declarations, to reduce the number of extra jobs
    // required for calculations. This is an N^2 algorithm, so we bound the
    // number of iterations.
    //
    // workflow math {
    //     Int ai
    //     call Add  { input:  a=ai, b=3 }
    //     Int scratch = 3
    //     Int xtmp2 = Add.result + 10
    //     call Multiply  { input: a=xtmp2, b=2 }
    // }
    //
    // workflow math {
    //     Int ai
    //->   Int scratch = 3
    //     call Add  { input:  a=ai, b=3 }
    //     Int xtmp2 = Add.result + 10
    //     call Multiply  { input: a=xtmp2, b=2 }
    // }
    private def collectDeclarations(drsInit:DeclReorgState) : Block = {
        var drs = drsInit
        var numIter = 0
        val totNumElems = drs.bottom.length
        while (!drs.bottom.isEmpty && numIter < MAX_NUM_COLLECT_ITER) {
/*            System.err.println(
                s"""|collectDeclaration ${numIter}
                    |size(defs)=${drs.definedVars.size} len(top)=${drs.top.length}
                    |len(bottom)=${drs.bottom.length}""".stripMargin.replaceAll("\n", " ")
            )*/
            drs = declMoveUp(drs)
            assert(totNumElems == drs.top.length + drs.bottom.length)
            numIter += 1
        }
        Block(drs.top ++ drs.bottom, drs.definedVars)
    }

    // Attempt to collect declarations at the block level, to reduce
    // the number of extra jobs required for calculations.
    private def reorgBlock(oldBlock:Scope, elems: Seq[Scope], definedVars: Set[String]) : Block  = {
        Utils.trace(verbose2, s"reorgBlock(${oldBlock.fullyQualifiedName})")
        val drs = DeclReorgState(definedVars, Vector.empty[Scope], elems.toVector)
        collectDeclarations(drs)
    }

    private def reorg(scope: Scope, definedVars: Set[String]): (Scope, Set[String]) = {
        scope match {
            case ssc:Scatter =>
                val blk = reorgBlock(ssc, ssc.children, definedVars + ssc.item)
                val ssc2 = WdlRewrite.scatter(ssc, blk.children, ssc.collection)
                (ssc2, blk.definedVars)
            case cond:If =>
                val blk = reorgBlock(cond, cond.children, definedVars)
                val cond2 = WdlRewrite.cond(cond, blk.children, cond.condition)
                (cond2, blk.definedVars)
            case call:WdlCall => (call, definedVars ++ definitions(call))
            case decl:Declaration => (decl, definedVars ++ definitions(decl))
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    def reorgWorkflow(wf: WdlWorkflow) : WdlWorkflow = {
        // split out the workflow outputs
        val wfProper = wf.children.filter(x => !x.isInstanceOf[WorkflowOutput])
        val wfOutputs :Vector[WorkflowOutput] = wf.outputs.toVector

        val blk = reorgBlock(wf, wfProper, Set.empty)
        WdlRewrite.workflow(wf, blk.children ++ wfOutputs)
    }
}

object ReorgDecl {
    def apply(nsTree: NamespaceOps.Tree,
              wdlSourceFile: Path,
              verbose: Verbose) : NamespaceOps.Tree = {
        Utils.trace(verbose.on, "Reorganizing declarations")

        // Process the original WDL file, rewrite all the workflows.
        // Do not modify the tasks.
        val nsTree1 = nsTree.transform{ case (wf, cef) =>
            val rg = new ReorgDecl(cef, verbose)
            val wf2 = rg.reorgWorkflow(wf)
            (wf2, None)
        }
        if (verbose.on)
            NamespaceOps.prettyPrint(wdlSourceFile, nsTree1, "reorg", verbose)
        nsTree1
    }
}
