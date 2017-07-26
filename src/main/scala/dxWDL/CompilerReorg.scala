/**
  *  Reorganize the WDL file. Move the declarations, where possible,
  *  to minimize the number of applets and workflow stages.
  */
package dxWDL

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.Queue
import scala.util.{Failure, Success, Try}
import wdl4s._
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.expression._
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions

case class CompilerReorg(ns: WdlNamespace, verbose: Boolean) {
    val MAX_NUM_COLLECT_ITER = 10
    val cef = new CompilerErrorFormatter(ns.terminalMap)

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
            case call:Call => Set(call.fullyQualifiedName)
            case decl:Declaration => Set(decl.fullyQualifiedName)
            case wfo:WorkflowOutput => Set(wfo.fullyQualifiedName)
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
        val vars:Set[String] = expr.variableReferences
            .map(WdlExpression.toString(_))
            .toSet
        val refs = nodeRefs ++ memberAccesses ++ vars

        // The memeber accesses and variable-references may not be
        // fully qualified, so try prefixing the FQN too.
        val fqn = decl.ancestry.head.fullyQualifiedName
        val retval = refs.forall{ x =>
            (definedVars contains x) ||
            (definedVars contains (fqn + "." + x))
        }
        Utils.trace(verbose, s"""|dependsOnlyOnVars ${expr.toWdlString}
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
            Utils.trace(verbose, s"Not moving definitions ${nonDeclVars}")
        //Utils.trace(verbose, s"len(nonDecls)=${nonDeclBlock.length} len(rest)=${remaining.length}")
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
    def collectDeclarations(drsInit:DeclReorgState) : Block = {
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
    def reorgBlock(elems: Seq[Scope], definedVars: Set[String]) : Block  = {
        val drs = DeclReorgState(definedVars, Vector.empty[Scope], elems.toVector)
        collectDeclarations(drs)
    }

    def reorg(scope: Scope, definedVars: Set[String]): (Scope, Set[String]) = {
        scope match {
            case ssc:Scatter =>
                val blk = reorgBlock(ssc.children, definedVars + ssc.item)
                val ssc2 = WdlRewrite.scatter(ssc, blk.children, ssc.collection)
                (ssc2, blk.definedVars)
            case cond:If =>
                val blk = reorgBlock(cond.children, definedVars)
                val cond2 = WdlRewrite.cond(cond, blk.children, cond.condition)
                (cond2, blk.definedVars)
            case call:Call => (call, definedVars ++ definitions(call))
            case decl:Declaration => (decl, definedVars ++ definitions(decl))
            case wfo:WorkflowOutput => (wfo, definedVars ++ definitions(wfo))
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    def reorgWorkflow(wf: Workflow) : Workflow = {
        val blk = reorgBlock(wf.children, Set.empty)
        WdlRewrite.workflow(wf, blk.children)
    }

    def apply : WdlNamespace = {
        Utils.trace(verbose, "Reorganizing declarations")

        // Process the original WDL file,
        // Do not modify the tasks
        ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                val wf2 = reorgWorkflow(nswf.workflow)
                WdlRewrite.namespace(nswf, wf2)
            case _ => ns
        }
    }
}
