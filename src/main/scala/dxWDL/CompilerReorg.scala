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

object CompilerReorg {
    val MAX_NUM_COLLECT_ITER = 10
    var tmpVarCnt = 0

    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(wf: Workflow,
                     cef: CompilerErrorFormatter,
                     verbose: Boolean)

    case class DeclReorgState(definedVars: Set[String],
                              top: Vector[Scope],
                              bottom: Vector[Scope])

    // Representation of a block of statements (scatter, if, loop, etc.)
    case class Block(children: Vector[Scope],
                     definedVars: Set[String])

    // A member access expression such as [A.x]. Check if
    // A is a call.
    private def isCallOutputAccess(expr: WdlExpression,
                                   ast: Ast,
                                   call: Call,
                                   cState: State) : Boolean = {
        val lhs:String = WdlExpression.toString(ast.getAttribute("lhs"))
        try {
            val wdlType = WdlNamespace.lookupType(cState.wf)(lhs)
            wdlType.isInstanceOf[WdlCallOutputsObjectType]
        } catch {
            case e:Throwable=> false
        }
    }

    // Check if the expression can be evaluated based on a set
    // of variables.
    private def dependsOnlyOnVars(expr: WdlExpression,
                                  definedVars: Set[String],
                                  cState: State) : Boolean = {
        val refVars = AstTools.findVariableReferences(expr.ast).map{
            case t:Terminal => WdlExpression.toString(t)
        }
        refVars.forall(x => definedVars contains x)
    }

    // Start from a split of the workflow elements into top and bottom blocks.
    // Move any declaration, whose dependencies are satisfied, to the top.
    //
    private def declMoveUp(drs: DeclReorgState, cState: State) : DeclReorgState = {
        var definedVars : Set[String] = drs.definedVars
        var moved = Vector.empty[Scope]

        // Move up declarations
        var bottom : Vector[Scope] = drs.bottom.map {
            case decl: Declaration =>
                decl.expression match {
                    case None =>
                        // An input parameter, move to top
                        definedVars = definedVars + decl.unqualifiedName
                        moved = moved :+ decl
                        None
                    case Some(expr) if dependsOnlyOnVars(expr, definedVars, cState)  =>
                        // Move the declaration to the top
                        definedVars = definedVars + decl.unqualifiedName
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
                        // Some statements define new variables, for example, calls
                        //
                        // TODO: what happens in case of scatters? Are we catching
                        // all of defined variables?
                        val crntDefs:Set[String] = x.taskCalls.map(_.unqualifiedName)
                        skipNonDecl(t :+ x, b.tail, defs ++ crntDefs)
                }
            }
        }
        val (nonDeclBlock, remaining, nonDeclVars) = skipNonDecl(Vector.empty, bottom, Set.empty)
        if (!nonDeclVars.isEmpty)
            Utils.trace(cState.verbose, s"Not moving definitions ${nonDeclVars}")
        //Utils.trace(cState.verbose, s"len(nonDecls)=${nonDeclBlock.length} len(rest)=${remaining.length}")
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
    def collectDeclarations(drsInit:DeclReorgState, cState: State) : Block = {
        var drs = drsInit
        var numIter = 0
        val totNumElems = drs.bottom.length
        while (!drs.bottom.isEmpty && numIter < MAX_NUM_COLLECT_ITER) {
/*            System.err.println(
                s"""|collectDeclaration ${numIter}
                    |size(defs)=${drs.definedVars.size} len(top)=${drs.top.length}
                    |len(bottom)=${drs.bottom.length}""".stripMargin.replaceAll("\n", " ")
            )*/
            drs = declMoveUp(drs, cState)
            assert(totNumElems == drs.top.length + drs.bottom.length)
            numIter += 1
        }
        Block(drs.top ++ drs.bottom, drs.definedVars)
    }

    // Attempt to collect declarations at the block level, to reduce
    // the number of extra jobs required for calculations.
    def reorgBlock(elems: Seq[Scope], definedVars: Set[String], cState: State) : Block  = {
        //Utils.trace(cState.verbose, "simplifying workflow top level")
        val drs = DeclReorgState(definedVars, Vector.empty[Scope], elems.toVector)
        collectDeclarations(drs, cState)
    }

    // Convert complex expressions to independent declarations
    def reorg(scope: Scope, definedVars: Set[String], cState:State): Block = {
        scope match {
            case ssc:Scatter =>
                val blk = reorgBlock(ssc.children, definedVars, cState)
                val ssc2 = WdlRewrite.scater(ssc, blk.children, ssc.collection)
                Block(Vector(ssc2), blk.definedVars)
            case cond:If =>
                val blk = reorgBlock(cond.children, definedVars, cState)
                val cond2 = WdlRewrite.cond(cond, blk.children, cond.condition)
                Block(Vector(cond2), blk.definedVars)
            case call:Call => Block(Vector(call), definedVars ++ call.fullyQualifiedName)
            case decl:Declaration => Block(Vector(decl), definedVars ++ decl.fullyQualifiedName)
            case wfo:WorkflowOutput => Block(Vector(wfo), definedVars)
            case x =>
                throw new Exception(cState.cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    def reorgWorkflow(wf: Workflow, cState: State) : Workflow = {
        val children: Vector[Scope] = wf.children
            .map(x => reorg(x, Set.empty, cState))
            .toVector
            .flatten
        val reChildren = reorgBlock(children, Set.empty, cState)
        WdlRewrite.workflow(wf, reChildren)
    }

    def apply(ns: WdlNamespace, verbose: Boolean) : WdlNamespace = {
        Utils.trace(verbose, "Reorganizing declarations")
        val cef = new CompilerErrorFormatter(ns.terminalMap)

        // Process the original WDL file,
        // Do not modify the tasks
        ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                val cState = State(nswf.workflow, cef, verbose)
                val wf1 = reorgWorkflow(nswf.workflow, cState)
                val nswf1 = new WdlNamespaceWithWorkflow(ns.importedAs,
                                                         wf1,
                                                         ns.imports,
                                                         ns.namespaces,
                                                         ns.tasks,
                                                         ns.terminalMap,
                                                         nswf.wdlSyntaxErrorFormatter,
                                                         ns.ast)
                nswf1.children = wf1.children
                nswf1.namespace = nswf.namespace
                nswf.parent match {
                    case Some(x) => nswf1.parent = x
                    case None => ()
                }
                nswf1
            case _ => ns
        }
    }
}
