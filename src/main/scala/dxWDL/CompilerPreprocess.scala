/**
  *  Preprocessing pass, simplify the original WDL.
  *
  *  Instead of handling expressions in calls directly,
  *  we lift the expressions, generate auxiliary variables, and
  *  call the task with values or variables (no expressions).
  *
  *  A difficulty we face here, is avoiding using internal
  *  representations used by wdl4s. For example, we want to reorganize
  *  scatter blocks, however, we cannot create valid new wdl4s scatter
  *  blocks. Instead, we pretty print a new workflow and then load it.
  */
package dxWDL

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Path, Paths}
import scala.collection.mutable.Queue
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.{Call, Declaration, Scatter, Scope,
    Task, TaskCall, TaskOutput,
    WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow,
    Workflow, WorkflowCall, WdlSource}
import wdl4s.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions

object CompilerPreprocess {
    val MAX_NUM_COLLECT_ITER = 10
    var tmpVarCnt = 0

    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(cef: CompilerErrorFormatter,
                     terminalMap: Map[Terminal, WdlSource],
                     verbose: Boolean)

    case class DeclReorgState(definedVars: Set[String],
                              top: Vector[Scope],
                              bottom: Vector[Scope])

    def genTmpVarName() : String = {
        val tmpVarName: String = s"xtmp${tmpVarCnt}"
        tmpVarCnt = tmpVarCnt + 1
        tmpVarName
    }

    // Add a suffix to a filename, before the regular suffix. For example:
    //  xxx.wdl -> xxx.simplified.wdl
    def addFilenameSuffix(src: Path, secondSuffix: String) : String = {
        val fName = src.toFile().getName()
        val index = fName.lastIndexOf('.')
        if (index == -1) {
            fName + secondSuffix
        }
        else {
            val prefix = fName.substring(0, index)
            val suffix = fName.substring(index)
            prefix + secondSuffix + suffix
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
    def simplifyCall(call: Call, cState: State) : Vector[Scope] = {
        val tmpDecls = Queue[Scope]()
        val inputs: Map[String, WdlExpression]  = call.inputMappings.map { case (key, expr) =>
            val rhs = expr.ast match {
                case t: Terminal => expr
                case a: Ast if a.isMemberAccess =>
                    // Accessing something like A.B.C
                    expr
                case a: Ast =>
                    // replace an expression with a temporary variable
                    val tmpVarName = genTmpVarName()
                    val calleeDecl: Declaration =
                        call.declarations.find(decl => decl.unqualifiedName == key).get
                    val wdlType = calleeDecl.wdlType
                    tmpDecls += Declaration(wdlType, tmpVarName, Some(expr), call.parent, a)
                    WdlExpression.fromString(tmpVarName)
            }
            (key -> rhs)
        }

        val callModifiedInputs = call match {
            case tc: TaskCall =>
                val tc1 = TaskCall(tc.alias, tc.task, inputs, tc.ast)
                tc1.namespace = tc.namespace
                tc1.children = tc.children
                tc.parent match {
                    case Some(x) => tc1.parent = x
                    case None => ()
                }
                tc1
            case wfc: WorkflowCall => throw new Exception(s"Unimplemented WorkflowCall")
        }
        tmpDecls += callModifiedInputs
        tmpDecls.toVector
    }

    // Start from a split of the workflow elements into top and bottom blocks.
    // Move any declaration, whose dependencies are satisfied, to the top.
    //
    def declMoveUp(drs: DeclReorgState, cState: State) : DeclReorgState = {
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
                    case Some(expr) =>
                        val (possible, deps) = Utils.findToplevelVarDeps(expr)
                        if (!possible) {
                            Some(decl)
                        } else if (!deps.forall(x => definedVars(x))) {
                            // some dependency is missing, can't move up
                            Some(decl)
                        } else {
                            // Move the declaration to the top
                            definedVars = definedVars + decl.unqualifiedName
                            moved = moved :+ decl
                            None
                        }
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
        def skipNonDecl(t: Vector[Scope], b: Vector[Scope]) :
                (Vector[Scope], Vector[Scope]) = {
            if (b.isEmpty) {
                (t,b)
            }
            else b.head match {
                case decl: Declaration =>
                    // end of skip section
                    (t,b)
                case x => skipNonDecl(t :+ x, b.tail)
            }
        }
        val (nonDeclBlock, remaining) = skipNonDecl(Vector.empty, bottom)
        Utils.trace(cState.verbose, s"len(nonDecls)=${nonDeclBlock.length} len(rest)=${remaining.length}")
        DeclReorgState(definedVars, drs.top ++ moved ++ nonDeclBlock, remaining)
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
    def collectDeclarations(drsInit:DeclReorgState,
                            cState: State) : Seq[Scope] = {
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
        drs.top ++ drs.bottom
    }

    // 1. Move sub-expressions in a scatter block into separate declarations.
    // 2. Collect the sub-expressions, as much as possible.
    def simplifyScatter(ssc: Scatter, definedVars: Set[String], cState: State) : Scatter = {
        // extract expressions from calls
        val children : Vector[Scope] = ssc.children.map {
            case call: Call => simplifyCall(call, cState)
            case x => Vector(x)
        }.flatten.toVector

        // Try to pull as declarations to the top of the block, this allows calculating
        // them with the scatter job we need to run anyway.
        val drs = DeclReorgState(definedVars, Vector.empty[Scope], children)
        val reorgChildren = collectDeclarations(drs, cState)

        // Build a new scatter structure. We are cheating on the AST; since we
        // don't know how to create a new one, we just keep the old AST.
        val scatter = Scatter(ssc.index, ssc.item, ssc.collection, ssc.ast)
        scatter.children = reorgChildren
        scatter
    }

    // Simplify scatter blocks inside the workflow. Return a valid new
    // WDL workflow.
    //
    // Note: we keep track of the defined variables, because this is required for
    // moving declarations inside the scatter sub blocks.
    def simplifyAllScatters(wf:Workflow, cState:State): Workflow = {
        Utils.trace(cState.verbose, "simplifying scatters")
        var definedVars:Set[String] = Set.empty
        val children: Vector[Scope] = wf.children.map {
            case ssc:Scatter =>
                // Be careful to add the indexing variable to the environment
                val sscDefVars = definedVars + ssc.item
                simplifyScatter(ssc, sscDefVars, cState)
            case call:Call => call
            case decl:Declaration =>
                definedVars = definedVars + decl.unqualifiedName
                decl
            case x => throw new Exception(s"Unimplemented workflow element ${x.toString}")
        }.toVector

        val smpWf = new Workflow(wf.unqualifiedName, wf.workflowOutputWildcards,
                                 wf.wdlSyntaxErrorFormatter, wf.meta, wf.parameterMeta,
                                 wf.ast)
        smpWf.children = children
        smpWf.namespace = wf.namespace
        smpWf
    }

    // Simplify the declarations at the top level of the workflow
    def simplifyTopLevel(wf: Workflow, cState: State) : Workflow = {
        Utils.trace(cState.verbose, "simplifying workflow top level")

        // simplification step
        val elems : Seq[Scope] = wf.children.map {
            case call: Call => simplifyCall(call, cState)
            case x => List(x)
        }.flatten

        // Attempt to collect declarations at the top level, to reduce the number of extra jobs
        // required for calculations.
        val drs = DeclReorgState(Set.empty[String], Vector.empty[Scope], elems.toVector)
        val reorgElems = collectDeclarations(drs, cState)

        val smpWf = new Workflow(wf.unqualifiedName, wf.workflowOutputWildcards,
                                 wf.wdlSyntaxErrorFormatter, wf.meta, wf.parameterMeta,
                                 wf.ast)
        smpWf.children = reorgElems
        smpWf.namespace = wf.namespace
        smpWf
    }

    def simplifyWorkflow(wf: Workflow, cState:State) : Workflow = {
        val wf1 = simplifyAllScatters(wf, cState)
        val wf2 = simplifyTopLevel(wf, cState)
        wf2
    }


    // Assuming the source file is xxx.wdl, the new name will
    // be xxx.simplified.wdl.
    def writeToFile(wdlSourceFile : Path, lines: String) : Unit = {
        val trgName: String = addFilenameSuffix(wdlSourceFile, ".simplified")
        val simpleWdl = Utils.appCompileDirPath.resolve(trgName).toFile
        val fos = new FileWriter(simpleWdl)
        val pw = new PrintWriter(fos)
        pw.println(lines)
        pw.flush()
        pw.close()
        System.err.println(s"Wrote simplified WDL to ${simpleWdl.toString}")
    }

    def apply(wdlSourceFile : Path,
              verbose: Boolean) : WdlNamespace = {
        Utils.trace(verbose, "Preprocessing pass")

        // Resolving imports. Look for referenced files in the
        // source directory.
        val sourceDir = wdlSourceFile.getParent()
        def resolver(filename: String) : WdlSource = {
            Utils.readFileContent(sourceDir.resolve(filename))
        }
        val ns = WdlNamespace.loadUsingPath(wdlSourceFile, None, Some(List(resolver))).get
        val tm = ns.terminalMap
        val cef = new CompilerErrorFormatter(tm)
        val cState = State(cef, tm, verbose)

        // Process the original WDL file,
        // Do not modify the tasks
        val rewrittenNs = ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                val wf1 = simplifyWorkflow(nswf.workflow, cState)
                new WdlNamespaceWithWorkflow(ns.importedAs,
                                             wf1,
                                             ns.imports,
                                             ns.namespaces,
                                             ns.tasks,
                                             ns.terminalMap,
                                             nswf.wdlSyntaxErrorFormatter,
                                             ns.ast)
            case _ => ns
        }

        // Convert to string representation and apply WDL parser again.
        // This fixes the ASTs, as well as any other imperfections in our rewriting
        // technology.
        //
        // Note: by keeping the namespace in memory, instead of writing to
        // a temporary file on disk, we can keep resolver.
        val lines: String = WdlPrettyPrinter.apply(rewrittenNs, 0).mkString("\n")
        val cleanNs = WdlNamespace.loadUsingSource(lines, None, Some(List(resolver))).get

        if (verbose)
            writeToFile(wdlSourceFile, lines)
        cleanNs
    }
}
