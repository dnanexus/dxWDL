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

object CompilerSimplify {
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

    private def genTmpVarName() : String = {
        val tmpVarName: String = s"${Utils.TMP_VAR_NAME_PREFIX}${tmpVarCnt}"
        tmpVarCnt = tmpVarCnt + 1
        tmpVarName
    }

    // A member access expression such as [A.x]. Check if
    // A is a call.
    private def isCallOutputAccess(expr: WdlExpression, ast: Ast, call: Call, cState: State) : Boolean = {
        val lhs:String = WdlExpression.toString(ast.getAttribute("lhs"))
        try {
            val wdlType = WdlNamespace.lookupType(cState.wf)(lhs)
            wdlType.isInstanceOf[WdlCallOutputsObjectType]
        } catch {
            case e:Throwable=> false
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
    private def simplifyCall(call: Call, cState: State) : Vector[Scope] = {
        val tmpDecls = Queue[Scope]()
        val inputs: Map[String, WdlExpression]  = call.inputMappings.map { case (key, expr) =>
            val rhs = expr.ast match {
                case t: Terminal => expr
                case a: Ast if (a.isMemberAccess && isCallOutputAccess(expr, a, call, cState)) =>
                    // Accessing an expression like A.B.C
                    // The expression could be:
                    // 1) Result from a previous call
                    // 2) Access to a field in a pair, or an object (pair.left, obj.name)
                    // Only the first case can be handled inline, the other requires
                    // a temporary variable
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
            case tc: TaskCall => WdlRewrite.taskCall(tc, inputs)
            case wfc: WorkflowCall => throw new Exception(s"Unimplemented WorkflowCall")
        }
        tmpDecls += callModifiedInputs
        tmpDecls.toVector
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

    // Figure out the expression type for a collection we loop over in a scatter
    def collectionWdlType(ssc : Scatter, cState: State) : WdlType = {
        val collectionType : WdlType =
            ssc.collection.evaluateType(WdlNamespace.lookupType(ssc),
                                        new WdlStandardLibraryFunctionsType,
                                        Some(ssc)) match {
                case Success(wdlType) => wdlType
//                case _ => throw new Exception(cState.cef.couldNotEvaluateType(ssc.collection.ast))
                case _ => throw new Exception(s"could not evaluate scatter type for ${ssc.collection.toWdlString}")
            }
        collectionType
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

    def isSimpleExpression(expr: WdlExpression) : Boolean = {
        expr.ast match {
            case t: Terminal if t.getTerminalStr == "identifier" => true
            case _ => false
        }
    }

    // - Move the collection expression to a separate declaration
    // - Move expressions in calls into separate declarations.
    // - As much as possible, move declarations to the the top of the block
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
    def simplifyScatter(ssc: Scatter,
                        definedVars: Set[String],
                        cState: State) : Vector[Scope] = {
        // extract expressions from calls
        val children : Vector[Scope] = ssc.children.map {
            case call: Call => simplifyCall(call, cState)
            case x => Vector(x)
        }.flatten.toVector

        // Try to pull declarations to the top of the block, this allows calculating
        // them with the scatter job we need to run anyway.
        val drs = DeclReorgState(definedVars, Vector.empty[Scope], children)
        val reorgChildren = collectDeclarations(drs, cState)

        if (isSimpleExpression(ssc.collection)) {
            // The collection is a simple variable, there is no need
            // to create an additional declaration
            val ssc1 = WdlRewrite.scatter(ssc, reorgChildren)
            Vector(ssc1)
        } else {
            // separate declaration for collection expression
            val collType : WdlType = collectionWdlType(ssc, cState)
            val colDecl = WdlRewrite.newDeclaration(collType,
                                                    genTmpVarName(),
                                                    Some(ssc.collection))
            val collVar = WdlExpression.fromString(colDecl.unqualifiedName)
            val ssc1 = WdlRewrite.scatter(ssc, reorgChildren, collVar)
            Vector(colDecl, ssc1)
        }
    }

    // Simplify scatter blocks inside the workflow. Return a valid new
    // WDL workflow.
    //
    // Note: we keep track of the defined variables, because this is required for
    // moving declarations inside the scatter sub blocks.
    def simplifyAllScatters(wf:Workflow, cState:State): Workflow = {
        Utils.trace(cState.verbose, "simplifying scatters")
        var definedVars = Set.empty[String]
        val children: Vector[Scope] = wf.children.map {
            case ssc:Scatter =>
                // Be careful to add the indexing variable to the environment
                val sscDefVars = definedVars + ssc.item
                simplifyScatter(ssc, sscDefVars, cState)
            case decl:Declaration =>
                definedVars = definedVars + decl.unqualifiedName
                Vector(decl)
            case wfo:WorkflowOutput => Vector(wfo)
            case call:Call => Vector(call)
            case x =>
                throw new Exception(cState.cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }.flatten.toVector
        WdlRewrite.workflow(wf, children)
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
        WdlRewrite.workflow(wf, reorgElems)
    }

    def simplifyWorkflow(ns: WdlNamespace, wf: Workflow, cState: State) : Workflow = {
        val wf1 = simplifyAllScatters(wf, cState)
        val wf2 = simplifyTopLevel(wf1, cState)
        wf2
    }


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

    def apply(ns: WdlNamespace, verbose: Boolean) : WdlNamespace = {
        Utils.trace(verbose, "Preprocessing pass")
        val cef = new CompilerErrorFormatter(ns.terminalMap)
        checkReservedWords(ns, cef)

        // Process the original WDL file,
        // Do not modify the tasks
        ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                val cState = State(nswf.workflow, cef, verbose)
                val wf1 = simplifyWorkflow(ns, nswf.workflow, cState)
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
