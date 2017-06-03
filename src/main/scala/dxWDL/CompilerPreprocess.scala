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
import scala.util.{Failure, Success, Try}
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.{Call, Declaration, Scatter, Scope,
    Task, TaskCall, TaskOutput,
    WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow,
    Workflow, WorkflowCall, WdlSource}
import wdl4s.command.{ParameterCommandPart, StringCommandPart}
import wdl4s.expression._
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
            case tc: TaskCall => WdlRewrite.taskCall(tc, inputs)
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

    // Figure out the expression type for a collection we loop over in a scatter
    //
    // Expressions like A.B.C are converted to A_B_C, in order to avoid
    // the wdl4s library from interpreting these as member accesses.
    def collectionWdlType(scatter : Scatter, env : Map[String,WdlType]) : WdlType = {
        def transformVarName(x: String) : String = { x.replaceAll("\\.", "___") }
        def revTransformVarName(x: String) : String = {  x.replaceAll("___", "\\.") }

        def lookup(varName : String) : WdlType = {
            val orgMembAccess = revTransformVarName(varName)
            env.get(orgMembAccess) match {
                case Some(x) => x
                case None => throw new Exception(s"No type found for variable ${varName}")
            }
        }

        // convert all sub-expressions of the form A.B.C to A_B_C
        // Note: this will work only for simple expressions.
        val collection : WdlExpression = WdlExpression.fromString(
            transformVarName(scatter.collection.toWdlString))
        val collectionType : WdlType =
            collection.evaluateType(lookup, new WdlStandardLibraryFunctionsType) match {
                case Success(wdlType) => wdlType
                case _ => throw new Exception(s"Could not evaluate the WdlType for ${collection.toWdlString}")
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
            case t: Terminal =>
                val srcStr = t.getSourceString
                t.getTerminalStr match {
                    case "identifier" => true
                    case _ => true  // a constant (I think)
                }

            case a: Ast if a.isMemberAccess =>
                // This is a case of accessing something like A.B.C
                // The RHS is C, and the LHS is A.B
                true

            case a: Ast =>
                false
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
                        typeEnv : Map[String, WdlType],
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
            val collType : WdlType = collectionWdlType(ssc, typeEnv)
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
        var typeEnv = Map.empty[String, WdlType]
        val children: Vector[Scope] = wf.children.map {
            case ssc:Scatter =>
                // Be careful to add the indexing variable to the environment
                val sscDefVars = definedVars + ssc.item
                simplifyScatter(ssc, sscDefVars, typeEnv, cState)
            case call:Call => Vector(call)
            case decl:Declaration =>
                definedVars = definedVars + decl.unqualifiedName
                typeEnv = typeEnv + (decl.unqualifiedName -> decl.wdlType)
                Vector(decl)
            case x => throw new Exception(s"Unimplemented workflow element ${x.toString}")
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

    def dbgWorkflow(wf: Workflow, msg: String) = {
        System.err.println(s"--- ${msg} ----------------------")
        System.err.println(s"${wf.unqualifiedName} ${wf.calls} ${wf.children}")
        //val x = wf.namespace.resolveCallOrOutputOrDeclaration(outputFqn)
        //System.err.println(s"${x}")

        System.err.println(s"${wf.expandedWildcardOutputs}")
        val lines = WdlPrettyPrinter(true).apply(wf, 0).mkString("\n")
        System.err.println(lines)
        System.err.println("")
    }

    def simplifyWorkflow(wf: Workflow, cState:State) : Workflow = {
        //dbgWorkflow(wf, "ORG")
        val wf1 = simplifyAllScatters(wf, cState)
        //dbgWorkflow(wf1, "WF1")
        val wf2 = simplifyTopLevel(wf1, cState)
        //dbgWorkflow(wf2, "WF2")
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

        // Convert to string representation and apply WDL parser again.
        // This fixes the ASTs, as well as any other imperfections in our rewriting
        // technology.
        //
        // Note: by keeping the namespace in memory, instead of writing to
        // a temporary file on disk, we keep the resolver valid.
        val lines: String = WdlPrettyPrinter(true).apply(rewrittenNs, 0).mkString("\n")
        val cleanNs = WdlNamespace.loadUsingSource(lines, None, Some(List(resolver))).get

        if (verbose)
            writeToFile(wdlSourceFile, lines)
        cleanNs
    }
}
