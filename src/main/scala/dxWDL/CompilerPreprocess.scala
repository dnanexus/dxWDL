/**
  *  Preprocessing pass, simplifies the original WDL.
  *
  *  Instead of handling expressions in calls directly,
  *  we lift the expressions, generate auxiliary variables, and
  *  call the task with values or variables (no expressions).
  */
package dxWDL

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Path, Paths}
import scala.collection.mutable.Queue
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.{Call, Declaration, Scatter, Scope, Task, TaskCall, WdlExpression, WdlNamespace,
    WdlNamespaceWithWorkflow, Workflow, WorkflowCall, WdlSource}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions

object CompilerPreprocess {
    val MAX_NUM_COLLECT_ITER = 10

    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(cef: CompilerErrorFormatter,
                     terminalMap: Map[Terminal, WdlSource],
                     verbose: Boolean)

    case class DeclReorgState(definedVars: Set[String],
                              top: Vector[Scope],
                              bottom: Vector[Scope])

    // Add a suffix to a filename, before the regular suffix. For example:
    //  xxx.wdl -> xxx.simplified.wdl
    def createTargetFileName(src: Path) : String = {
        val secondSuffix = ".simplified"
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

    // Get the source lines of an AST structure.
    //
    // This is done by find all the terminals in the task AST, and
    // mapping to the source WDL lines. It is somewhat of a hack,
    // because the first and last lines source lines could,
    // potentially, contain additional code beyond the AST we want.
    def getAstSourceLines(ast: Ast, cState: State) : String = {
        val allTerminals : Seq[Terminal] = AstTools.findTerminals(ast)
        // This doesn't work, because the all the brackets have been removed from
        // the terminal list
        //val buf = allTerminals.map(x => x.getSourceString()).mkString(" ")

        val firstLine = allTerminals.head.getLine -1
        val lastLine = allTerminals.last.getLine

        val lines : Array[String] = cState.terminalMap.get(allTerminals.head).get.split("\n")
        val (_, startPlus) = lines.splitAt(firstLine)
        val (middle,_) = startPlus.splitAt(lastLine - firstLine)
        middle.mkString("\n").trim
    }

    // Print a task, without modification, to the output. This is
    // done by find all the terminals in the task AST, and mapping
    // to the source WDL lines. It is somewhat of a hack, because
    // the first and last lines source lines could, potentially, contain additional
    // code beyond the task itself.
    def getTaskLines(task: Task, cState: State) : String = {
        val buf = getAstSourceLines(task.ast, cState)

        // Add a closing bracket(s) at the end, if needed.
        //
        // We are normally missing the closing bracket for the output section and the task.
        // TODO: It would be great to find a better way around this!
        if (buf.endsWith("}"))
            buf ++ "\n" ++ "}"
        else
            buf ++ "\n" ++ "}}"
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
    def simplifyCall(call: Call, tmpVarCnt: Int, cState: State) : (Int, Seq[Scope]) = {
        var varNum = tmpVarCnt + 1
        val tmpDecls = Queue[Scope]()
        val inputs: Map[String, WdlExpression]  = call.inputMappings.map { case (key, expr) =>
            val rhs = expr.ast match {
                case t: Terminal => expr
                case a: Ast if a.isMemberAccess =>
                    // Accessing something like A.B.C
                    expr
                case a: Ast if a.isFunctionCall || a.isUnaryOperator || a.isBinaryOperator
                      || a.isTupleLiteral || a.isArrayOrMapLookup =>
                    // replace an expression with a temporary variable
                    val tmpVarName: String = s"xtmp${varNum}"
                    val calleeDecl: Declaration =
                        call.declarations.find(decl => decl.unqualifiedName == key).get
                    val wdlType = calleeDecl.wdlType
                    varNum = varNum + 1
                    tmpDecls += Declaration(wdlType, tmpVarName, Some(expr), call.parent, a)
                    WdlExpression.fromString(tmpVarName)
            }
            (key -> rhs)
        }

        val callModifiedInputs = call match {
            case tc: TaskCall => TaskCall(tc.alias, tc.task, inputs, tc.ast)
            case wfc: WorkflowCall => WorkflowCall(wfc.alias, wfc.calledWorkflow, inputs, wfc.ast)
        }
        tmpDecls += callModifiedInputs
        (varNum, tmpDecls.toList)
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
                    System.err.println(s"skipNonDecl, reached declaration ${prettyPrint(decl, 4, cState)}")
                    (t,b)
                case x => skipNonDecl(t :+ x, b.tail)
            }
        }
        val (nonDeclBlock, remaining) = skipNonDecl(Vector.empty, bottom)
        System.err.println(s"len(nonDecls)=${nonDeclBlock.length} len(rest)=${remaining.length}")
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
    def collectDeclarations(elems: Seq[Scope], cState: State) : Seq[Scope] = {
        var numIter = 0
        var drs = DeclReorgState(Set.empty[String],
                                 Vector.empty[Scope],
                                 elems.toVector)
        while (!drs.bottom.isEmpty && numIter < MAX_NUM_COLLECT_ITER) {
            System.err.println(
                s"""|collectDeclaration ${numIter}
                    |size(defs)=${drs.definedVars.size} len(top)=${drs.top.length}
                    |len(bottom)=${drs.bottom.length}""".stripMargin.replaceAll("\n", " ")
            )
            drs = declMoveUp(drs, cState)
            numIter += 1
        }
        drs.top ++ drs.bottom
    }


    // Create an indentation of [n] spaces
    def genNSpaces(n: Int) = {
        s"${" " * n}"
    }

    // convert everything to valid textual WDL
    def prettyPrint(call: Call, indent: Int, cState: State) : String = {
        val aliasStr = call.alias match {
            case None => ""
            case Some(nm) => " as " ++ nm
        }
        val inputs: Seq[String] = call.inputMappings.map { case (key, expr) =>
            val rhs = expr.ast match {
                case t: Terminal => t.getSourceString
                case a: Ast => WdlExpression.toString(a)
            }
            s"${key}=${rhs}"
        }.toList
        val inputsConcat = inputs.mkString(", ")
        val spaces = genNSpaces(indent)
        s"""|${spaces}call ${call.unqualifiedName} ${aliasStr} {
            |${spaces}  input:
            |${spaces}${spaces}${inputsConcat}
            |${spaces}}""".stripMargin
    }

    def prettyPrint(decl: Declaration, indent: Int, cState: State) : String = {
        val exprStr = decl.expression match {
            case None => ""
            case Some(x) => " = " ++ x.toWdlString
        }
        s"""|${genNSpaces(indent)}
            |${decl.wdlType.toWdlString} ${decl.unqualifiedName}
            |${exprStr}""".stripMargin.replaceAll("\n","")
    }

    def prettyPrint(ssc: Scatter, indent: Int, cState: State) : String = {
        getAstSourceLines(ssc.ast, cState)
    }

    def simplifyWorkflow(wf: Workflow, indent: Int, cState: State) : String = {
        // simplification step
        var tmpVarCnt = 0
        var elems : Seq[Scope] = wf.children.map {
            case call: Call =>
                val (nCnt, callElems) = simplifyCall(call, tmpVarCnt, cState)
                tmpVarCnt = nCnt
                callElems
            case x => List(x)
        }.flatten

        // Attempt to collect declarations, to reduce the number of extra jobs
        // required for calculations.
        elems = collectDeclarations(elems, cState)

        // pretty print the workflow to a file. The output
        // must be readable by the standard WDL compiler.
        val elemsStr : Seq[String] = elems.map {
            case call: Call => prettyPrint(call, 4, cState)
            case decl: Declaration => prettyPrint(decl, 4, cState)
            case ssc: Scatter => prettyPrint(ssc, 4, cState)
            case x =>
                throw new Exception(cState.cef.notCurrentlySupported(x.ast,
                                                                     "workflow element"))
        }
        val topLine = s"workflow ${wf.unqualifiedName} {"
        val endLine = "}"
        val wfLines = List(topLine) ++ elemsStr ++ List(endLine)
        wfLines.mkString("\n")
    }

    def apply(wdlSourceFile : Path,
              verbose: Boolean) : Path = {
        Utils.trace(verbose, "Preprocessing")

        val ns = WdlNamespaceWithWorkflow.load(
            Utils.readFileContent(wdlSourceFile),
            Seq.empty).get
        val tm = ns.wdlSyntaxErrorFormatter.terminalMap
        val cef = new CompilerErrorFormatter(tm)
        val cState = State(cef, tm, verbose)

        // Create a new file to hold the result.
        //
        // Assuming the source file is xxx.wdl, the new name will
        // be xxx.simplified.wdl.
        val trgName: String = createTargetFileName(wdlSourceFile)
        val simplWdl = Utils.appCompileDirPath.resolve(trgName).toFile
        val fos = new FileWriter(simplWdl)
        val pw = new PrintWriter(fos)

        // Process the original WDL file, write the output to
        // xxx.simplified.wdl
        // Do not modify the tasks
        ns.tasks.foreach{ task =>
            val taskLines = getTaskLines(task, cState)
            pw.println(taskLines + "\n")
        }
        ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                val rewritten: String = simplifyWorkflow(nswf.workflow, 4, cState)
                pw.println(rewritten)
            case _ => ()
        }

        pw.flush()
        pw.close()
        simplWdl.toPath
        //wdlSourceFile
    }
}
