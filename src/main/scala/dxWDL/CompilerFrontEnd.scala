/** Compile a WDL Workflow into a dx:workflow
  *
  * The front end takes a WDL workflow, and generates an intermediate
  * representation from it.
  */
package dxWDL

import java.nio.file.{Files, Paths, Path}
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import scala.util.{Failure, Success, Try}
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s._
import wdl4s.expression._
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions

object CompilerFrontEnd {
    class DynamicInstanceTypesException private(ex: Exception) extends RuntimeException(ex) {
        def this() = this(new RuntimeException(
                              "Only runtime constants are supported, currently, instance types should be known at compile time."))
    }

    // Linking between a variable, and which stage we got
    // it from.
    case class LinkedVar(cVar: IR.CVar, sArg: IR.SArg) {
        def yaml : YamlObject = {
            YamlObject(
                YamlString("cVar") -> IR.yaml(cVar),
                YamlString("sArg") -> IR.yaml(sArg)
            )
        }
    }

    // Environment (scope) where a call is made
    type CallEnv = Map[String, LinkedVar]

    // The minimal environment needed to execute a call
    type Closure =  CallEnv

    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(destination: String,
                     cef: CompilerErrorFormatter,
                     verbose: Boolean)

    // Convert the environment to yaml, and then pretty
    // print it.
    def prettyPrint(env: CallEnv) : String = {
        val m : Map[YamlValue, YamlValue] = env.map{ case (key, lVar) =>
            YamlString(key) -> lVar.yaml
        }.toMap
        YamlObject(m).print()
    }

    // Lookup for variables like x, A.x, A.B.x. The difficulty
    // is that A.x is interpreted in WDL as member access.
    def lookupInEnv(env: CallEnv, expr: WdlExpression, cState: State) : LinkedVar = {
        val fqn: String = expr.toWdlString
        env.get(fqn) match {
            case Some(x) => x
            case None =>
                val t: Terminal = AstTools.findTerminals(expr.ast).head
                throw new Exception(cState.cef.missingVarRefException(t))
        }
    }

    // Create a declaration.
    //
    // The difficulty here is in generating an AST.
    def declarationGen(wdlType: WdlType,
                       name: String,
                       expr: Option[WdlExpression]) : Declaration = {
        val textualRepr = expr match {
            case None => s"${wdlType.toWdlString} ${name}"
            case Some(e) => s"${wdlType.toWdlString} ${name} = ${e.toWdlString}"
        }
        val ast: Ast = AstTools.getAst(textualRepr, "")
        Declaration(wdlType, name, expr, None, ast)
    }

    def genDefaultValueOfType(wdlType: WdlType) : WdlValue = {
        /*def primitive(t: WdlType) : WdlValue = {
            case WdlBooleanType => WdlBoolean(true)
            case WdlIntegerType => WdlInteger(0)
            case WdlFloatType => WdlFloat(0.0)
            case WdlStringType => WdlString("")
            case WdlFileType => WdlFile("/tmp/X.txt")
            case _ => throw new Exception(s"Non primitive value ${t.toWdlString}")
        }*/
        wdlType match {
            case WdlArrayType(x) => WdlArray(WdlArrayType(x), List())  // an empty array
            case WdlBooleanType => WdlBoolean(true)
            case WdlIntegerType => WdlInteger(0)
            case WdlFloatType => WdlFloat(0.0)
            case WdlStringType => WdlString("")
            case WdlFileType => WdlFile("/tmp/X.txt")
            case _ => throw new Exception(s"Unhandled type ${wdlType.toWdlString}")
        }
    }

    /** Create a stub for an applet. This is an empty task
      that includes the input and output definitions. It is used
      to allow linking to externally defined tasks and applets.

      For example, the stub for the Add task:
task Add {
    Int a
    Int b
    command { }
    output {
        Int result = a + b
    }
}

      is:

task Add {
    Int a
    Int b

    output {
        Int result
    }
*/
    def genAppletStub(applet: IR.Applet) : Task = {
        // write a Task as human readable text
        val inputs = applet.inputs.map(cVar => s"${cVar.wdlType.toWdlString} ${cVar.name}")
        val outputs = applet.outputs.map{ cVar =>
            val defaultVal: WdlValue = genDefaultValueOfType(cVar.wdlType)
            s"${cVar.wdlType.toWdlString} ${cVar.name} = ${defaultVal.toWdlString}"
        }

        val body = inputs.map(line => WdlPrettyPrinter.indentLine(line, 1)) ++
            WdlPrettyPrinter.buildBlock("command", Vector(""), 1) ++
            WdlPrettyPrinter.buildBlock("output", outputs, 1)
        val wdlCode = WdlPrettyPrinter.buildBlock(s"task ${applet.name}", body, 0).mkString("\n")

        // Convert to a Task structure
        val ns = WdlNamespace.loadUsingSource(wdlCode, None, None) match {
            case Success(x) => x
            case Failure(f) =>
                System.err.println(s"Did not generate legal WDL code for applet ${applet.name}")
                System.err.println(wdlCode)
                throw f
        }
        ns.tasks.head
    }

    // Rename member accesses inside an expression, from
    // the form A.x to A_x. This is used inside an applet WDL generated code.
    //
    // Here, we take a shortcut, and just replace strings, instead of
    // doing a recursive syntax analysis (see ValueEvaluator wdl4s
    // module).
    def exprRenameVars(expr: WdlExpression,
                       allVars: Vector[IR.CVar]) : WdlExpression = {
        var sExpr: String = expr.toWdlString
        for (cVar <- allVars) {
            // A.x => A_x
            sExpr = sExpr.replaceAll(cVar.name, cVar.dxVarName)
        }
        WdlExpression.fromString(sExpr)
    }

    // Figure out the expression type for a collection we loop over in a scatter
    //
    // Expressions like A.B.C are converted to A_B_C, in order to avoid
    // the wdl4s library from interpreting these as member accesses.
    def calcIterWdlType(scatter : Scatter, env : Map[String,WdlType]) : WdlType = {
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

        // remove the array qualifier
        collectionType match {
            case WdlArrayType(x) => x
            case _ => throw new Exception(s"type ${collectionType} is not an array")
        }
    }

    def callUniqueName(call : Call, cState: State) = {
        val nm = call.alias match {
            case Some(x) => x
            case None => Utils.taskOfCall(call).name
        }
        Utils.reservedAppletPrefixes.foreach{ prefix =>
            if (nm.startsWith(prefix))
                throw new Exception(cState.cef.illegalCallName(call))
        }
        Utils.reservedSubstrings.foreach{ sb =>
            if (nm contains sb)
                throw new Exception(cState.cef.illegalCallName(call))
        }
        nm
    }

    // Figure out which instance to use.
    //
    //   Extract three fields from the task:
    // RAM, disk space, and number of cores. These are WDL expressions
    // that, in the general case, could be calculated only at runtime.
    // Currently, we support only constants. If a runtime expression is used,
    // we convert it to a moderatly high constant.
    def calcInstanceType(taskOpt: Option[Task]) : String = {
        def lookup(varName : String) : WdlValue = {
            throw new DynamicInstanceTypesException()
        }
        def evalAttr(task: Task, attrName: String, defaultValue: WdlValue) : Option[WdlValue] = {
            task.runtimeAttributes.attrs.get(attrName) match {
                case None => None
                case Some(expr) =>
                    try {
                        Some(expr.evaluate(lookup, NoFunctions).get)
                    } catch {
                        case e : DynamicInstanceTypesException =>
                            val v = Some(defaultValue)
                            System.err.println(
                                s"""|Warning: runtime expressions are used in
                                    |task ${task.name}, but are currently not supported.
                                    |Default ${defaultValue} substituted for
                                    |attribute ${attrName}."""
                                    .stripMargin.trim)
                            Some(defaultValue)
                    }
            }
        }

        taskOpt match {
            case None =>
                // A utility calculation, that requires minimal computing resources.
                // For example, the top level of a scatter block. We use
                // the default instance type, because one will probably be available,
                // and it will probably be inexpensive.
                InstanceTypes.getMinimalInstanceType()
            case Some(task) =>
                val memory = evalAttr(task, "memory", WdlString("60 GB"))
                val diskSpace = evalAttr(task, "disks", WdlString("local-disk 400 HDD"))
                val cores = evalAttr(task, "cpu", WdlInteger(8))
                InstanceTypes.apply(memory, diskSpace, cores)
        }
    }

    // split a block of statements into sub-blocks with continguous declarations.
    // Example:
    //
    //   call x
    //   Int a
    //   Int b
    //   call y
    //   call z
    //   String s
    //   call w
    //  =>
    //   Singleton(call x),
    //   DeclsBlock(Int a, Int b),
    //   Singleton(call y)
    //   Singleton(call z)
    //   DeclsBlock(String s)
    //   Singleton(call w)
    sealed trait Block
    case class DeclBlock(decls: Vector[Declaration]) extends Block
    case class Singleton(scope: Scope) extends Block
    def splitBlock(children: Seq[Scope]) : Vector[Block] = {
        val initAccu = (Vector[Block](), Vector[Declaration]())
        val (subBlocks, decls) = children.foldLeft(initAccu) { (accu, child) =>
            val (subBlocks, decls) = accu
            child match {
                case decl: Declaration =>
                    (subBlocks, decls :+ decl)
                case scope =>
                    if (decls.isEmpty)
                        (subBlocks :+ Singleton(scope),  Vector[Declaration]())
                    else
                        (subBlocks :+ DeclBlock(decls) :+ Singleton(scope), Vector[Declaration]())
            }
        }
        if (decls.isEmpty)
            subBlocks
        else
            subBlocks :+ DeclBlock(decls)
    }

    // Update a closure with all the variables required
    // for an expression.
    //
    // @param  closure   call closure
    // @param  env       mapping from fully qualified WDL name to a dxlink
    // @param  expr      expression as it appears in source WDL
    def updateClosure(closure : Closure,
                      env : CallEnv,
                      expr : WdlExpression,
                      strict : Boolean,
                      cState: State) : Closure = {
        expr.ast match {
            case t: Terminal =>
                val srcStr = t.getSourceString
                t.getTerminalStr match {
                    case "identifier" =>
                        val lVar = env.get(srcStr) match {
                            case Some(x) => x
                            case None => throw new Exception(cState.cef.missingVarRefException(t))
                        }
                        val fqn = WdlExpression.toString(t)
                        closure + (fqn -> lVar)
                    case _ => closure
                }

            case a: Ast if a.isMemberAccess =>
                // This is a case of accessing something like A.B.C
                // The RHS is C, and the LHS is A.B
                val rhs : Terminal = a.getAttribute("rhs") match {
                    case rhs:Terminal if rhs.getTerminalStr == "identifier" => rhs
                    case _ => throw new Exception(cState.cef.rightSideMustBeIdentifer(a))
                }

                // The FQN is "A.B.C"
                val fqn = WdlExpression.toString(a)
                env.get(fqn) match {
                    case Some(lVar) =>
                        closure + (fqn -> lVar)
                    case None =>
                        if (strict) {
                            System.err.println(s"Cannot find ${fqn} in env=\n${prettyPrint(env)}")
                            throw new Exception(cState.cef.undefinedMemberAccess(a))
                        }
                        closure
                }
            case a: Ast if a.isFunctionCall || a.isUnaryOperator || a.isBinaryOperator
                  || a.isTupleLiteral || a.isArrayOrMapLookup =>
                // Figure out which variables are needed to calculate this expression,
                // and add bindings for them
                val memberAccesses = a.findTopLevelMemberAccesses().map(
                    // This is an expression like A.B.C
                    varRef => WdlExpression.toString(varRef)
                )
                val variables = AstTools.findVariableReferences(a).map{ case t:Terminal =>
                    WdlExpression.toString(t)
                }
                val allDeps = (memberAccesses ++ variables).map{ fqn =>
                    env.get(fqn) match {
                        case Some(lVar) => Some(fqn -> lVar)

                        // There are cases where
                        // [findVariableReferences] gives us previous
                        // call names. We still don't know how to get rid of those cases.
                        case None => None
                        //case None => throw new Exception(s"Variable ${fqn} is not found in the scope expr=${expr.toWdlString}")
                    }
                }.flatten
                closure ++ allDeps

            case a: Ast =>
                throw new Exception(cState.cef.notCurrentlySupported(
                                        a,
                                        s"The expression ${expr.toString} is currently not handled"))
        }
    }

    // Make sure that the WDL code we generate is actually legal.
    def verifyWdlCodeIsLegal(wdlCode: String) : Unit = {
        val ns = WdlNamespace.loadUsingSource(wdlCode, None, None)
        ns match {
            case Success(_) => ()
            case Failure(f) =>
                System.err.println("Error verifying generated WDL code")
                System.err.println(wdlCode)
                throw f
        }
    }

    // Print a WDL workflow that evaluates expressions
    //
    // Note: we do not generate outputs, the applet deals with this issue.
    def genEvalWorkflowFromDeclarations(name: String,
                                        declarations: Seq[Declaration]) : String = {
        val inputLines: Vector[String] =
            if (declarations.isEmpty) {
                // Corner case: there are no inputs and no
                // expressions to calculate. Generated a valid
                // workflow that does nothing.
                Vector("Int xxxx = 0")
            } else {
                declarations.map(x => WdlPrettyPrinter.apply(x, 1)).flatten.toVector
            }
        val wdlCode = WdlPrettyPrinter.buildBlock(s"workflow w", inputLines, 0).mkString("\n")
        verifyWdlCodeIsLegal(wdlCode)
        wdlCode
    }

    // Create a preliminary applet to handle workflow inputs, top-level
    // expressions and constants.
    //
    // For example, a workflow could start like:
    //
    // workflow PairedEndSingleSampleWorkflow {
    //   String tag = "v1.56"
    //   String gvcf_suffix
    //   File ref_fasta
    //   String cmdline="perf mem -K 100000000 -p -v 3 $tag"
    //   ...
    // }
    //
    //  The first varible [tag] has a default value, and does not have to be
    //  defined on the command line. The next two ([gvcf_suffix, ref_fasta])
    //  must be defined on the command line. [cmdline] is a calculation that
    //  uses constants and variables.
    //
    def compileCommon(appletName: String,
                      declarations: Seq[Declaration],
                      cState: State) : (IR.Stage, IR.Applet) = {
        Utils.trace(cState.verbose, s"Compiling common applet ${appletName}".format(appletName))
        val code = genEvalWorkflowFromDeclarations(appletName, declarations)

        // Only workflow declarations that do not have an expression,
        // needs to be provide by the user.
        //
        // Examples:
        //   File x
        //   String y = "abc"
        //   Float pi = 3 + .14
        //
        // x - must be provided as an applet input
        // y, pi -- calculated, non inputs
        val inputVars : Vector[IR.CVar] =  declarations.map{ decl =>
            decl.expression match {
                case None => Some(IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast))
                case Some(_) => None
            }
        }.flatten.toVector

        val outputVars: Vector[IR.CVar] = declarations.map{ decl =>
            IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast)
        }.toVector

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               None,
                               cState.destination,
                               IR.AppletKind.Eval,
                               code)
        (IR.Stage(appletName, appletName, Vector[IR.SArg](), outputVars),
         applet)
    }

    /**
      Create an applet+stage to evaluate expressions in the middle of
      a workflow.  Sometimes, we need to pass arguments to the
      applet. For example, the declaration xtmp2 below requires
      passing in the result of the Add call.

workflow w {
    Int ai
    call Add  {
        input: a=ai, b=3
    }
    Int xtmp2 = Add.result + 10
    call Multiply  {
        input: a=xtmp2, b=2
    }
}
      */
    def compileEvalAndPassClosure(appletName: String,
                                  declarations: Seq[Declaration],
                                  env: CallEnv,
                                  cState: State) : (IR.Stage, IR.Applet) = {
        Utils.trace(cState.verbose, s"Compiling evaluation applet ${appletName}".format(appletName))

        // Figure out the closure
        var closure = Map.empty[String, LinkedVar]
        declarations.foreach { decl =>
            decl.expression match {
                case Some(expr) =>
                    closure = updateClosure(closure, env, expr, false, cState)
                case None => ()
            }
        }

        // figure out the inputs
        closure = closure.map{ case (key,lVar) =>
            val cVar = IR.CVar(key, lVar.cVar.wdlType, lVar.cVar.ast)
            key -> LinkedVar(cVar, lVar.sArg)
        }.toMap
        val inputVars: Vector[IR.CVar] = closure.map{ case (_, lVar) => lVar.cVar }.toVector
        val inputDecls: Vector[Declaration] = closure.map{ case(_, lVar) =>
            declarationGen(lVar.cVar.wdlType, lVar.cVar.dxVarName, None)
        }.toVector

        // figure out the outputs
        val outputVars: Vector[IR.CVar] = declarations.map{ decl =>
            IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast)
        }.toVector
        val outputDeclarations = declarations.map{ decl =>
            decl.expression match {
                case Some(expr) =>
                    declarationGen(decl.wdlType, decl.unqualifiedName,
                                   Some(exprRenameVars(expr, inputVars)))
                case None => decl
            }
        }.toVector

        // We need minimal compute resources, use the default instance type
        val code = genEvalWorkflowFromDeclarations(appletName, inputDecls ++ outputDeclarations)
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               None,
                               cState.destination,
                               IR.AppletKind.Eval,
                               code)

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = closure.map{ case (_, lVar) => lVar.sArg }.toVector

        (IR.Stage(appletName, appletName, inputs, outputVars),
         applet)
    }

    // Compile a WDL task into an applet
    def compileTask(task : Task, cState: State) : (IR.Applet, Vector[IR.CVar]) = {
        Utils.trace(cState.verbose, s"Compiling task ${task.name}")
        val appletFqn : String = task.name

        // The task inputs are those that do not have expressions
        val inputVars : Vector[IR.CVar] =  task.declarations.map{ decl =>
            decl.expression match {
                case None => Some(IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast))
                case Some(_) => None
            }
        }.flatten.toVector
        val outputVars : Vector[IR.CVar] = task.outputs.map{ tso =>
            IR.CVar(tso.unqualifiedName, tso.wdlType, tso.ast)
        }.toVector

        // Figure out if we need to use docker
        val docker = task.runtimeAttributes.attrs.get("docker") match {
            case None => None
            case Some(wdlExpression) =>
                var buf = wdlExpression.toWdlString
                buf = buf.replaceAll("\"","")
                Some(buf)
        }
        val wdlCode = WdlPrettyPrinter.apply(task, 0).mkString("\n")
        verifyWdlCodeIsLegal(wdlCode)
        val applet = IR.Applet(appletFqn,
                               inputVars,
                               outputVars,
                               calcInstanceType(Some(task)),
                               docker,
                               cState.destination,
                               IR.AppletKind.Task,
                               wdlCode)
        (applet, outputVars)
    }

    def compileCall(call: Call,
                    taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])],
                    env : CallEnv,
                    cState: State) : IR.Stage = {
        // Find the right applet
        val name = call match {
            case x:TaskCall => x.task.name
            case x:WorkflowCall =>
                throw new Exception(cState.cef.notCurrentlySupported(call.ast, s"calling a workflow"))
        }
        val (callee, outputs) = taskApplets(name)

        // Extract the input values/links from the environment
        val inputs: Vector[IR.SArg] = callee.inputs.map{ cVar =>
            val expr: Option[(String,WdlExpression)] =
                call.inputMappings.find{ case (k,v) => k == cVar.name }
            expr match {
                case None =>
                    // input is unbound; the workflow does not provide it.
                    if (Utils.isOptional(cVar.wdlType)) {
                        // This is an applet optional input, we don't have to supply it
                        IR.SArgEmpty
                    } else {
                        // A compulsory input. Print a warning, the user may wish to supply
                        // it at runtime.
                        System.err.println(s"""|Note: workflow doesn't supply required input
                                               |${cVar.name} to call ${call.unqualifiedName};
                                               |leaving corresponding DNAnexus workflow input unbound"""
                                               .stripMargin.replaceAll("\n", " "))
                        IR.SArgEmpty
                    }
                case Some((_,e)) => e.ast match {
                    case t: Terminal if t.getTerminalStr == "identifier" =>
                        val lVar = env.get(t.getSourceString) match {
                            case Some(x) => x
                            case None => throw new Exception(cState.cef.missingVarRefException(t))
                        }
                        lVar.sArg
                    case t: Terminal =>
                        def lookup(x:String) : WdlValue = {
                            throw new Exception(cState.cef.evaluatingTerminal(t, x))
                        }
                        val ve = ValueEvaluator(lookup, PureStandardLibraryFunctions)
                        val wValue: WdlValue = ve.evaluate(e.ast).get
                        IR.SArgConst(wValue)
                    case a: Ast if a.isMemberAccess =>
                        // An expression like A.B.C, or A.x
                        val lVar = lookupInEnv(env, e, cState)
                        lVar.sArg
                    case _:Ast =>
                        throw new Exception(cState.cef.expressionMustBeConstOrVar(e))
                }
            }
        }

        val stageName = callUniqueName(call, cState)
        IR.Stage(stageName, name, inputs, callee.outputs)
    }

    // Create a valid WDL workflow that runs a scatter. The main modification
    // required here is renaming variables of the form A.x to A_x.
    def scGenWorklow(scatter: Scatter,
                     taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])],
                     inputVars: Vector[IR.CVar],
                     outputVars: Vector[IR.CVar],
                     cState: State) : String = {
        // A workflow must have definitions for all the tasks it
        // calls. However, a scatter calls tasks, which missing from
        // the WDL file we generate. To ameliorate this, we add stubs
        // for called tasks.
        val calls: Vector[Call] = scatter.calls.toVector
        val taskStubs: Vector[String] = calls.map{ call =>
            val name = call match {
                case x:TaskCall => x.task.name
                case x:WorkflowCall =>
                    throw new Exception(cState.cef.notCurrentlySupported(x.ast, "calling workflows"))
            }
            val (irApplet,_) = taskApplets.get(name) match {
                case None => throw new Exception(s"Calling undefined task ${name}")
                case Some(x) => x
            }
            val task = genAppletStub(irApplet)
            WdlPrettyPrinter.apply(task, 0).mkString("\n") ++ "\n"
        }

        val decls: Vector[String]  = inputVars.map{ cVar =>
            val d = declarationGen(cVar.wdlType, cVar.dxVarName, None)
            WdlPrettyPrinter.apply(d, 1)
        }.flatten

        // Rename the variables we got from the input.
        def exprTransform(expr: WdlExpression) : WdlExpression = {
            exprRenameVars(expr, inputVars)
        }

        val lines: Vector[String] = decls ++
            WdlPrettyPrinter.scatterRewrite(scatter, 1, exprTransform)
        val wfCode = WdlPrettyPrinter.buildBlock("workflow w", lines, 0).mkString("\n")
        val wdlCode = taskStubs.mkString("\n") ++ "\n" ++ wfCode
        verifyWdlCodeIsLegal(wdlCode)
        wdlCode
    }

    // Compile a scatter block
    def compileScatter(wf : Workflow,
                       scatter: Scatter,
                       taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])],
                       env : CallEnv,
                       cState: State) : (IR.Stage, IR.Applet) = {
        val (topDecls, rest) = Utils.splitBlockDeclarations(scatter.children.toList)
        val calls : Seq[Call] = rest.map {
            case call: Call => call
            case x =>
                throw new Exception(cState.cef.notCurrentlySupported(x.ast, "scatter element"))
        }
        if (calls.isEmpty)
            throw new Exception(cState.cef.notCurrentlySupported(scatter.ast, "scatter with no calls"))

        // Construct a unique stage name by adding "scatter" to the
        // first call name. This is guaranteed to be unique within a
        // workflow, because call names must be unique (or aliased)
        val stageName = Utils.SCATTER ++ "_" ++ callUniqueName(calls(0), cState)
        Utils.trace(cState.verbose, s"compiling scatter ${stageName}")

        // Construct the block output by unifying individual call outputs.
        // Each applet output becomes an array of that type. For example,
        // an Int becomes an Array[Int].
        val outputVars : Vector[IR.CVar] = calls.map { call =>
            val task = Utils.taskOfCall(call)
            task.outputs.map { tso =>
                val varName = callUniqueName(call, cState) ++ "." ++ tso.unqualifiedName
                IR.CVar(varName, WdlArrayType(tso.wdlType), tso.ast)
            }
        }.flatten.toVector

        // Figure out the closure required to evaluate the expression being looped over.
        var closure = Map.empty[String, LinkedVar]
        closure = updateClosure(closure, env, scatter.collection, true, cState)

        // Figure out the type of the iteration variable,
        // and ignore it for closure purposes
        val typeEnv : Map[String, WdlType] = env.map { case (x, LinkedVar(cVar,_)) =>
            x -> cVar.wdlType
        }.toMap
        val iterVarType : WdlType = calcIterWdlType(scatter, typeEnv)
        val cVar = IR.CVar(scatter.item, iterVarType, scatter.ast)
        var innerEnv : CallEnv = env + (scatter.item -> LinkedVar(cVar, IR.SArgEmpty))

        // Add the declarations at the top of the block to the environment
        val localDecls = topDecls.map{ decl =>
            val cVar = IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast)
            decl.unqualifiedName -> LinkedVar(cVar, IR.SArgEmpty)
        }.toMap
        innerEnv = innerEnv ++ localDecls

        // Get closure dependencies from the top declarations
        topDecls.foreach { decl =>
            decl.expression match {
                case Some(expr) =>
                    closure = updateClosure(closure, innerEnv, expr, false, cState)
                case None => ()
            }
        }
        calls.foreach { call =>
            call.inputMappings.foreach { case (_, expr) =>
                closure = updateClosure(closure, innerEnv, expr, false, cState)
            }
        }

        // remove the iteration variable from the closure
        closure -= scatter.item
        // remove the local variables from the closure
        topDecls.foreach( decl =>
            closure -= decl.unqualifiedName
        )
        //Utils.trace(cState.verbose, s"scatter closure=${closure}")

        val inputVars : Vector[IR.CVar] = closure.map {
            case (varName, LinkedVar(cVar, _)) => IR.CVar(varName, cVar.wdlType, cVar.ast)
        }.toVector
        val wdlCode = scGenWorklow(scatter, taskApplets, inputVars, outputVars, cState)
        val applet = IR.Applet(wf.unqualifiedName ++ "." ++ stageName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               None,
                               cState.destination,
                               IR.AppletKind.Scatter,
                               wdlCode)

        // The calls will be made from the scatter applet at runtime.
        // Collect all the outputs in arrays.
        calls.foreach { call =>
            val task = Utils.taskOfCall(call)
            val outputs = task.outputs.map { tso =>
                IR.CVar(callUniqueName(call, cState) ++ "." ++ tso.unqualifiedName,
                        tso.wdlType, tso.ast)
            }
            // Add bindings for all call output variables. This allows later calls to refer
            // to these results. Links to their values are unknown at compile time, which
            // is why they are set to SArgEmpty.
            for (cVar <- outputs) {
                val fqVarName = callUniqueName(call, cState) ++ "." ++ cVar.name
                innerEnv = innerEnv + (fqVarName ->
                                           LinkedVar(IR.CVar(fqVarName, cVar.wdlType, cVar.ast),
                                                        IR.SArgEmpty))
            }
        }

        val sargs : Vector[IR.SArg] = closure.map {
            case (_, LinkedVar(_, sArg)) => sArg
        }.toVector
        (IR.Stage(stageName, applet.name, sargs, outputVars),
         applet)
    }


    // compile the WDL source code into intermediate representation
    def apply(ns : WdlNamespace,
              destination: String,
              cef: CompilerErrorFormatter,
              verbose: Boolean) : IR.Workflow = {
        val cState = new State(destination, cef, verbose)
        Utils.trace(cState.verbose, "FrontEnd pass")

        // compile all the tasks into applets
        Utils.trace(cState.verbose, "FrontEnd: compiling applets")
        val taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])] = ns.tasks.map{ task =>
            val (applet, outputs) = compileTask(task, cState)
            task.name -> (applet, outputs)
        }.toMap

        // extract the workflow
        Utils.trace(cState.verbose, "FrontEnd: compiling workflow")
        val wf = ns match {
            case nswf : WdlNamespaceWithWorkflow => nswf.workflow
            case _ => throw new Exception("WDL does not have a workflow")
        }

        // Lift all declarations that can be evaluated at the top of
        // the block.
        val (topDecls, wfBody) = Utils.splitBlockDeclarations(wf.children.toList)

        // Create a preliminary stage to handle workflow inputs, and top-level
        // declarations.
        val (commonStage, commonApplet) = compileCommon(wf.unqualifiedName ++ "_" ++ Utils.COMMON,
                                                        topDecls, cState)

        // An environment where variables are defined
        var env : CallEnv = commonStage.outputs.map { cVar =>
            cVar.name -> LinkedVar(cVar, IR.SArgLink(commonStage.name, cVar))
        }.toMap
        var evalAppletNum = 0

        // Create a stage per call/scatter-block/declaration-block
        val subBlocks = splitBlock(wfBody)
        val initAccu : Vector[(IR.Stage, Option[IR.Applet])] = Vector((commonStage, Some(commonApplet)))
        val allStageInfo = subBlocks.foldLeft(initAccu) { (accu, child) =>
            val (stage, appletOpt) = child match {
                case DeclBlock(decls) =>
                    evalAppletNum = evalAppletNum + 1
                    val appletName = wf.unqualifiedName ++ "_eval" ++ evalAppletNum.toString
                    val (stage, applet) = compileEvalAndPassClosure(appletName, decls, env, cState)
                    (stage, Some(applet))
                case Singleton(call: Call) =>
                    val stage = compileCall(call, taskApplets, env, cState)
                    (stage, None)
                case Singleton(scatter : Scatter) =>
                    val (stage, applet) = compileScatter(wf, scatter, taskApplets, env, cState)
                    (stage, Some(applet))
                case Singleton(x) =>
                    throw new Exception(cState.cef.notCurrentlySupported(
                                            x.ast, "Workflow element"))
            }
            // Add bindings for the output variables. This allows later calls to refer
            // to these results. In case of scatters, there is no block name to reference.
            for (cVar <- stage.outputs) {
                val fqVarName : String = child match {
                    case DeclBlock(decls) => cVar.name
                    case Singleton(call : Call) => stage.name ++ "." ++ cVar.name
                    case Singleton(scatter : Scatter) => cVar.name
                    case _ => throw new Exception("Sanity")
                }
                env = env + (fqVarName ->
                                 LinkedVar(cVar, IR.SArgLink(stage.name, cVar)))
            }
            accu :+ (stage,appletOpt)
        }

        val (stages, auxApplets) = allStageInfo.unzip
        val tApplets: Vector[IR.Applet] = taskApplets.map{ case (k,(applet,_)) => applet }.toVector
        IR.Workflow(wf.unqualifiedName, stages, tApplets ++ auxApplets.flatten.toVector)
    }
}
