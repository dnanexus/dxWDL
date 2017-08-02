/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL

import java.nio.file.{Files, Paths, Path}
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import scala.util.{Failure, Success, Try}
import wdl4s._
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.expression._
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions

case class CompilerIR(gWorkflowOutputs: Option[Seq[WorkflowOutput]],
                      destination: String,
                      instanceTypeDB: InstanceTypeDB,
                      cef: CompilerErrorFormatter,
                      reorg: Boolean,
                      verbose: Utils.Verbose) {
    class DynamicInstanceTypesException private(ex: Exception) extends RuntimeException(ex) {
        def this() = this(new RuntimeException("Runtime instance type calculation required"))
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
    def lookupInEnv(env: CallEnv, expr: WdlExpression) : LinkedVar = {
        val fqn: String = expr.toWdlString
        env.get(fqn) match {
            case Some(x) => x
            case None =>
                val t: Terminal = AstTools.findTerminals(expr.ast).head
                throw new Exception(cef.missingVarRefException(t))
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
    def genAppletStub(applet: IR.Applet, scope: Scope) : Task = {
        val task = WdlRewrite.taskGenEmpty(applet.name, scope)
        val inputs = applet.inputs.map{ cVar =>
            WdlRewrite.declaration(cVar.wdlType, cVar.name, None)
        }.toVector
        val outputs = applet.outputs.map{ cVar =>
            WdlRewrite.taskOutput(cVar.name, cVar.wdlType, task)
        }.toVector
        task.children = inputs ++ outputs
        task
    }

    // Rename member accesses inside an expression, from the form A.x
    // to A_x. This is used inside an applet of WDL generated code.
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

    def callUniqueName(call : Call) = {
        val nm = call.alias match {
            case Some(x) => x
            case None => Utils.taskOfCall(call).name
        }
        Utils.reservedAppletPrefixes.foreach{ prefix =>
            if (nm.startsWith(prefix))
                throw new Exception(cef.illegalCallName(call))
        }
        Utils.reservedSubstrings.foreach{ sb =>
            if (nm contains sb)
                throw new Exception(cef.illegalCallName(call))
        }
        nm
    }

    // Figure out which instance to use.
    //
    // Extract three fields from the task:
    // RAM, disk space, and number of cores. These are WDL expressions
    // that, in the general case, could be calculated only at runtime.
    // Currently, we support only constants. If a runtime expression is used,
    // we convert it to a moderatly high constant.
    def calcInstanceType(taskOpt: Option[Task]) : IR.InstanceType = {
        def lookup(varName : String) : WdlValue = {
            throw new DynamicInstanceTypesException()
        }
        def evalAttr(task: Task, attrName: String) : Option[WdlValue] = {
            task.runtimeAttributes.attrs.get(attrName) match {
                case None => None
                case Some(expr) =>
                    Some(expr.evaluate(lookup, NoFunctions).get)
            }
        }

        try {
            taskOpt match {
                case None =>
                    // A utility calculation, that requires minimal computing resources.
                    // For example, the top level of a scatter block. We use
                    // the default instance type, because one will probably be available,
                    // and it will probably be inexpensive.
                    IR.InstanceTypeDefault
                case Some(task) =>
                    val dxInstaceType = evalAttr(task, Utils.DX_INSTANCE_TYPE_ATTR)
                    val memory = evalAttr(task, "memory")
                    val diskSpace = evalAttr(task, "disks")
                    val cores = evalAttr(task, "cpu")
                    IR.InstanceTypeConst(
                        instanceTypeDB.apply(dxInstaceType, memory, diskSpace, cores))
            }
        } catch {
            case e : DynamicInstanceTypesException =>
                // The generated code will need to calculate the instance type at runtime
                IR.InstanceTypeRuntime
        }
    }

    // split a block of statements into sub-blocks with continguous declarations.
    // Scatters are special, because they can also hold declarations that come
    // before them
    // Example:
    //
    //   call x
    //   Int a
    //   Int b
    //   call y
    //   call z
    //   String buf
    //   scatter ssc
    //  =>
    //   [call x]
    //   [Int a, Int b]
    //   [call y]
    //   [call z]
    //   [String buf, scatter ssc]
    //
    sealed trait Block
    case class BlockDecl(decls: Vector[Declaration]) extends Block
    case class BlockIf(preDecls: Vector[Declaration], cond: If) extends Block
    case class BlockScatter(preDecls: Vector[Declaration], scatter: Scatter) extends Block
    case class BlockScope(scope: Scope) extends Block

    def splitIntoBlocks(children: Seq[Scope]) : Vector[Block] = {
        val initAccu = (Vector[Block](), Vector[Declaration]())
        val (subBlocks, decls) = children.foldLeft(initAccu) {
            case ((subBlocks, decls), d:Declaration) =>
                (subBlocks, decls :+ d)
            case ((subBlocks, decls), ssc:Scatter) =>
                (subBlocks :+ BlockScatter(decls, ssc), Vector.empty)
            case ((subBlocks, decls), cond:If) =>
                (subBlocks :+ BlockIf(decls, cond), Vector.empty)
            case ((subBlocks, decls), scope) =>
                if (decls.isEmpty)
                    (subBlocks :+ BlockScope(scope),  Vector.empty)
                else
                    (subBlocks :+ BlockDecl(decls) :+ BlockScope(scope), Vector.empty)
        }
        if (decls.isEmpty)
            subBlocks
        else
            subBlocks :+ BlockDecl(decls)
    }

    // Update a closure with all the variables required
    // for an expression. Ignore variable accesses outside the environment;
    // it is assumed that these are accesses to local variables.
    //
    // @param  closure   call closure
    // @param  env       mapping from fully qualified WDL name to a dxlink
    // @param  expr      expression as it appears in source WDL
    def updateClosure(closure : CallEnv,
                      env : CallEnv,
                      expr : WdlExpression) : CallEnv = {
        expr.ast match {
            case t: Terminal =>
                val srcStr = t.getSourceString
                t.getTerminalStr match {
                    case "identifier" =>
                        env.get(srcStr) match {
                            case Some(lVar) =>
                                val fqn = WdlExpression.toString(t)
                                closure + (fqn -> lVar)
                            case None => closure
                        }
                    case _ => closure
                }

            case a: Ast if a.isMemberAccess =>
                // This is a case of accessing something like A.B.C
                // The RHS is C, and the LHS is A.B
                val rhs : Terminal = a.getAttribute("rhs") match {
                    case rhs:Terminal if rhs.getTerminalStr == "identifier" => rhs
                    case _ => throw new Exception(cef.rightSideMustBeIdentifer(a))
                }

                // The FQN is "A.B.C"
                val fqn = WdlExpression.toString(a)
                env.get(fqn) match {
                    case Some(lVar) =>
                        closure + (fqn -> lVar)
                    case None =>
                        closure
                }
            case a: Ast =>
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
                    }
                }.flatten
                closure ++ allDeps
        }
    }

    // Make sure that the WDL code we generate is actually legal.
    def verifyWdlCodeIsLegal(ns: WdlNamespace) : Unit = {
        // convert to a string
        val wdlCode = WdlPrettyPrinter(false, None).apply(ns, 0).mkString("\n")
        val nsTest:Try[WdlNamespace] = WdlNamespace.loadUsingSource(wdlCode, None, None)
        nsTest match {
            case Success(_) => ()
            case Failure(f) =>
                System.err.println("Error verifying generated WDL code")
                System.err.println(wdlCode)
                throw f
        }
    }

    // Print a WDL workflow that evaluates expressions
    def genEvalWorkflowFromDeclarations(name: String,
                                        declarations_i: Seq[Declaration],
                                        outputs: Vector[IR.CVar]) : Workflow = {
        val declarations =
            if (declarations_i.isEmpty) {
                // Corner case: there are no inputs and no
                // expressions to calculate. Generated a valid
                // workflow that does nothing.
                val d = WdlRewrite.declaration(WdlIntegerType,
                                               "xxxx",
                                               Some(WdlExpression.fromString("0")))
                Vector(d)
            } else {
                declarations_i.toVector
            }
        val wf = WdlRewrite.workflowGenEmpty("w")
        val wfOutputs = outputs.map(
            cVar => WdlRewrite.workflowOutput(cVar.name, cVar.wdlType, wf))
        wf.children = declarations ++ wfOutputs
        wf
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
                      declarations: Seq[Declaration]) : (IR.Stage, IR.Applet) = {
        Utils.trace(verbose.on, s"Compiling common applet ${appletName}".format(appletName))

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
        val code:Workflow = genEvalWorkflowFromDeclarations(appletName,
                                                            declarations,
                                                            outputVars)

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               false,
                               destination,
                               IR.AppletKindEval,
                               WdlRewrite.namespace(code, Seq.empty))
        verifyWdlCodeIsLegal(applet.ns)

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
                                  env: CallEnv) : (IR.Stage, IR.Applet) = {
        Utils.trace(verbose.on, s"Compiling evaluation applet ${appletName}")

        // Figure out the closure
        var closure = Map.empty[String, LinkedVar]
        declarations.foreach { decl =>
            decl.expression match {
                case Some(expr) =>
                    closure = updateClosure(closure, env, expr)
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
            WdlRewrite.declaration(lVar.cVar.wdlType, lVar.cVar.dxVarName, None)
        }.toVector

        // figure out the outputs
        val outputVars: Vector[IR.CVar] = declarations.map{ decl =>
            IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast)
        }.toVector
        val outputDeclarations = declarations.map{ decl =>
            decl.expression match {
                case Some(expr) =>
                    WdlRewrite.declaration(decl.wdlType, decl.unqualifiedName,
                                           Some(exprRenameVars(expr, inputVars)))
                case None => decl
            }
        }.toVector
        val code:Workflow = genEvalWorkflowFromDeclarations(appletName,
                                                            inputDecls ++ outputDeclarations,
                                                            outputVars)

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               false,
                               destination,
                               IR.AppletKindEval,
                               WdlRewrite.namespace(code, Seq.empty))
        verifyWdlCodeIsLegal(applet.ns)

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = closure.map{ case (_, lVar) => lVar.sArg }.toVector

        (IR.Stage(appletName, appletName, inputs, outputVars),
         applet)
    }


    // 1. The output variable name must not have dots, these
    //    are illegal in dx.
    // 2. The expression requires variable renaming
    //
    // For example:
    // workflow w {
    //    Int mutex_count
    //    File genome_ref
    //    output {
    //       Int count = mutec.count
    //       File ref = genome.ref
    //    }
    // }
    //
    // Must be converted into:
    // output {
    //     Int count = mutec_count
    //     File ref = genome_ref
    // }
    def compileOutputSection(appletName: String,
                             env: CallEnv,
                             wfOutputs: Seq[WorkflowOutput]) : (IR.Stage, IR.Applet) = {
        Utils.trace(verbose.on, s"Compiling output section applet ${appletName}")

        // Figure out the closure
        var closure = Map.empty[String, LinkedVar]
        wfOutputs.foreach { wot =>
            closure = updateClosure(closure, env, wot.requiredExpression)
        }

        // figure out the inputs
        closure = closure.map{ case (key,lVar) =>
            val cVar = IR.CVar(key, lVar.cVar.wdlType, lVar.cVar.ast)
            key -> LinkedVar(cVar, lVar.sArg)
        }.toMap
        val inputVars: Vector[IR.CVar] = closure.map{ case (_, lVar) => lVar.cVar }.toVector
        val inputDecls: Vector[Declaration] = closure.map{ case(_, lVar) =>
            WdlRewrite.declaration(lVar.cVar.wdlType, lVar.cVar.dxVarName, None)
        }.toVector

        // Rename the variables appearing in the output expressions.
        // mutec.count  ---> mutec_count
        def transform(expr: WdlExpression) : WdlExpression = {
            exprRenameVars(expr, inputVars)
        }

        // Workflow outputs
        val outputPairs: Vector[(WorkflowOutput, IR.CVar)] = wfOutputs.map { wot =>
            val cVar = IR.CVar(wot.unqualifiedName, wot.wdlType, wot.ast)
            val dxVarName = Utils.transformVarName(wot.unqualifiedName)
            val dxWot = WdlRewrite.workflowOutput(dxVarName,
                                                  wot.wdlType,
                                                  transform(wot.requiredExpression))
            (dxWot, cVar)
        }.toVector
        val (outputs, outputVars) = outputPairs.unzip

        // Create a workflow with no calls.
        val code:Workflow = WdlRewrite.workflowGenEmpty("w")
        code.children = inputDecls ++ outputs

        // Should we reorganize the output folder?
        val appletKind =
            if (reorg) IR.AppletKindWorkflowOutputsAndReorg
            else IR.AppletKindWorkflowOutputs

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               false,
                               destination,
                               appletKind,
                               WdlRewrite.namespace(code, Seq.empty))
        verifyWdlCodeIsLegal(applet.ns)

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = closure.map{ case (_, lVar) => lVar.sArg }.toVector

        (IR.Stage(appletName, appletName, inputs, outputVars),
         applet)
    }

    // Compile a WDL task into an applet
    def compileTask(task : Task) : (IR.Applet, Vector[IR.CVar]) = {
        Utils.trace(verbose.on, s"Compiling task ${task.name}")

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
        val useDocker = task.runtimeAttributes.attrs.get("docker") match {
            case None => false
            case Some(_) => true
        }
        val applet = IR.Applet(task.name,
                               inputVars,
                               outputVars,
                               calcInstanceType(Some(task)),
                               useDocker,
                               destination,
                               IR.AppletKindTask,
                               WdlRewrite.namespace(task))
        verifyWdlCodeIsLegal(applet.ns)
        (applet, outputVars)
    }

    def taskOfCall(call:Call): Task = {
        call match {
            case x:TaskCall => x.task
            case x:WorkflowCall =>
                throw new Exception(cef.notCurrentlySupported(call.ast, s"calling a workflow"))
        }
    }

    def findInputByName(call: Call, cVar: IR.CVar) : Option[(String,WdlExpression)] = {
        call.inputMappings.find{ case (k,v) => k == cVar.name }
    }

    def compileCall(call: Call,
                    taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])],
                    env : CallEnv) : IR.Stage = {
        // Find the right applet
        val task = taskOfCall(call)
        val (callee, outputs) = taskApplets.get(task.name) match {
            case Some(x) => x
            case None => throw new Exception(s"Undefined task ${task.name}")
        }

        // Extract the input values/links from the environment
        val inputs: Vector[IR.SArg] = callee.inputs.map{ cVar =>
            findInputByName(call, cVar) match {
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
                            case None => throw new Exception(cef.missingVarRefException(t))
                        }
                        lVar.sArg
                    case t: Terminal =>
                        def lookup(x:String) : WdlValue = {
                            throw new Exception(cef.evaluatingTerminal(t, x))
                        }
                        val ve = ValueEvaluator(lookup, PureStandardLibraryFunctions)
                        val wValue: WdlValue = ve.evaluate(e.ast).get
                        IR.SArgConst(wValue)
                    case a: Ast if a.isMemberAccess =>
                        // An expression like A.B.C, or A.x
                        val lVar = lookupInEnv(env, e)
                        lVar.sArg
                    case _:Ast =>
                        throw new Exception(cef.expressionMustBeConstOrVar(e))
                }
            }
        }

        val stageName = callUniqueName(call)
        IR.Stage(stageName, task.name, inputs, callee.outputs)
    }

    // Split a block (Scatter, If, ..) into the top declarations,
    // the and the bottom calls.
    def blockSplit(children: Vector[Scope]) : (Vector[Declaration], Vector[Call]) = {
        val (topDecls, rest) = Utils.splitBlockDeclarations(children.toList)
        val calls : Seq[Call] = rest.map {
            case call: Call => call
            case decl:Declaration =>
                throw new Exception(cef.notCurrentlySupported(decl.ast,
                                                              "declaration in the middle of a block"))
            case x =>
                throw new Exception(cef.notCurrentlySupported(x.ast, "block element"))
        }
        (topDecls.toVector ,calls.toVector)
    }

    // Figure out the closure for a block, and then build the input
    // definitions.
    //
    //  preDecls: Declarations that come immediately before the block.
    //            We pack them into the same applet.
    //  topBlockExpr: condition variable, scatter loop expression, ...
    //  topDecls: declarations inside the block, that come at the beginning
    //  env: environment outside the block
    def blockInputs(preDecls: Vector[Declaration],
                    topBlockExpr: WdlExpression,
                    topDecls: Vector[Declaration],
                    calls: Vector[Call],
                    env : CallEnv) : (Map[String, LinkedVar], Vector[IR.CVar]) = {
        var closure = Map.empty[String, LinkedVar]
        preDecls.foreach { decl =>
            decl.expression match {
                case Some(expr) =>
                    closure = updateClosure(closure, env, expr)
                case None => ()
            }
        }

        // Ensure the top variable is in the closure.
        closure = updateClosure(closure, env, topBlockExpr)

        // Get closure dependencies from the top declarations
        topDecls.foreach { decl =>
            decl.expression match {
                case Some(expr) =>
                    closure = updateClosure(closure, env, expr)
                case None => ()
            }
        }
        // Make a pass on the calls inside the block
        calls.foreach { call =>
            call.inputMappings.foreach { case (_, expr) =>
                closure = updateClosure(closure, env, expr)
            }
        }

        val inputVars: Vector[IR.CVar] = closure.map {
            case (varName, LinkedVar(cVar, _)) =>
                // a variable that must be passed to the scatter applet
                assert(env contains varName)
                Some(IR.CVar(varName, cVar.wdlType, cVar.ast))
        }.flatten.toVector

        (closure, inputVars)
    }

    // figure out if a variable is used only inside
    // the block (scatter, if, ...)
    def isLocal(decl: Declaration): Boolean = {
        if (Utils.isGeneratedVar(decl.unqualifiedName)) {
            // A variable generated by the compiler. It might be used only
            // locally.
            // find all dependent nodes
            val dNodes:Set[GraphNode] = decl.downstream
            val declParent:Scope = decl.parent.get

            // figure out if these downstream nodes are in the same scope.
            val dnScopes:Set[GraphNode] = dNodes.filter{ node =>
                node.parent.get.fullyQualifiedName != declParent.fullyQualifiedName
            }
            dnScopes.isEmpty
        } else {
            // A variable defined by the user, most likely used
            // somewhere else in the workflow. Don't check.
            false
        }
    }

    // Construct block outputs (scatter, conditional, ...), these are
    // made up of several categories:
    // 1. All the preamble variables, these could potentially be accessed
    // outside the scatter block.
    // 2. Individual call outputs. Each applet output becomes an array of
    // that type. For example, an Int becomes an Array[Int].
    // 3. Variables defined in the scatter block.
    //
    // Notes:
    // - exclude variables used only inside the block
    // - The type outside a block is *different* than the type in the block.
    //   For example, 'Int x' declared inside a scatter, is
    //   'Array[Int] x]' outside the scatter.
    def blockOutputs(preDecls: Vector[Declaration],
                     scope: Scope,
                     children : Seq[Scope]) : Vector[IR.CVar] = {
        def outsideType(t: WdlType) : WdlType = {
            scope match {
                case _:Scatter => WdlArrayType(t)
                case _:If => WdlOptionalType(t)
                case _ => t
            }
        }
        val preVars: Vector[IR.CVar] = preDecls
            .map( decl => IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast) )
            .toVector
        val outputVars : Vector[IR.CVar] = children.map {
            case call:TaskCall =>
                val task = taskOfCall(call)
                task.outputs.map { tso =>
                    val varName = callUniqueName(call) ++ "." ++ tso.unqualifiedName
                    IR.CVar(varName, outsideType(tso.wdlType), tso.ast)
                }
            case decl:Declaration if !isLocal(decl) =>
                Vector(IR.CVar(decl.unqualifiedName, outsideType(decl.wdlType), decl.ast))
            case decl:Declaration =>
                // local variables, do not export
                Vector()
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast, s"Unimplemented scatter element"))
        }.flatten.toVector
        preVars ++ outputVars
    }

    // Check for each task input, if it is unbound. Make a list, and
    // prefix each variable with the call name. This makes it unique
    // as a scatter input.
    def unspecifiedInputs(call: Call,
                          taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])])
            : Vector[IR.CVar] = {
        val task = taskOfCall(call)
        val (callee, _) = taskApplets(task.name)
        callee.inputs.map{ cVar =>
            val input = findInputByName(call, cVar)
            input match {
                case None =>
                    // unbound input; the workflow does not provide it.
                    if (!Utils.isOptional(cVar.wdlType)) {
                        // A compulsory input. Print a warning, the user may wish to supply
                        // it at runtime.
                        System.err.println(s"""|Note: workflow doesn't supply required required
                                               |input ${cVar.name} to call ${call.unqualifiedName};
                                               |which is in a scatter.
                                               |Propagating input to applet."""
                                               .stripMargin.replaceAll("\n", " "))
                    }
                    Some(IR.CVar(s"${call.unqualifiedName}_${cVar.name}", cVar.wdlType, cVar.ast))
                case Some(_) =>
                    // input is provided
                    None
            }
        }.flatten
    }

    // Modify all the expressions used inside a block
    def blockTransform(preDecls: Vector[Declaration],
                       scope: Scope,
                       inputVars: Vector[IR.CVar]) : (Vector[Declaration], Scope) = {
        // Rename the variables we got from the input.
        def transform(expr: WdlExpression) : WdlExpression = {
            exprRenameVars(expr, inputVars)
        }

        // transform preamble declarations
        val trPreDecls: Vector[Declaration] = preDecls.map { decl =>
            WdlRewrite.declaration(decl.wdlType, decl.unqualifiedName,
                                   decl.expression.map(transform))
        }

        // transform the expressions in a scatter
        def transformChild(scope: Scope): Scope = {
            scope match {
                case tc:TaskCall =>
                    val inputs = tc.inputMappings.map{ case (k,expr) => (k, transform(expr)) }.toMap
                    WdlRewrite.taskCall(tc, inputs)
                case d:Declaration =>
                    new Declaration(d.wdlType, d.unqualifiedName,
                                    d.expression.map(transform), d.parent, d.ast)
                case _ => throw new Exception("Unimplemented scatter element")
            }
        }

        val trScope = scope match {
            case ssc:Scatter =>
                val children = ssc.children.map(transformChild(_))
                WdlRewrite.scatter(ssc, children, transform(ssc.collection))
            case cond:If =>
                val children = cond.children.map(transformChild(_))
                WdlRewrite.cond(cond, children, transform(cond.condition))
            case x =>
                throw new Exception(cef.notCurrentlySupported(x.ast, "block class"))
        }
        (trPreDecls, trScope)
    }

    // Create a valid WDL workflow that runs a block (Scatter, If,
    // etc.) The main modification is renaming variables of the
    // form A.x to A_x.
    def blockGenWorklow(preDecls: Vector[Declaration],
                        scope: Scope,
                        taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])],
                        inputVars: Vector[IR.CVar],
                        outputVars: Vector[IR.CVar]) : WdlNamespace = {
        // A workflow must have definitions for all the tasks it
        // calls. However, a scatter calls tasks, that are missing from
        // the WDL file we generate. To ameliorate this, we add stubs
        // for called tasks.
        val calls: Vector[Call] = scope.calls.toVector
        val taskStubs: Map[String, Task] =
            calls.foldLeft(Map.empty[String,Task]) { case (accu, call) =>
                val name = call match {
                    case x:TaskCall => x.task.name
                    case x:WorkflowCall =>
                        throw new Exception(cef.notCurrentlySupported(x.ast, "calling workflows"))
                }
                val (irApplet,_) = taskApplets.get(name) match {
                    case None => throw new Exception(s"Calling undefined task ${name}")
                    case Some(x) => x
                }
                if (accu contains irApplet.name) {
                    // we have already created a stub for this call
                    accu
                } else {
                    // no existing stub, create it
                    val task = genAppletStub(irApplet, scope)
                    accu + (name -> task)
                }
            }
        val (trPreDecls, trScope) = blockTransform(preDecls, scope, inputVars)
        val decls: Vector[Declaration]  = inputVars.map{ cVar =>
            WdlRewrite.declaration(cVar.wdlType, cVar.dxVarName, None)
        }

        // Create new workflow that includes only this block
        val wf = WdlRewrite.workflowGenEmpty("w")
        wf.children = decls ++ trPreDecls :+ trScope
        val tasks = taskStubs.map{ case (_,x) => x}.toVector
        // namespace that includes the task stubs, and the workflow
        WdlRewrite.namespace(wf, tasks)
    }


    // Compile a scatter block. This includes the block of declarations that
    // come before it [preDecls]. Since we are creating a special applet for this, we might as
    // well evaluate those expressions as well.
    //
    // Note: the front end pass ensures that the scatter collection is a variable.
    def compileScatter(wfUnqualifiedName : String,
                       stageName: String,
                       preDecls: Vector[Declaration],
                       scatter: Scatter,
                       taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])],
                       env : CallEnv) : (IR.Stage, IR.Applet) = {
        Utils.trace(verbose.on, s"compiling scatter ${stageName}")
        val (topDecls, calls) = blockSplit(scatter.children.toVector)

        // Figure out the input definitions
        val (closure, inputVars) = blockInputs(preDecls,
                                               scatter.collection,
                                               topDecls,
                                               calls,
                                               env)

        // Collect unbound inputs. We we want to allow
        // the user to provide them on the command line.
        val extraTaskInputVars: Vector[IR.CVar] =
            calls.map { call =>
                unspecifiedInputs(call, taskApplets)
            }.flatten.toVector

        val outputVars = blockOutputs(preDecls, scatter, scatter.children)
        val wdlCode = blockGenWorklow(preDecls, scatter, taskApplets, inputVars, outputVars)
        val applet = IR.Applet(wfUnqualifiedName ++ "_" ++ stageName,
                               inputVars ++ extraTaskInputVars,
                               outputVars,
                               calcInstanceType(None),
                               false,
                               destination,
                               IR.AppletKindScatter(calls.map(_.unqualifiedName).toVector),
                               wdlCode)
        verifyWdlCodeIsLegal(applet.ns)

        val sargs : Vector[IR.SArg] = closure.map {
            case (_, LinkedVar(_, sArg)) => sArg
        }.toVector
        (IR.Stage(stageName, applet.name, sargs, outputVars),
         applet)
    }

    // Compile a conditional block.
    //
    // Note: the front end pass ensures that the if condition is a variable.
    def compileIf(wfUnqualifiedName : String,
                  stageName: String,
                  preDecls: Vector[Declaration],
                  cond: If,
                  taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])],
                  env : CallEnv) : (IR.Stage, IR.Applet) = {
        Utils.trace(verbose.on, s"compiling If block ${stageName}")
        val (topDecls, calls) = blockSplit(cond.children.toVector)

        // Figure out the input definitions
        val (closure, inputVars) = blockInputs(preDecls,
                                               cond.condition,
                                               topDecls,
                                               calls,
                                               env)

        // Collect unbound inputs. We we want to allow
        // the user to provide them on the command line.
        val extraTaskInputVars: Vector[IR.CVar] =
            calls.map { call =>
                unspecifiedInputs(call, taskApplets)
            }.flatten.toVector

        val outputVars = blockOutputs(preDecls, cond, cond.children)
        val wdlCode = blockGenWorklow(preDecls, cond, taskApplets, inputVars, outputVars)
        val applet = IR.Applet(wfUnqualifiedName ++ "_" ++ stageName,
                               inputVars ++ extraTaskInputVars,
                               outputVars,
                               calcInstanceType(None),
                               false,
                               destination,
                               IR.AppletKindIf(calls.map(_.unqualifiedName).toVector),
                               wdlCode)
        verifyWdlCodeIsLegal(applet.ns)

        val sargs : Vector[IR.SArg] = closure.map {
            case (_, LinkedVar(_, sArg)) => sArg
        }.toVector
        (IR.Stage(stageName, applet.name, sargs, outputVars),
         applet)
    }

    // Compile a workflow, having compiled the independent tasks.
    def compileWorkflow(wf: Workflow,
                        taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])]) : IR.Workflow = {
        Utils.trace(verbose.on, "FrontEnd: compiling workflow")

        // Get rid of workflow output declarations
        val children = wf.children.filter(x => !x.isInstanceOf[WorkflowOutput])

        // Lift all declarations that can be evaluated at the top of
        // the block.
        val (topDecls, wfBody) = Utils.splitBlockDeclarations(children.toList)

        // Create a preliminary stage to handle workflow inputs, and top-level
        // declarations.
        val (commonStage, commonApplet) = compileCommon(wf.unqualifiedName ++ "_" ++ Utils.COMMON,
                                                        topDecls)

        // An environment where variables are defined
        var env : CallEnv = commonStage.outputs.map { cVar =>
            cVar.name -> LinkedVar(cVar, IR.SArgLink(commonStage.name, cVar))
        }.toMap
        var evalAppletNum = 0
        var scatterNum = 0
        var condNum = 0

        // Create a stage per call/scatter-block/declaration-block
        val subBlocks = splitIntoBlocks(wfBody)
        val initAccu : Vector[(IR.Stage, Option[IR.Applet])] =
            Vector((commonStage, Some(commonApplet)))
        val allStageInfo = subBlocks.foldLeft(initAccu) { (accu, child) =>
            val (stage, appletOpt) = child match {
                case BlockDecl(decls) =>
                    evalAppletNum = evalAppletNum + 1
                    val appletName = wf.unqualifiedName ++ "_eval" ++ evalAppletNum.toString
                    val (stage, applet) = compileEvalAndPassClosure(appletName, decls, env)
                    (stage, Some(applet))
                case BlockIf(preDecls, cond) =>
                    condNum = condNum + 1
                    val condName = Utils.IF ++ "_" ++ condNum.toString
                    val (stage, applet) = compileIf(wf.unqualifiedName, condName, preDecls,
                                                    cond, taskApplets, env)
                    (stage, Some(applet))
                case BlockScatter(preDecls, scatter) =>
                    scatterNum = scatterNum + 1
                    val scatterName = Utils.SCATTER ++ "_" ++ scatterNum.toString
                    val (stage, applet) = compileScatter(wf.unqualifiedName, scatterName, preDecls,
                                                         scatter, taskApplets, env)
                    (stage, Some(applet))
                case BlockScope(call: Call) =>
                    val stage = compileCall(call, taskApplets, env)
                    (stage, None)
                case BlockScope(x) =>
                    throw new Exception(cef.notCurrentlySupported(
                                            x.ast, s"Workflow element type=${x}"))
            }
            // Add bindings for the output variables. This allows later calls to refer
            // to these results. In case of scatters, there is no block name to reference.
            for (cVar <- stage.outputs) {
                val fqVarName : String = child match {
                    case BlockDecl(decls) => cVar.name
                    case BlockIf(_, _) => cVar.name
                    case BlockScatter(_, _) => cVar.name
                    case BlockScope(call : Call) => stage.name ++ "." ++ cVar.name
                    case _ => throw new Exception("Sanity")
                }
                env = env + (fqVarName ->
                                 LinkedVar(cVar, IR.SArgLink(stage.name, cVar)))
            }
            accu :+ (stage,appletOpt)
        }

        // Possibly add workflow output processing
        val allStageWithOutputInfo: Vector[(IR.Stage, Option[IR.Applet])] = gWorkflowOutputs match {
            case None =>
                // output section is empty, no need for an additional phase
                allStageInfo
            case Some(outputs) =>
                // output section is non empty, keep only those files
                // at the destination directory
                val (outSecStg, outSecApl) = compileOutputSection(
                    wf.unqualifiedName ++ "_" ++ Utils.OUTPUT_SECTION,
                    env, outputs)
                allStageInfo :+ (outSecStg, Some(outSecApl))
        }
        val (stages, auxApplets) = allStageWithOutputInfo.unzip
        val tApplets: Vector[IR.Applet] = taskApplets.map{ case (k,(applet,_)) => applet }.toVector
        IR.Workflow(wf.unqualifiedName, stages, tApplets ++ auxApplets.flatten.toVector)
    }

    // Load imported tasks
    def loadImportedTasks(ns: WdlNamespace) : Set[Task] = {
        // Make a pass, and figure out what we access
        //
        ns.taskCalls.map{ call:TaskCall =>
            val taskFqn = call.task.fullyQualifiedName
            ns.resolve(taskFqn) match {
                case Some(task:Task) => task
                case x => throw new Exception(s"Resolved call to ${taskFqn} and got (${x})")
            }
        }.toSet
    }

    // compile the WDL source code into intermediate representation
    def apply(ns : WdlNamespace) : IR.Namespace = {
        Utils.trace(verbose.on, "FrontEnd pass")

        // Load all accessed applets, local or imported
        val accessedTasks: Set[Task] = loadImportedTasks(ns)
        val accessedTaskNames = accessedTasks.map(task => task.name)
        Utils.trace(verbose.on, s"Accessed tasks = ${accessedTaskNames}")

        // Make sure all local tasks are included; we want to compile
        // them even if they are not accessed.
        val allTasks:Set[Task] = accessedTasks ++ ns.tasks.toSet

        // compile all the tasks into applets
        Utils.trace(verbose.on, "FrontEnd: compiling tasks into dx:applets")

        val taskApplets: Map[String, (IR.Applet, Vector[IR.CVar])] = allTasks.map{ task =>
            val (applet, outputs) = compileTask(task)
            task.name -> (applet, outputs)
        }.toMap

        val irApplets: Vector[IR.Applet] = taskApplets.map{
            case (key, (irApplet,_)) => irApplet
        }.toVector

        ns match {
            case nswf : WdlNamespaceWithWorkflow =>
                val wf = nswf.workflow
                val irWf = compileWorkflow(wf, taskApplets)
                IR.Namespace(Some(irWf), irApplets)
            case _ =>
                // The namespace contains only applets, there
                // is no workflow to compile.
                IR.Namespace(None, irApplets)
        }
    }
}
