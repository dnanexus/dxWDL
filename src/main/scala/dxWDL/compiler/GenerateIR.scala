/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import dxWDL.{CompilerErrorFormatter, DeclAttrs, DxPath, InstanceTypeDB, Verbose, WdlPrettyPrinter}
import dxWDL.Utils
import IR.{CVar, LinkedVar, SArg}
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex.Match
import wdl._
import wdl.expression._
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wdl.WdlExpression.AstForExpressions
import wom.types._
import wom.values._

case class GenerateIR(callables: Map[String, IR.Callable],
                      reorg: Boolean,
                      locked: Boolean,
                      blockKind: NamespaceOps.Kind.Value,
                      appletNamePrefix : String,
                      execNameBox: NameBox,
                      cef: CompilerErrorFormatter,
                      verbose: Verbose) {
    private val verbose2:Boolean = verbose.keywords contains "GenerateIR"
    private val freeVarAnalysis = new FreeVarAnalysis(cef, verbose)

    // regular expression for fully-qualified identifiers ("A", "A.B", "A.B.C")
    private val fqnRegex = raw"""[a-zA-Z][a-zA-Z0-9_.]*""".r

    private class DynamicInstanceTypesException private(ex: Exception) extends RuntimeException(ex) {
        def this() = this(new RuntimeException("Runtime instance type calculation required"))
    }

    // Environment (scope) where a call is made
    private type CallEnv = Map[String, LinkedVar]

    // generate a stage Id
    private var stageNum = 0
    private def genStageId(stageName: Option[String] = None) : Utils.DXWorkflowStage = {
        stageName match {
            case None =>
                val retval = Utils.DXWorkflowStage(s"stage-${stageNum}")
                stageNum += 1
                retval
            case Some(nm) =>
                Utils.DXWorkflowStage(s"stage-${nm}")
        }
    }

    private var fragNum = 0
    private def genFragId() : String = {
        fragNum += 1
        fragNum.toString
    }

    // Is a declaration of a task/workflow an input for the
    // compiled dx:applet/dx:workflow ?
    //
    // Examples:
    //   File x
    //   String y = "abc"
    //   Float pi = 3 + .14
    //   Int? z = 3
    //
    // x - must be provided as an applet input
    // y - can be overriden, so is an input
    // pi -- calculated, non inputs
    // z - is an input with a default value
    private def declarationIsInput(decl: Declaration) : Boolean = {
        if (blockKind != NamespaceOps.Kind.TopLevel) {
            // This is a generated workflow, the only inputs
            // are free variables.
            decl.expression match {
                case None => true
                case Some(_) => false
            }
        } else {
            decl.expression match {
                case None => true
                case Some(expr) if Utils.isExpressionConst(expr) => true
                case _ => false
            }
        }
    }

    /** Create a stub for an applet. This is an empty task
      that includes the input and output definitions. It is used
      to allow linking to native DNAx applets (and workflows in the future).

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
    private def genAppletStub(callable: IR.Callable) : WdlTask = {
        Utils.trace(verbose2,
                    s"""|genAppletStub  callable=${callable.name}
                        |  inputs= ${callable.inputVars.map(_.name)}
                        |  outputs= ${callable.outputVars.map(_.name)}"""
                        .stripMargin)
        val task = WdlRewrite.taskGenEmpty(callable.name)
        val inputs = callable.inputVars.map{ cVar =>
            WdlRewrite.declaration(cVar.womType, cVar.name, None)
        }.toVector
        val outputs = callable.outputVars.map{ cVar =>
            WdlRewrite.taskOutput(cVar.name, cVar.womType, task)
        }.toVector
        task.children = inputs ++ outputs
        task
    }

    // Figure out which instance to use.
    //
    // Extract three fields from the task:
    // RAM, disk space, and number of cores. These are WDL expressions
    // that, in the general case, could be calculated only at runtime.
    // Currently, we support only constants. If a runtime expression is used,
    // we convert it to a moderatly high constant.
    def calcInstanceType(taskOpt: Option[WdlTask]) : IR.InstanceType = {
        def lookup(varName : String) : WomValue = {
            throw new DynamicInstanceTypesException()
        }
        def evalAttr(task: WdlTask, attrName: String) : Option[WomValue] = {
            task.runtimeAttributes.attrs.get(attrName) match {
                case None => None
                case Some(expr) =>
                    try {
                        Some(expr.evaluate(lookup, PureStandardLibraryFunctions).get)
                    } catch {
                        case e : Exception =>
                            // The expression can only be evaluated at runtime
                            throw new DynamicInstanceTypesException
                    }
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
                    val iTypeDesc = InstanceTypeDB.parse(dxInstaceType, memory, diskSpace, cores)
                    IR.InstanceTypeConst(iTypeDesc.dxInstanceType,
                                         iTypeDesc.memoryMB,
                                         iTypeDesc.diskGB,
                                         iTypeDesc.cpu)
            }
        } catch {
            case e : DynamicInstanceTypesException =>
                // The generated code will need to calculate the instance type at runtime
                IR.InstanceTypeRuntime
        }
    }

    // Check if the environment has a variable with a binding for
    // a fully-qualified name. For example, if fqn is "A.B.C", then
    // look for "A.B.C", "A.B", or "A", in that order.
    //
    // If the environment has a pair "p", then we want to be able to
    // to return "p" when looking for "p.left" or "p.right".
    //
    def lookupInEnv(fqn: String, env: CallEnv) : Option[(String, LinkedVar)] = {
        if (env contains fqn) {
            // exact match
            Some(fqn, env(fqn))
        } else {
            // A.B.C --> A.B
            val pos = fqn.lastIndexOf(".")
            if (pos < 0) None
            else {
                val lhs = fqn.substring(0, pos)
                lookupInEnv(lhs, env)
            }
        }
    }

    // Find the closure of a block. All the variables defined earlier,
    // that need to be pased the Find all the variables outside this block
    private def blockClosure(statements: Vector[Scope],
                             env : CallEnv,
                             dbg: String) : CallEnv = {
        val xtrnRefs: Vector[DVar] = freeVarAnalysis.apply(statements)
        val closure = xtrnRefs.flatMap { dVar =>
            lookupInEnv(dVar.fullyQualifiedName, env)
        }.toMap
        Utils.trace(verbose2,
                    s"""|blockClosure
                        |   stage: ${dbg}
                        |   xtrnRefs: ${xtrnRefs}
                        |   env: ${env.keys}
                        |   found: ${closure.keys}""".stripMargin)
        closure
    }


    // Make sure that the WDL code we generate is actually legal.
    private def verifyWdlCodeIsLegal(ns: WdlNamespace) : Unit = {
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

    //  1) Assert that there are no calculations in the outputs
    //  2) Figure out from the output cVars and sArgs.
    //
    private def prepareOutputSection(
        env: CallEnv,
        wfOutputs: Seq[WorkflowOutput]) : Vector[(CVar, SArg)] =
    {
        wfOutputs.map { wot =>
            val cVar = CVar(wot.unqualifiedName, wot.womType, DeclAttrs.empty, wot.ast)
            val expr = wot.requiredExpression

            // we only deal with expressions that do not require calculation.
            val sArg = expr.ast match {
                case t: Terminal =>
                    lookupInEnv(expr.toWomString, env) match {
                        case Some((_, lVar)) => lVar.sArg
                        case None => throw new Exception(cef.missingVarRef(t))
                    }
                case a: Ast if a.isMemberAccess =>
                    // This is a case of accessing something like A.B.C.
                    lookupInEnv(expr.toWomString, env) match {
                        case Some((_, lVar)) => lVar.sArg
                        case None => throw new Exception(cef.missingVarRef(a))
                    }
                case a:Ast =>
                    throw new Exception(cef.notCurrentlySupported(
                                            a,
                                            s"Expressions in output section (${expr.toWomString})"))
            }

            (cVar, sArg)
        }.toVector
    }


    // Compile a WDL task into an applet.
    //
    // Note: check if a task is a real WDL task, or if it is a wrapper for a
    // native applet.
    private def compileTask(task : WdlTask) : IR.Applet = {
        Utils.trace(verbose.on, s"Compiling task ${task.name}")

        // The task inputs are declarations that:
        // 1) are unassigned (do not have an expression)
        // 2) OR, are assigned, but optional
        //
        // According to the WDL specification, in fact, all task declarations
        // are potential inputs. However, that does not make that much sense.
        //
        // if the declaration is set to a constant, we need to make it a default
        // value
        val inputVars : Vector[CVar] =  task.declarations.map{ decl =>
            if (declarationIsInput(decl))  {
                val taskAttrs = DeclAttrs.get(task, decl.unqualifiedName)
                val attrs = decl.expression match {
                    case None => taskAttrs
                    case Some(expr) =>
                        // the constant is a default value
                        if (!Utils.isExpressionConst(expr))
                            throw new Exception(cef.taskInputDefaultMustBeConst(expr))
                        val wdlConst:WomValue = Utils.evalConst(expr)
                        taskAttrs.setDefault(wdlConst)
                }
                Some(CVar(decl.unqualifiedName, decl.womType, attrs, decl.ast))
            } else {
                None
            }
        }.flatten.toVector
        val outputVars : Vector[CVar] = task.outputs.map{ tso =>
            CVar(tso.unqualifiedName, tso.womType, DeclAttrs.empty, tso.ast)
        }.toVector

        // Figure out if we need to use docker
        val docker = task.runtimeAttributes.attrs.get("docker") match {
            case None =>
                IR.DockerImageNone
            case Some(expr) if Utils.isExpressionConst(expr) =>
                val wdlConst = Utils.evalConst(expr)
                wdlConst match {
                    case WomString(url) if url.startsWith(Utils.DX_URL_PREFIX) =>
                        // A constant image specified with a DX URL
                        val dxRecord = DxPath.lookupDxURLRecord(url)
                        IR.DockerImageDxAsset(dxRecord)
                    case _ =>
                        // Probably a public docker image
                        IR.DockerImageNetwork
                }
            case _ =>
                // Image will be downloaded from the network
                IR.DockerImageNetwork
        }
        // The docker container is on the platform, we need to remove
        // the dxURLs in the runtime section, to avoid a runtime
        // lookup. For example:
        //
        //   dx://dxWDL_playground:/glnexus_internal  ->   dx://project-xxxx:record-yyyy
        val taskCleaned = docker match {
            case IR.DockerImageDxAsset(dxRecord) =>
                WdlRewrite.taskReplaceDockerValue(task, dxRecord)
            case _ => task
        }
        val kind =
            (task.meta.get("type"), task.meta.get("id")) match {
                case (Some("native"), Some(id)) =>
                    // wrapper for a native applet
                    IR.AppletKindNative(id)
                case (_,_) =>
                    // a WDL task
                    IR.AppletKindTask
            }
        val applet = IR.Applet(task.name,
                               inputVars,
                               outputVars,
                               calcInstanceType(Some(task)),
                               docker,
                               kind,
                               WdlRewrite.namespace(taskCleaned))
        verifyWdlCodeIsLegal(applet.ns)
        applet
    }

    private def findInputByName(call: WdlCall, cVar: CVar) : Option[(String,WdlExpression)] = {
        call.inputMappings.find{ case (k,v) => k == cVar.name }
    }

    // Can a call be compiled directly to a workflow stage?
    //
    // The requirement is that all call arguments are already in the environment.
    private def canCompileCallDirectly(call: WdlCall,
                                       env : CallEnv) : Boolean = {
        // Find the callee
        val calleeName = Utils.calleeGetName(call)
        val callee = callables(calleeName)

        // Extract the input values/links from the environment
        val envAllKeys: Set[String] = env.keys.toSet
        callee.inputVars.forall{ cVar =>
            findInputByName(call, cVar) match {
                case None =>
                    // unbound variable
                    true
                case Some((_,expr)) if Utils.isExpressionConst(expr) =>
                    // constant expression, there is no need to look
                    // in the environment
                    true
                case Some((_,expr)) =>
                    // check if expression exists in the environment
                    val exprSourceString = expr.toWomString
                    envAllKeys contains exprSourceString
            }
        }
    }

    // compile a call into a stage in an IR.Workflow
    private def compileCall(call: WdlCall,
                            env : CallEnv) : IR.Stage = {
        // Find the callee
        val calleeName = Utils.calleeGetName(call)
        val callee = callables(calleeName)

        // Extract the input values/links from the environment
        val inputs: Vector[SArg] = callee.inputVars.map{ cVar =>
            findInputByName(call, cVar) match {
                case None =>
                    IR.SArgEmpty
                case Some((_,expr)) if Utils.isExpressionConst(expr) =>
                    IR.SArgConst(Utils.evalConst(expr))
                case Some((_,expr)) =>
                    val exprSourceString = expr.toWomString
                    val lVar = env(exprSourceString)
                    lVar.sArg
            }
        }
        val stageName = call.unqualifiedName
        IR.Stage(stageName, None, genStageId(), calleeName, inputs, callee.outputVars)
    }

    // Figure out all the outputs from a sequence of WDL statements.
    //
    // Note: The type outside a block is *different* than the type in
    // the block.  For example, 'Int x' declared inside a scatter, is
    // 'Array[Int] x' outside the scatter.
    private def blockOutputs(statements: Vector[Scope]) : Vector[CVar] = {
        statements.foldLeft(Vector.empty[CVar]) {
            case (accu, call:WdlCall) =>
                val calleeName = Utils.calleeGetName(call)
                val callee = callables(calleeName)
                val callOutputs = callee.outputVars.map { cVar =>
                    val varName = call.unqualifiedName ++ "." ++ cVar.name
                    CVar(varName, cVar.womType, DeclAttrs.empty, cVar.ast)
                }
                accu ++ callOutputs

            case (accu, decl:Declaration) =>
                val cVar = CVar(decl.unqualifiedName, decl.womType,
                                DeclAttrs.empty, decl.ast)
                accu :+ cVar

            case (accu, ssc:Scatter) =>
                // recurse into the scatter, then add an array on top
                // of the types
                val sscOutputs = blockOutputs(ssc.children.toVector)
                val sscOutputs2 =  sscOutputs.map{
                    cVar => cVar.copy(womType = WomArrayType(cVar.womType))
                }
                accu ++ sscOutputs2

            case (accu, ifStmt:If) =>
                // recurse into the if block, and amend the type.
                val ifStmtOutputs = blockOutputs(ifStmt.children.toVector)
                val ifStmtOutputs2 = ifStmtOutputs.map{
                    cVar => cVar.copy(womType = Utils.makeOptional(cVar.womType))
                }
                accu ++ ifStmtOutputs2

            case (accu, other) =>
                throw new Exception(cef.notCurrentlySupported(
                                        other.ast, s"Unimplemented workflow element"))
        }
    }

    // Create a valid WDL workflow that runs a block (Scatter, If,
    // etc.) The main modification is renaming variables of the
    // form A.x to A_x.
    //
    // A workflow must have definitions for all the tasks it
    // calls. However, a scatter calls tasks, that are missing from
    // the WDL file we generate. To ameliorate this, we add stubs for
    // called tasks. The generated tasks are named by their
    // unqualified names, not their fully-qualified names. This works
    // because the WDL workflow must be "flattenable".
    def blockGenWorklow(block: Block,
                        inputVars: Vector[CVar],
                        outputVars: Vector[CVar]) : WdlNamespace = {
        val calls: Vector[WdlCall] = block.findCalls
        val taskStubs: Map[String, WdlTask] =
            calls.foldLeft(Map.empty[String,WdlTask]) { case (accu, call) =>
                val name = Utils.calleeGetName(call)
                val callable = callables.get(name) match {
                    case None => throw new Exception(s"Calling undefined task/workflow ${name}")
                    case Some(x) => x
                }
                if (accu contains name) {
                    // we have already created a stub for this call
                    accu
                } else {
                    // no existing stub, create it
                    val task = genAppletStub(callable)
                    accu + (name -> task)
                }
            }

        // rename usages of the input variables in the statment block
        val wordTranslations = inputVars.map{ cVar =>
            cVar.name -> cVar.dxVarName
        }.toMap
        val erv = VarAnalysis(Set.empty, wordTranslations, cef, verbose)
        val statements2 = block.statements.map{
            case stmt => erv.rename(stmt)
        }.toVector

        val decls: Vector[Declaration]  = inputVars.map{ cVar =>
            WdlRewrite.declaration(cVar.womType, cVar.dxVarName, None)
        }
        val wfOutputs: Vector[WorkflowOutput] = outputVars.map{ cVar =>
            WdlRewrite.workflowOutput(cVar.dxVarName,
                                      cVar.womType,
                                      WdlExpression.fromString(cVar.name))
        }

        // Create new workflow that includes only this block
        val wf = WdlRewrite.workflowGenEmpty("w")
        wf.children = decls ++ statements2 ++ wfOutputs
        val tasks = taskStubs.map{ case (_,x) => x}.toVector
        // namespace that includes the task stubs, and the workflow
        WdlRewrite.namespace(wf, tasks)
    }


    // Create a human readable name for a block of statements
    //
    // 1. Ignore all declarations
    // 2. If there is a scatter/if, use that
    // 3. Otherwise, there must be at least one call. Use the first one.
    private def createBlockName(block: Block) : String = {
        val coreStmts = block.statements.filter(x => !x.isInstanceOf[DeclarationInterface])
        if (coreStmts.isEmpty)
            s"eval_${genFragId()}"
        else
            coreStmts.head match {
                case ssc:Scatter =>
                    s"scatter_${ssc.item}"
                case ifStmt:If =>
                    val condRawString = ifStmt.condition.toWomString
                    // Leave only identifiers
                    val matches: List[Match] = fqnRegex.findAllMatchIn(condRawString).toList
                    val ids = matches.map{ _.toString}
                    // limit the number of identifiers, and concatenate
                    val name = ids.take(4).mkString("_")
                    s"if_${name}"
                case call:WdlCall =>
                    call.unqualifiedName
            }
    }

    private def genBlockDescription(block: Block) : String = {
        val coreStmts = block.statements.filter(x => !x.isInstanceOf[DeclarationInterface])
        if (coreStmts.isEmpty)
            "declarations"
        else
            coreStmts.head match {
                case ssc:Scatter =>
                    s"scatter (${ssc.item} in ${ssc.collection.toWomString})"
                case ifStmt:If =>
                    s"if (${ifStmt.condition.toWomString})"
                case call:WdlCall =>
                    call.unqualifiedName
            }
    }

    // Build an applet to evaluate a WDL workflow fragment
    //
    // Note: all the calls have the compulsory arguments, this has
    // been checked in the Validate step.
    private def compileWfFragment(block: Block,
                                  env : CallEnv) : (IR.Stage, IR.Applet) = {
        val baseName = createBlockName(block)
        val stageName = execNameBox.chooseUniqueName(baseName)
        Utils.trace(verbose.on, s"Compiling wfFragment ${stageName}")

        // Figure out the closure required for this block, out of the
        // environment
        val closure = blockClosure(block.statements, env, stageName)

        // create input variable definitions
        val inputVars: Vector[CVar] = closure.map {
            case (fqn, LinkedVar(cVar, _)) =>
                cVar.copy(name = fqn)
        }.toVector

        val outputVars = blockOutputs(block.statements)
        val wdlCode = blockGenWorklow(block, inputVars, outputVars)
        val callDict = block.findCalls.map{ c =>
            c.unqualifiedName -> Utils.calleeGetName(c)
        }.toMap

        val applet = IR.Applet(appletNamePrefix ++ stageName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindWfFragment(callDict),
                               wdlCode)
        verifyWdlCodeIsLegal(applet.ns)

        val sargs : Vector[SArg] = closure.map {
            case (_, LinkedVar(_, sArg)) => sArg
        }.toVector
        (IR.Stage(stageName,
                  Some(genBlockDescription(block)),
                  genStageId(), applet.name, sargs, outputVars),
         applet)
    }

    // Create an applet to reorganize the output files. We want to
    // move the intermediate results to a subdirectory.  The applet
    // needs to process all the workflow outputs, to find the files
    // that belong to the final results.
    private def createReorgApplet(wfOutputs: Vector[(CVar, SArg)]) : (IR.Stage, IR.Applet) = {
        val appletName = execNameBox.chooseUniqueName("reorg")
        Utils.trace(verbose.on, s"Compiling output reorganization applet ${appletName}")

        val inputVars: Vector[CVar] = wfOutputs.map{ case (cVar, _) => cVar }
        val outputVars= Vector.empty[CVar]
        val inputDecls: Vector[Declaration] = wfOutputs.map{ case(cVar, _) =>
            WdlRewrite.declaration(cVar.womType, cVar.dxVarName, None)
        }.toVector

        // Create a workflow with no calls.
        val code:WdlWorkflow = WdlRewrite.workflowGenEmpty("w")
        code.children = inputDecls

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletNamePrefix ++ appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindWorkflowOutputReorg,
                               WdlRewrite.namespace(code, Seq.empty))
        verifyWdlCodeIsLegal(applet.ns)

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = wfOutputs.map{ case (_, sArg) => sArg }.toVector

        (IR.Stage(Utils.REORG, None, genStageId(), applet.name, inputs, outputVars),
         applet)
    }

    // Represent the workflow inputs with CVars.
    // It is possible to provide a default value to a workflow input.
    // For example:
    // workflow w {
    //   Int? x = 3
    //   ...
    // }
    // We handle only the case where the default is a constant.
    def buildWorkflowInputs(wfInputDecls: Seq[Declaration]) : Vector[(CVar,SArg)] = {
        wfInputDecls.map{
            case decl:Declaration =>
                val cVar = CVar(decl.unqualifiedName, decl.womType, DeclAttrs.empty, decl.ast)
                decl.expression match {
                    case None =>
                        // A workflow input
                        (cVar, IR.SArgWorkflowInput(cVar))
                    case Some(expr) =>
                        // the constant is a default value
                        if (!Utils.isExpressionConst(expr))
                            throw new Exception(cef.workflowInputDefaultMustBeConst(expr))
                        val wdlConst:WomValue = Utils.evalConst(expr)
                        val attrs = DeclAttrs.empty.setDefault(wdlConst)
                        val cVarWithDflt = CVar(decl.unqualifiedName, decl.womType,
                                                attrs, decl.ast)
                        (cVarWithDflt, IR.SArgWorkflowInput(cVar))
                }
        }.toVector
    }

    // Is this a very simple block, that can be compiled directly
    // to a stage
    private def isSingletonBlock(block: Block, env: CallEnv) : Boolean = {
        if (block.size == 1 &&
                block.head.isInstanceOf[WdlCall]) {
            val call: WdlCall = block.head.asInstanceOf[WdlCall]
            canCompileCallDirectly(call, env)
        } else
            false
    }

    private def buildWorkflowBackbone(
        wf: WdlWorkflow,
        subBlocks: Vector[Block],
        accu: Vector[(IR.Stage, Option[IR.Applet])],
        env_i: CallEnv)
            : (Vector[(IR.Stage, Option[IR.Applet])], CallEnv) = {
        var env = env_i

        val allStageInfo = subBlocks.foldLeft(accu) {
            case (accu, block) if isSingletonBlock(block, env) =>
                // The block contains exactly one call, with no extra declarations.
                // All the variables are already in the environment, so there
                // is no need to do any extra work. Compile directly into a workflow
                // stage.
                val call = block.head.asInstanceOf[WdlCall]
                val stage = compileCall(call, env)

                // Add bindings for the output variables. This allows later calls to refer
                // to these results.
                for (cVar <- stage.outputs) {
                    env = env + (call.unqualifiedName ++ "." + cVar.name ->
                                     LinkedVar(cVar, IR.SArgLink(stage.stageName, cVar)))
                }
                accu :+ (stage, None)

            case (accu, block) =>
                // General case
                val (stage, apl) = compileWfFragment(block, env)
                for (cVar <- stage.outputs) {
                    env = env + (cVar.name ->
                                     LinkedVar(cVar, IR.SArgLink(stage.stageName, cVar)))
                }
                accu :+ (stage, Some(apl))
        }
        (allStageInfo, env)
    }


    // Create a preliminary applet to handle workflow input/outputs. This is
    // used only in the absence of workflow-level inputs/outputs.
    def compileCommonApplet(inputs: Vector[(CVar, SArg)]) : (IR.Stage, IR.Applet) = {
        val appletName = execNameBox.chooseUniqueName(Utils.COMMON)
        Utils.trace(verbose.on, s"Compiling common applet ${appletName}")

        val inputVars : Vector[CVar] = inputs.map{ case (cVar, _) => cVar }
        val outputVars: Vector[CVar] = inputVars
        val declarations: Seq[Declaration] = inputs.map { case (cVar,_) =>
            WdlRewrite.declaration(cVar.womType, cVar.name, None)
        }
        val wfOutputs: Vector[WorkflowOutput] = outputVars.map{ cVar =>
            WdlRewrite.workflowOutput(cVar.dxVarName,
                                      cVar.womType,
                                      WdlExpression.fromString(cVar.name))
        }

        val wf = WdlRewrite.workflowGenEmpty("w")
        wf.children = declarations ++ wfOutputs
        val wdlCode = WdlRewrite.namespace(wf, Vector.empty)

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletNamePrefix ++ appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindWfFragment(Map.empty),
                               wdlCode)
        verifyWdlCodeIsLegal(applet.ns)

        val sArgs: Vector[SArg] = inputs.map{ _ => IR.SArgEmpty}.toVector
        (IR.Stage(Utils.COMMON, None, genStageId(), applet.name, sArgs, outputVars),
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
    def compileOutputSection(wfOutputs: Vector[(CVar, SArg)],
                             outputDecls: Seq[WorkflowOutput]) : (IR.Stage, IR.Applet) = {
        val appletName = execNameBox.chooseUniqueName(Utils.OUTPUT_SECTION)
        Utils.trace(verbose.on, s"Compiling output section applet ${appletName}")

        val inputVars: Vector[CVar] = wfOutputs.map{ case (cVar,_) => cVar }.toVector
        val inputDecls: Vector[Declaration] = wfOutputs.map{ case(cVar, _) =>
            WdlRewrite.declaration(cVar.womType, cVar.dxVarName, None)
        }.toVector

        // Workflow outputs
        val outputPairs: Vector[(WorkflowOutput, CVar)] = outputDecls.map { wot =>
            val cVar = CVar(wot.unqualifiedName, wot.womType, DeclAttrs.empty, wot.ast)
            val dxVarName = Utils.transformVarName(wot.unqualifiedName)
            val dxWot = WdlRewrite.workflowOutput(dxVarName,
                                                  wot.womType,
                                                  WdlExpression.fromString(dxVarName))
            (dxWot, cVar)
        }.toVector
        val (outputs, outputVars) = outputPairs.unzip

        // Create a workflow with no calls.
        val code:WdlWorkflow = WdlRewrite.workflowGenEmpty("w")
        code.children = inputDecls ++ outputs

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletNamePrefix ++ appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindWfFragment(Map.empty),
                               WdlRewrite.namespace(code, Seq.empty))
        verifyWdlCodeIsLegal(applet.ns)

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = wfOutputs.map{ case (_, sArg) => sArg }.toVector

        (IR.Stage(Utils.OUTPUT_SECTION,
                  None,
                  genStageId(Some(Utils.LAST_STAGE)),
                  applet.name,
                  inputs,
                  outputVars),
         applet)
    }

    // Compile a workflow, having compiled the independent tasks.
    private def compileWorkflowLocked(wf: WdlWorkflow,
                                      wfInputs: Vector[(CVar, SArg)],
                                      subBlocks: Vector[Block]) :
            (Vector[(IR.Stage, Option[IR.Applet])], Vector[(CVar, SArg)]) = {
        Utils.trace(verbose2, s"Compiling locked-down workflow ${wf.unqualifiedName}")

        // Locked-down workflow, we have workflow level inputs and outputs
        val initEnv : CallEnv = wfInputs.map { case (cVar,sArg) =>
            cVar.name -> LinkedVar(cVar, sArg)
        }.toMap
        val stageAccu = Vector.empty[(IR.Stage, Option[IR.Applet])]

        // link together all the stages into a linear workflow
        val (allStageInfo, env) = buildWorkflowBackbone(wf, subBlocks, stageAccu, initEnv)
        val wfOutputs: Vector[(CVar, SArg)] = prepareOutputSection(env, wf.outputs)
        (allStageInfo, wfOutputs)
    }

    // Compile a workflow, having compiled the independent tasks.
    private def compileWorkflowRegular(wf: WdlWorkflow,
                                       wfInputs: Vector[(CVar, SArg)],
                                       subBlocks: Vector[Block]) :
            (Vector[(IR.Stage, Option[IR.Applet])], Vector[(CVar, SArg)]) = {
        Utils.trace(verbose2, s"Compiling regular workflow ${wf.unqualifiedName}")

        // Create a preliminary stage to handle workflow inputs, and top-level
        // declarations.
        val (inputStage, inputApplet) = compileCommonApplet(wfInputs)

        // An environment where variables are defined
        val initEnv : CallEnv = inputStage.outputs.map { cVar =>
            cVar.name -> LinkedVar(cVar, IR.SArgLink(inputStage.stageName, cVar))
        }.toMap

        val initAccu : (Vector[(IR.Stage, Option[IR.Applet])]) =
            (Vector((inputStage, Some(inputApplet))))

        // link together all the stages into a linear workflow
        val (allStageInfo, env) = buildWorkflowBackbone(wf, subBlocks, initAccu, initEnv)
        val wfOutputs: Vector[(CVar, SArg)] = prepareOutputSection(env, wf.outputs)

        // output section is non empty, keep only those files
        // at the destination directory
        val (outputStage, outputApplet) = compileOutputSection(wfOutputs, wf.outputs)

        (allStageInfo :+ (outputStage, Some(outputApplet)),
         wfOutputs)
    }

    // Compile a workflow, having compiled the independent tasks.
    def compileWorkflow(wf: WdlWorkflow)
            : (IR.Workflow, Map[String, IR.Applet]) = {
        Utils.trace(verbose.on, s"compiling workflow ${wf.unqualifiedName}")

        // Get rid of workflow output declarations
        val children = wf.children.filter(x => !x.isInstanceOf[WorkflowOutput])

        // Only a subset of the workflow declarations are considered inputs.
        // Limit the search to the top block of declarations. Those that come at the very
        // beginning of the workflow.
        val (wfInputDecls, wfProper) = children.toList.partition{
            case decl:Declaration => declarationIsInput(decl)
            case _ => false
        }
        val wfInputs:Vector[(CVar, SArg)] = buildWorkflowInputs(
            wfInputDecls.map{ _.asInstanceOf[Declaration]}
        )

        // Create a stage per call/scatter-block/declaration-block
        val subBlocks = Block.splitIntoBlocks(wfProper.toVector)

        val (allStageInfo_i, wfOutputs) =
            if (locked)
                compileWorkflowLocked(wf, wfInputs, subBlocks)
            else
                compileWorkflowRegular(wf, wfInputs, subBlocks)

        // Add a reorganization applet if requested
        val allStageInfo =
            if (reorg) {
                val (rStage, rApl) = createReorgApplet(wfOutputs)
                allStageInfo_i :+ (rStage, Some(rApl))
            } else {
                allStageInfo_i
            }

        val (stages, auxApplets) = allStageInfo.unzip
        val aApplets: Map[String, IR.Applet] =
            auxApplets
                .flatten
                .map(apl => apl.name -> apl).toMap

        val irwf = IR.Workflow(wf.unqualifiedName, wfInputs, wfOutputs, stages, locked)
        execNameBox.add(wf.unqualifiedName)
        (irwf, aApplets)
    }
}


object GenerateIR {
    // The applets and subworkflows are combined into noe big map. Split
    // it, and return a namespace.
    private def makeNamespace(name: String,
                              entrypoint: Option[IR.Workflow],
                              callables: Map[String, IR.Callable]) : IR.Namespace  = {
        val subWorkflows = callables
            .filter{case (name, exec) => exec.isInstanceOf[IR.Workflow]}
            .map{case (name, exec) => name -> exec.asInstanceOf[IR.Workflow]}.toMap
        val applets = callables
            .filter{case (name, exec) => exec.isInstanceOf[IR.Applet]}
            .map{case (name, exec) => name -> exec.asInstanceOf[IR.Applet]}.toMap
        IR.Namespace(name, entrypoint, subWorkflows, applets)
    }

    // Recurse into the the namespace, assuming that all subWorkflows, and applets
    // have already been compiled.
    private def applyRec(nsTree : NamespaceOps.Tree,
                         callables: Map[String, IR.Callable],
                         reorg: Boolean,
                         locked: Boolean,
                         execNameBox: NameBox,
                         verbose: Verbose) : IR.Namespace = {
        // recursively generate IR for the entire tree
        val nsTree1 = nsTree match {
            case NamespaceOps.TreeLeaf(name, cef, tasks) =>
                // compile all the [tasks], that have not been already compiled, to IR.Applet
                val gir = new GenerateIR(Map.empty, reorg, locked, NamespaceOps.Kind.TopLevel,
                                         "", execNameBox, nsTree.cef, verbose)
                val alreadyCompiledNames: Set[String] = callables.keys.toSet
                val tasksNotCompiled = tasks.filter{
                    case (taskName,_) =>
                        !(alreadyCompiledNames contains taskName)
                }
                val taskApplets = tasksNotCompiled.map{ case (_,task) =>
                    val applet = gir.compileTask(task)
                    task.name -> applet
                }.toMap

                val allCallables = callables ++ taskApplets
                //Utils.trace(verbose.on, s"leaf: callables = ${allCallables.keys}")
                makeNamespace(name, None, allCallables)

            case NamespaceOps.TreeNode(name, cef, _, workflow, children, blockKind, true, originalWorkflowName) =>
                // Recurse into the children.
                //
                // The reorg and locked flags only apply to the top level
                // workflow. All other workflows are sub-workflows, and they do
                // not reorganize the outputs.
                val childCallables = children.foldLeft(callables){
                    case (accu, child) =>
                        val childIr = applyRec(child, accu, false, true, execNameBox, verbose)
                        accu ++ childIr.buildCallables
                }
                //Utils.trace(verbose.on, s"node: ${childCallables.keys}")
                val locked2 =
                    if (blockKind != NamespaceOps.Kind.TopLevel) true
                    else locked
                val gir = new GenerateIR(childCallables, reorg, locked2, blockKind,
                                         s"${originalWorkflowName}_", execNameBox, cef, verbose)
                val (irWf, auxApplets) = gir.compileWorkflow(workflow)
                val allCallables = childCallables ++ auxApplets
                //Utils.trace(verbose.on, s"node: callables = ${allCallables.keys}")
                makeNamespace(name, Some(irWf), allCallables)

            case NamespaceOps.TreeNode(_, _, _, workflow, _, _, false, _) =>
                val wdlCode = WdlPrettyPrinter(false, None).apply(workflow, 0).mkString("\n")
                System.err.println(s"""|=== Workflow is not fully reduced ===
                                       |${wdlCode}
                                       |""".stripMargin)
                throw new Exception("Internal error, tree is not fully reduced")
        }
        nsTree1
    }

    // Entrypoint
    def apply(nsTree : NamespaceOps.Tree,
              reorg: Boolean,
              locked: Boolean,
              verbose: Verbose) : IR.Namespace = {
        Utils.trace(verbose.on, s"IR pass")
        val execNameBox = new NameBox(verbose)
        applyRec(nsTree, Map.empty, reorg, locked, execNameBox, verbose)
    }
}
