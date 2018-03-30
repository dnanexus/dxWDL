/** Generate intermediate representation from a WDL namespace.
  */
package dxWDL.compiler

import dxWDL.{CompilerErrorFormatter, DeclAttrs, DxPath, InstanceTypeDB, Verbose, WdlPrettyPrinter}
import dxWDL.Utils
import IR.{CVar, LinkedVar, SArg}
import scala.util.{Failure, Success, Try}
import wdl._
import wdl.AstTools
import wdl.AstTools.EnhancedAstNode
import wdl.expression._
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl.WdlExpression.AstForExpressions
import wom.types._
import wom.values._

case class GenerateIR(callables: Map[String, IR.Callable],
                      cef: CompilerErrorFormatter,
                      reorg: Boolean,
                      locked: Boolean,
                      verbose: Verbose) {
    private val verbose2:Boolean = verbose.keywords contains "CompilerIR"

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

    // Lookup for variables like x, A.x, A.B.x. The difficulty
    // is that A.x is interpreted in WDL as member access.
    private def lookupInEnv(env: CallEnv, expr: WdlExpression) : LinkedVar = {
        val fqn: String = expr.toWomString
        env.get(fqn) match {
            case Some(x) => x
            case None =>
                val t: Terminal = AstTools.findTerminals(expr.ast).head
                throw new Exception(cef.missingVarRef(t))
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
        Utils.trace(verbose.on,
                    s"""|genAppletStub  callable=${callable.name}
                        |inputs= ${callable.inputVars.map(_.name)}
                        |outputs= ${callable.outputVars.map(_.name)}"""
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

    // Rename member accesses inside an expression, from the form A.x
    // to A_x. This is used inside an applet of WDL generated code.
    //
    // Here, we take a shortcut, and just replace strings, instead of
    // doing a recursive syntax analysis (see ValueEvaluator wdl4s
    // module).
    private def exprRenameVars(expr: WdlExpression,
                               allVars: Vector[CVar]) : WdlExpression = {
        var sExpr: String = expr.toWomString
        for (cVar <- allVars) {
            // A.x => A_x
            sExpr = sExpr.replaceAll(cVar.name, cVar.dxVarName)
        }
        WdlExpression.fromString(sExpr)
    }

    // Here, we use the flat namespace assumption. We use
    // unqualified names as Fully-Qualified-Names, because
    // task and workflow names are unique.
    private def calleeGetName(call: WdlCall) : String = {
        call match {
            case tc: WdlTaskCall =>
                tc.task.unqualifiedName
            case wfc: WdlWorkflowCall =>
                wfc.calledWorkflow.unqualifiedName
        }
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

    /*
     Split a block of statements into sub-blocks, each of which contains:
     1) at most one call
     2) at most one toplevel if/scatter block
     Note that the if/scatter block could be deeply nested. Previous compiler passes
     must make sure that there is no more a single call inside an if/scatter block.

     For example:
     call x
     Int a
     Int b
     call y
     call z
     String buf
     Float x
     scatter {
       call V
     }

     =>

     [call x, Int a, Int b]
     [call y]
     [call z]
     [String buf, Float x, scatter]
     */
    case class Block(statements: Vector[Scope]) {
        def append(stmt: Scope) : Block =
            Block(statements :+ stmt)

        // Check if one of the statements is a scatter/if block
        def hasSubBlock : Boolean = {
            statements.foldLeft(false) {
                case (accu, _:Scatter) => true
                case (accu, _:If) => true
                case (accu, _) => accu
            }
        }
    }

    // Find all the calls inside a statement block
    private def findCalls(statments: Vector[Scope]) : Vector[WdlCall] = {
        statements.foldLeft(Vector.empty[WdlCall]) {
            case (accu, call:WdlCall) =>
                accu :+ call
            case (accu, ssc:Scatter) =>
                accu ++ findCalls(ssc.children)
            case ifStmt:If =>
                accu ++ findCalls(ifStmt.children)
            case x =>
                accu
        }
    }

    // Count how many calls (task or workflow) there are in a series
    // of statements.
    private def countCalls(statements: Vector[Scope]) : Int = {
        findCalls(statments).length
    }

    private def splitIntoBlocks(children: Vector[Scope]) : Vector[Block] = {
        // base cases: zero and one children
        if (children.isEmpty)
            return Vector.empty
        if (children.length == 1)
            return Vector(Block(children))

        // Normal case, recurse into the first N-1 statements,
        // than append the Nth.
        val blocks = splitIntoBlocks(children.dropRight(1))
        val allBlocksButLast = blocks.dropRight(1)

        val lastBlock = blocks.last
        val lastChild = children.last
        val trailing: Vector[Block] = (countCalls(lastBlock), countCalls(lsatChild)) match {
            case (0, (0|1)) => Vector(lastBlock.append(lastChild))
            case (1, 0) =>
                if (lastBlock.hasSubBlock) {
                    // Existing block already includes an if/scatter block.
                    // Don't merge
                    Vector(lastBlock, Block(lastChild))
                } else {
                    // We can merge the child into the existing block
                    Vector(lastBlock.append(lastChild))
                }
            case (1, 1) => Vector(lastBlock, Block(lastChild))
            case (x,y) =>
                if (x > 1) {
                    Sytem.err.println("GenerateIR, block:")
                    Sytem.err.println(lastBlock)
                    throw new Exception(s"block has ${x} calls")
                }
                if (y > 1) {
                    Sytem.err.println("GenerateIR, child:")
                    Sytem.err.println(lastChild)
                    throw new Exception(s"child has ${y} calls")
                }
        }
        allBlocksButLast ++ trailing
    }


    // Check if the environment has A.B.C, A.B, or A.
    private def trailSearch(env: CallEnv, ast: Ast) : Option[(String, LinkedVar)] = {
        val exprStr = WdlExpression.toString(ast)
        env.get(exprStr) match {
            case None if !ast.isMemberAccess =>
                None
            case None =>
                ast.getAttribute("lhs") match {
                    case t: Terminal =>
                        val srcStr = t.getSourceString
                        t.getTerminalStr match {
                            case "identifier" =>
                                env.get(srcStr) match {
                                    case Some(lVar) => Some(srcStr, lVar)
                                    case None => None
                                }
                            case _ =>
                                throw new Exception(s"Terminal `${srcStr}` is not an identifier")
                        }
                    case lhs: Ast => trailSearch(env, lhs)
                    case _ => None
                }
            case Some(lVar) =>
                Some(exprStr, lVar)
        }
    }

    // Figure out what an expression depends on from the environment
    private def envDeps(env : CallEnv,
                        aNode : AstNode) : Set[String] = {
        // recurse into list of subexpressions
        def subExpr(exprVec: Seq[AstNode]) : Set[String] = {
            val setList = exprVec.map(a => envDeps(env, a))
            setList.foldLeft(Set.empty[String]){case (accu, st) =>
                accu ++ st
            }
        }
        aNode match {
            case t: Terminal =>
                val srcStr = t.getSourceString
                t.getTerminalStr match {
                    case "identifier" =>
                        env.get(srcStr) match {
                            case Some(lVar) => Set(srcStr)
                            case None => Set.empty
                        }
                    case "string" if Utils.isInterpolation(srcStr) =>
                        // From a string like: "Tamara likes ${fruit}s and ${ice}s"
                        // extract the variables {fruit, ice}. In general, they
                        // could be full fledged expressions
                        //
                        // from: wdl4s.wdl.expression.ValueEvaluator.InterpolationTagPattern
                        val iPattern = "\\$\\{\\s*([^\\}]*)\\s*\\}".r
                        val subExprRefs: List[String] = iPattern.findAllIn(srcStr).toList
                        val subAsts:List[AstNode] = subExprRefs.map{ tag =>
                            // ${fruit} ---> fruit
                            val expr = WdlExpression.fromString(tag.substring(2, tag.length - 1))
                            expr.ast
                        }
                        subExpr(subAsts)
                    case _ =>
                        // A literal
                        Set.empty
                }
            case a: Ast if a.isMemberAccess =>
                // This is a case of accessing something like A.B.C.
                trailSearch(env, a) match {
                    case Some((varName, _)) => Set(varName)
                    case None =>
                        // The variable is declared locally, it is not
                        // passed from the outside.
                        Set.empty
                }
            case a: Ast if a.isBinaryOperator =>
                val lhs = a.getAttribute("lhs")
                val rhs = a.getAttribute("rhs")
                subExpr(List(lhs, rhs))
            case a: Ast if a.isUnaryOperator =>
                val expression = a.getAttribute("expression")
                envDeps(env, expression)
            case TernaryIf(condition, ifTrue, ifFalse) =>
                subExpr(List(condition, ifTrue, ifFalse))
            case a: Ast if a.isArrayLiteral =>
                subExpr(a.getAttribute("values").astListAsVector)
            case a: Ast if a.isTupleLiteral =>
                subExpr(a.getAttribute("values").astListAsVector)
            case a: Ast if a.isMapLiteral =>
                val kvMap = a.getAttribute("map").astListAsVector.map { kv =>
                    val key = kv.asInstanceOf[Ast].getAttribute("key")
                    val value = kv.asInstanceOf[Ast].getAttribute("value")
                    key -> value
                }.toMap
                val elems: Vector[AstNode] = kvMap.keys.toVector ++ kvMap.values.toVector
                subExpr(elems)
            case a: Ast if a.isArrayOrMapLookup =>
                val index = a.getAttribute("rhs")
                val mapOrArray = a.getAttribute("lhs")
                subExpr(List(index, mapOrArray))
            case a: Ast if a.isFunctionCall =>
                subExpr(a.params)
            case _ =>
                throw new Exception(s"unhandled expression ${aNode}")
        }
    }

    // Update a closure with all the variables required
    // for an expression. Ignore variable accesses outside the environment;
    // it is assumed that these are accesses to local variables.
    //
    // @param  closure   call closure
    // @param  env       mapping from fully qualified WDL name to a dxlink
    // @param  expr      expression as it appears in source WDL
    private def updateClosure(closure : CallEnv,
                              env : CallEnv,
                              expr : WdlExpression) : CallEnv = {
        val deps:Set[String] = envDeps(env, expr.ast)
        Utils.trace(verbose2, s"updateClosure deps=${deps}  expr=${expr.toWomString}")
        closure ++ deps.map{ v => v -> env(v) }
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

            // we only want to deal with expressions that do
            // not require calculation.
            val expr = wot.requiredExpression
            val sArg = expr.ast match {
                case t: Terminal =>
                    val srcStr = t.getSourceString
                    t.getTerminalStr match {
                        case "identifier" =>
                            env.get(srcStr) match {
                                case Some(lVar) => lVar.sArg
                                case None => throw new Exception(cef.missingVarRef(t))
                            }
                        case _ => throw new Exception(cef.missingVarRef(t))
                    }

                case a: Ast if a.isMemberAccess =>
                    // This is a case of accessing something like A.B.C.
                    trailSearch(env, a) match {
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


    // Check if a task is a real WDL task, or if it is a wrapper for a
    // native applet.

    // Compile a WDL task into an applet
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
            if (Utils.declarationIsInput(decl))  {
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

    // validate that the call is providing all the necessary arguments
    // to the callee (task/workflow)
    private def validateCall(call: WdlCall) : Unit = {
        // Find the callee
        val calleeName = calleeGetName(call)
        val callee = callables(calleeName)
        val inputVarNames = callee.inputVars.map(_.name).toVector
        Utils.trace(verbose2, s"callee=${calleeName}  inputVars=${inputVarNames}")

        callee.inputVars.map{ cVar =>
            findInputByName(call, cVar) match {
                case None if (!Utils.isOptional(cVar.womType) && cVar.attrs.getDefault == None) =>
                    // A missing compulsory input, without a default
                    // value. In an unlocked workflow it can be
                    // provided as an input. In a locked workflow,
                    // that is not possible.
                    val msg = s"""|Workflow doesn't supply required input ${cVar.name}
                                  |to call ${call.unqualifiedName}
                                  |""".stripMargin.replaceAll("\n", " ")
                    if (locked) {
                        throw new Exception(cef.missingCallArgument(call.ast, msg))
                    } else {
                        Utils.warning(verbose, msg)
                    }
                case _ => ()
            }
        }
    }


    // Figure out the closure for a block, and then build the input
    // definitions.
    //
    private def blockInputs(statements: Vector[Scope],
                            env : CallEnv) : (Map[String, LinkedVar], Vector[CVar]) = {
        var closure = Map.empty[String, LinkedVar]
        statements.foreach {
            case decl:Declaration =>
                decl.expression match {
                    case Some(expr) =>
                        closure = updateClosure(closure, env, expr)
                    case None => ()
                }
            case call:WdlCall =>
                call.inputMappings.foreach { case (_, expr) =>
                    closure = updateClosure(closure, env, expr)
                }
            case ssc:Scatter =>
                closure = updateClosure(closure, env, ssc.collection)
            case ifStmt:If =>
                closure = updateClosure(closure, env, ifStmt.condition)
            case _ => ()
        }
        val inputVars: Vector[CVar] = closure.map {
            case (varName, LinkedVar(cVar, _)) =>
                // a variable that must be passed to the scatter applet
                assert(env contains varName)
                Some(CVar(varName, cVar.womType, DeclAttrs.empty, cVar.ast))
        }.flatten.toVector

        (closure, inputVars)
    }

    // figure out if a variable is used only inside
    // the block (scatter, if, ...)
    private def isLocal(decl: Declaration): Boolean = {
        if (Utils.isGeneratedVar(decl.unqualifiedName)) {
            // A variable generated by the compiler. It might be used only
            // locally.
            // find all dependent nodes
            val dNodes:Set[WdlGraphNode] = decl.downstream
            val declParent:Scope = decl.parent.get

            // figure out if these downstream nodes are in the same scope.
            val dnScopes:Set[WdlGraphNode] = dNodes.filter{ node =>
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
    //   'Array[Int] x' outside the scatter.
    private def blockOutputs(statements: Vector[Scope]) : Vector[CVar] = {
        def outsideType(t: WomType) : WomType = {
            scope match {
                case _:Scatter => WomArrayType(t)
                case _:If => t match {
                    // If the type is already optional, don't make it
                    // double optional.
                    case WomOptionalType(_) => t
                    case _ => WomOptionalType(t)
                }
                case _ => t
            }
        }

        statements.foldLeft(Vector.empty[CVar]) {
            case (accu, call:WdlCall) =>
                val calleeName = calleeGetName(call)
                val callee = callables(calleeName)
                val callOutputs = callee.outputVars.map { cVar =>
                    val varName = call.unqualifiedName ++ "." ++ cVar.name
                    CVar(varName, cVar.womType, DeclAttrs.empty, cVar.ast)
                }
                accu ++ callOutputs

            case (accu, decl:Declaration) if !isLocal(decl) =>
                val cVar = CVar(decl.unqualifiedName, decl.womType,
                                DeclAttrs.empty, decl.ast)
                accu :+ cVar

            case (accu, decl:Declaration) =>
                // local variable, do not export
                accu

            case (accu, ssc:Scatter) =>
                // recurse into the scatter, then add an array on top
                // of the types
                val sscOutputs = blockOutputs(ssc)
                val sscOutputs2 =  sscOutputs.map{
                    cVar => cVar.copy(womType = outsideType(cVar.womType))
                }
                accu ++ sscOutputs2

            case ifStmt:If =>
                // recurse into the if block, and amend the type.
                val ifStmtOutputs = blockOutputs(ifStmt)
                val ifStmtOutputs2 = ifStmtOutputs.map{
                    cVar => cVar.copy(womType = outsideType(cVar.womType))
                }
                accu ++ ifStmtOutputs2

            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast, s"Unimplemented scatter element"))
        }.flatten.toVector
        outputVars
    }

    // Modify all the expressions used inside a block
    def blockTransform(statements: Vector[Scope],
                       inputVars: Vector[CVar]) : (Vector[Scope]) = {
        // Rename the variables we got from the input.
        def transform(expr: WdlExpression) : WdlExpression =
            exprRenameVars(expr, inputVars)

        statements.foldLeft(Vector.empty[Scope]) {
            case (accu, tc:WdlTaskCall) =>
                val inputs = tc.inputMappings.map{ case (k,expr) => (k, transform(expr)) }.toMap
                val stmt2 = WdlRewrite.taskCall(tc, inputs)
                accu :+ stmt2

            case (accu, wfc:WdlWorkflowCall) =>
                val inputs = wfc.inputMappings.map{ case (k,expr) => (k, transform(expr)) }.toMap
                val stmt2 = WdlRewrite.workflowCall(wfc, inputs)
                accu :+ stmt2

            case (accu, decl:Declaration) =>
                val decl2 = new Declaration(decl.womType, decl.unqualifiedName,
                                            decl.expression.map(transform), decl.parent,
                                            decl.ast)
                accu :+ decl2

            case (accu, ssc:Scatter) =>
                // recurse into the scatter
                val children = blockTransform(ssc.children, inputVars)
                val ssc2 = WdlRewrite.scatter(ssc, children, transform(ssc.collection))
                accu :+ ssc2

            case ifStmt:If =>
                // recurse into the if block
                val children = blockTransform(ifStmt.children, inputVars)
                val ifStmt2 = WdlRewrite.cond(cond, children, transform(cond.condition))
                accu :+ ifStmt2

            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast, s"Unimplemented scatter element"))
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
    def blockGenWorklow(statements: Vector[Scope],
                        inputVars: Vector[CVar],
                        outputVars: Vector[CVar]) : WdlNamespace = {
        val calls: Vector[WdlCall] = findCalls(statement)
        val taskStubs: Map[String, WdlTask] =
            calls.foldLeft(Map.empty[String,WdlTask]) { case (accu, call) =>
                val name = calleeGetName(call)
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
        val statements2 = blockTransform(statements, inputVars)
        val decls: Vector[Declaration]  = inputVars.map{ cVar =>
            WdlRewrite.declaration(cVar.womType, cVar.dxVarName, None)
        }

        // Create new workflow that includes only this block
        val wf = WdlRewrite.workflowGenEmpty("w")
        wf.children = decls ++ statements2
        val tasks = taskStubs.map{ case (_,x) => x}.toVector
        // namespace that includes the task stubs, and the workflow
        WdlRewrite.namespace(wf, tasks)
    }


    // Build an applet to evaluate a WDL workflow fragment
    private def compileWfFragment(wfUnqualifiedName : String,
                                  stageName: String,
                                  statements: Vector[Scope],
                                  env : CallEnv) : (IR.Stage, IR.Applet) = {
        Utils.trace(verbose.on, s"Compiling wfFragment ${stageName}")

        // validate all the calls -- this should be moved to a
        // separate module. Preferably, check in the validate step.
        val calls = findCalls(statments)
        calls.foreach{ validateCall(_) }

        // Figure out the input definitions
        val (closure, inputVars) = blockInputs(statements, env)
        val outputVars = blockOutputs(statements)
        val wdlCode = blockGenWorklow(statements, inputVars, outputVars)
        val callDict = calls.map{ c =>
            c.unqualifiedName -> calleeGetName(c)
        }.toMap

        val applet = IR.Applet(wfUnqualifiedName ++ "_" ++ stageName,
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
        (IR.Stage(stageName, genStageId(), applet.name, sargs, outputVars),
         applet)
    }

    // Create an applet to reorganize the output files. We want to
    // move the intermediate results to a subdirectory.  The applet
    // needs to process all the workflow outputs, to find the files
    // that belong to the final results.
    private def createReorgApplet(wfUnqualifiedName: String,
                                  wfOutputs: Vector[(CVar, SArg)]) : (IR.Stage, IR.Applet) = {
        val appletName = wfUnqualifiedName ++ "_reorg"
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
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindWorkflowOutputReorg,
                               WdlRewrite.namespace(code, Seq.empty))
        verifyWdlCodeIsLegal(applet.ns)

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = wfOutputs.map{ case (_, sArg) => sArg }.toVector

        (IR.Stage(Utils.REORG, genStageId(), appletName, inputs, outputVars),
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

    // create a human readable name for a stage. If there
    // are no calls, use the iteration expression. Otherwise,
    // concatenate call names, while limiting total string length.
    private def humanReadableStageName(prefix: String,
                                       statements: Vector[Scope]) : String = {
        val calls: Vector[WdlCall] = findCalls(statements)
        val cName =
            if (calls.isEmpty) {
                prefix
            } else {
                "${prefix}_${calls.head.unqualifiedName}"
            }
        if (cName.length > Utils.MAX_STAGE_NAME_LEN)
            cName.substring(0, Utils.MAX_STAGE_NAME_LEN)
        else
            cName
    }

    private def buildWorkflowBackbone(
        wf: WdlWorkflow,
        subBlocks: Vector[Block],
        accu: Vector[(IR.Stage, Option[IR.Applet])],
        env_i: CallEnv)
            : (Vector[(IR.Stage, Option[IR.Applet])], CallEnv) = {
        var env = env_i

        val allStageInfo = subBlocks.foldLeft(accu) {
            (accu, Block(statements)) =>
            val stageName = humanReadableStageName("WfFragment", statements)
            val (stage, applet) = compileScatter(wf.unqualifiedName, stageName, preDecls,
                                                 scatter, env)

            // Add bindings for the output variables. This allows later calls to refer
            // to these results. In case of scatters, there is no block name to reference.
            for (cVar <- stage.outputs) {
                val fqVarName : String = child match {
                    case BlockDecl(decls) => cVar.name
                    case BlockIf(_, _) => cVar.name
                    case BlockScatter(_, _) => cVar.name
                    case BlockScope(call : WdlCall) => stage.name ++ "." ++ cVar.name
                    case _ => throw new Exception("Sanity")
                }
                env = env + (fqVarName ->
                                 LinkedVar(cVar, IR.SArgLink(stage.name, cVar)))
            }
            accu :+ (stage,appletOpt)
        }
        (allStageInfo, env)
    }


    // Create a preliminary applet to handle workflow input/outputs. This is
    // used only in the absence of workflow-level inputs/outputs.
    def compileCommonApplet(wf: WdlWorkflow,
                            inputs: Vector[(CVar, SArg)]) : (IR.Stage, IR.Applet) = {
        val appletName = wf.unqualifiedName ++ "_" ++ Utils.COMMON
        Utils.trace(verbose.on, s"Compiling common applet ${appletName}")

        val inputVars : Vector[CVar] = inputs.map{ case (cVar, _) => cVar }
        val outputVars: Vector[CVar] = inputVars
        val declarations: Seq[Declaration] = inputs.map { case (cVar,_) =>
            WdlRewrite.declaration(cVar.womType, cVar.name, None)
        }
        val code:WdlWorkflow = genEvalWorkflowFromDeclarations(appletName,
                                                               declarations,
                                                               outputVars)

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindEval,
                               WdlRewrite.namespace(code, Seq.empty))
        verifyWdlCodeIsLegal(applet.ns)

        val sArgs: Vector[SArg] = inputs.map{ _ => IR.SArgEmpty}.toVector
        (IR.Stage(Utils.COMMON, genStageId(), appletName, sArgs, outputVars),
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
                             wfOutputs: Vector[(CVar, SArg)],
                             outputDecls: Seq[WorkflowOutput]) : (IR.Stage, IR.Applet) = {
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
        val applet = IR.Applet(appletName,
                               inputVars,
                               outputVars,
                               calcInstanceType(None),
                               IR.DockerImageNone,
                               IR.AppletKindEval,
                               WdlRewrite.namespace(code, Seq.empty))
        verifyWdlCodeIsLegal(applet.ns)

        // Link to the X.y original variables
        val inputs: Vector[IR.SArg] = wfOutputs.map{ case (_, sArg) => sArg }.toVector

        (IR.Stage(Utils.OUTPUT_SECTION,
                  genStageId(Some(Utils.LAST_STAGE)),
                  appletName,
                  inputs,
                  outputVars),
         applet)
    }

    // Compile a workflow, having compiled the independent tasks.
    private def compileWorkflowLocked(wf: WdlWorkflow,
                                      wfInputs: Vector[(CVar, SArg)],
                                      subBlocks: Vector[Block]) :
            (Vector[(IR.Stage, Option[IR.Applet])], Vector[(CVar, SArg)]) = {
        Utils.trace(verbose.on, s"Compiling locked-down workflow ${wf.unqualifiedName}")

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
        Utils.trace(verbose.on, s"Compiling regular workflow ${wf.unqualifiedName}")

        // Create a preliminary stage to handle workflow inputs, and top-level
        // declarations.
        val (inputStage, inputApplet) = compileCommonApplet(wf, wfInputs)

        // An environment where variables are defined
        val initEnv : CallEnv = inputStage.outputs.map { cVar =>
            cVar.name -> LinkedVar(cVar, IR.SArgLink(inputStage.name, cVar))
        }.toMap

        val initAccu : (Vector[(IR.Stage, Option[IR.Applet])]) =
            (Vector((inputStage, Some(inputApplet))))

        // link together all the stages into a linear workflow
        val (allStageInfo, env) = buildWorkflowBackbone(wf, subBlocks, initAccu, initEnv)
        val wfOutputs: Vector[(CVar, SArg)] = prepareOutputSection(env, wf.outputs)

        // output section is non empty, keep only those files
        // at the destination directory
        val (outputStage, outputApplet) = compileOutputSection(
            wf.unqualifiedName ++ "_" ++ Utils.OUTPUT_SECTION,
            wfOutputs, wf.outputs)

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
            case decl:Declaration =>
                Utils.declarationIsInput(decl) &&
                !Utils.isGeneratedVar(decl.unqualifiedName)
            case _ => false
        }
        val wfInputs:Vector[(CVar, SArg)] = buildWorkflowInputs(
            wfInputDecls.map{ _.asInstanceOf[Declaration]}
        )

        // Create a stage per call/scatter-block/declaration-block
        val subBlocks = splitIntoBlocks(wfProper)

        val (allStageInfo_i, wfOutputs) =
            if (locked)
                compileWorkflowLocked(wf, wfInputs, subBlocks)
            else
                compileWorkflowRegular(wf, wfInputs, subBlocks)

        // Add a reorganization applet if requested
        val allStageInfo =
            if (reorg) {
                val (rStage, rApl) = createReorgApplet(wf.unqualifiedName, wfOutputs)
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

        /*val wfInputNames = wfInputs.map{ case (cVar,_) => cVar.name }.toVector
        val wfOutputNames = wfOutputs.map{ case (cVar,_) => cVar.name }.toVector
        System.err.println(s"""|${wf.unqualifiedName}
                               |  wfInputs=${wfInputNames}
                               |  wfOutputs=${wfOutputNames}
                               |""".stripMargin)*/
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
                         verbose: Verbose) : IR.Namespace = {
        // recursively generate IR for the entire tree
        val nsTree1 = nsTree match {
            case NamespaceOps.TreeLeaf(name, cef, tasks) =>
                // compile all the [tasks], that have not been already compiled, to IR.Applet
                val gir = new GenerateIR(Map.empty, nsTree.cef, reorg, locked, verbose)
                val alreadyCompiledNames: Set[String] = callables.keys.toSet
                val tasksNotCompiled = tasks.filter{
                    case (taskName,_) =>
                        !(alreadyCompiledNames contains taskName)
                }
                val taskApplets = tasksNotCompiled.map{ case (_,task) =>
                    val applet = gir.compileTask(task)
                    task.name -> applet
                }.toMap
                makeNamespace(name, None, callables ++ taskApplets)

            case NamespaceOps.TreeNode(name, cef, _, _, workflow, children) =>
                // Recurse into the children.
                //
                // The reorg and locked flags only apply to the top level
                // workflow. All other workflows are sub-workflows, and they do
                // not reorganize the outputs.
                val childCallables = children.foldLeft(callables){
                    case (accu, child) =>
                        val childIr = applyRec(child, accu, false, true, verbose)
                        accu ++ childIr.buildCallables
                }
                val gir = new GenerateIR(childCallables, cef, reorg, locked, verbose)
                val (irWf, auxApplets) = gir.compileWorkflow(workflow)
                makeNamespace(name, Some(irWf), childCallables ++ auxApplets)
        }
        nsTree1
    }

    // Entrypoint
    def apply(nsTree : NamespaceOps.Tree,
              reorg: Boolean,
              locked: Boolean,
              verbose: Verbose) : IR.Namespace = {
        Utils.trace(verbose.on, s"IR pass")
        applyRec(nsTree, Map.empty, reorg, locked, verbose)
    }
}
