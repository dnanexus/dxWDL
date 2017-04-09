/** Compile a WDL Workflow into a dx:workflow
  *
  * The front end takes a WDL workflow, and generates an intermediate
  * representation from it.
  */
package dxWDL

import java.nio.file.{Files, Paths, Path}
import scala.util.{Failure, Success, Try}
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.{Call, Declaration, Scatter, Scope, Task, TaskOutput, WdlExpression,
    WdlNamespace, WdlNamespaceWithWorkflow, WdlSource, Workflow}
import wdl4s.expression.NoFunctions
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions
import wdl4s.expression.WdlStandardLibraryFunctionsType

object CompilerFrontEnd {
    class DynamicInstanceTypesException private(ex: Exception) extends RuntimeException(ex) {
        def this() = this(new RuntimeException(
                              "Only runtime constants are supported, currently, instance types should be known at compile time."))
    }

    // Linking between a variable, and which stage we got
    // it from.
    case class LinkedVar(cVar: IR.CVar, sArg: IR.SArg)

    // Environment (scope) where a call is made
    type CallEnv = Map[String, LinkedVar]

    // The minimal environment needed to execute a call
    type Closure =  CallEnv

    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(wf : Workflow,
                     destination: String,
                     cef: CompilerErrorFormatter,
                     verbose: Boolean)

    // Figure out the expression type for a collection we loop over in a scatter
    //
    // Expressions like A.B.C are converted to A___B___C, in order to avoid
    // the wdl4s library from interpreting these as member accesses. The environment
    // passed into the method [env] has types for all these variables, as fully qualified names.
    def calcIterWdlType(scatter : Scatter, env : Map[String,WdlType]) : WdlType = {
        def lookup(varName : String) : WdlType = {
            val v = varName.replaceAll("___", "\\.")
            env.get(v) match {
                case Some(x) => x
                case None => throw new Exception(s"No type found for variable ${varName}")
            }
        }

        // convert all sub-expressions of the form A.B.C to A___B___C
        val s : String = scatter.collection.toWdlString.replaceAll("\\.", "___")
        val collection : WdlExpression = WdlExpression.fromString(s)

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
                        if (strict)
                            throw new Exception(cState.cef.missingVarRefException(a))
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

    // Print a WDL task that evaluates expressions, and outputs
    // all of them
    def genEvalTaskFromDeclarations(name: String,
                                    declarations: Seq[Declaration]) : String = {
        val inputs = declarations.map(x => apply(x, 0)).flatten.toVector
        val outputs = declarations.map(x =>
            s"${decl.wdlType.toWdlString} ${decl.unqualifiedName}"
        )
        val lines = inputs ++
            WdlPrettyPrinter.buildBlock("command", Vector(""), 0) ++
            WdlPrettyPrinter.buildBlock("output", outputs, 0)
        WdlPrettyPrinter.buildBlock(s"task ${name}", lines, 0)
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
    def compileCommon(wf : Workflow,
                      topDeclarations: Seq[Declaration],
                      env : CallEnv,
                      cState: State) : (IR.Applet, Closure, String, List[IR.CVar]) = {
        Utils.trace(cState.verbose, s"Compiling workflow initialization sequence")
        val appletFqn : String = wf.unqualifiedName ++ "." ++ Utils.COMMON
        val code = genEvalTaskFromDeclarations(appletFqn, topDeclarations)

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
        val inputSpec : List[IR.CVar] =  topDeclarations.map{ decl =>
            decl.expression match {
                case None => Some(IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast))
                case Some(_) => None
            }
        }.flatten.toList
        val outputSpec : List[IR.CVar] = topDeclarations.map{ decl =>
            IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast)
        }.toList

        // We need minimal compute resources, use the default instance type
        val applet = IR.Applet(appletFqn,
                               inputSpec,
                               outputSpec,
                               calcInstanceType(None),
                               None,
                               cState.destination,
                               IR.AppletKind.Eval,
                               code,
                               wf.ast)
        (applet, Map.empty[String, LinkedVar], Utils.COMMON, outputSpec)
    }

    // Compile a WDL task into an applet
    def compileTask(task : Task, cState: State) : (IR.Applet, List[IR.CVar]) = {
        Utils.trace(cState.verbose, s"Compiling task ${task.name}")
        val appletFqn : String = wf.unqualifiedName ++ "." ++ task.name

        // The task inputs are those that do not have expressions
        val inputSpec : List[IR.CVar] =  task.declarations.map{ decl =>
            decl.expression match {
                case None => Some(IR.CVar(decl.unqualifiedName, decl.wdlType, decl.ast))
                case Some(_) => None
            }
        }.flatten.toList
        val outputSpec : List[IR.CVar] = desk.outputs.map{ tso =>
            IR.CVar(tso.unqualifiedName, tso.wdlType, tso.ast)
        }.toList

        // Figure out if we need to use docker
        val docker = task.runtimeAttributes.attrs.get("docker") match {
            case None => None
            case Some(wdlExpression) =>
                var buf = wdlExpression.toWdlString
                buf = buf.replaceAll("\"","")
                Some(buf)
        }
        val code = WdlPrettyPrinter.apply(task, 0).mkString("\n")
        val applet = IR.Applet(appletFqn,
                               inputSpec,
                               outputSpec,
                               calcInstanceType(Some(task)),
                               docker,
                               cState.destination,
                               IR.AppletKind.Task,
                               code,
                               task.ast)
        (applet, Map.empty[String, LinkedVar], task.name, outputSpec)
    }

    def compileCall(call: Call,
                    appletDict: Map[String, (IR.Applet, List[IR.CVar])],
                    env : CallEnv,
                    cState: State) : (IR.Applet, Closure, String, List[IR.CVar]) = {
        // Find the right applet
        val name = call match {
            case x:TaskCall => x.task.name
            case x:WorkflowCall =>
                throw new Exception(cState.cef.notCurrentlySupported(call.ast, s"calling a workflow"))
        }
        val (callee, outputs) = taskApplets(name)
        val inputs : List[IR.CVar] = call.inputMappings.foreach { case (varName, expr) =>
            // TODO: handle optional and missing arguments.
        }
        val inputs  = closure.map {
            case (varName, LinkedVar(cVar, _)) => IR.CVar(varName, cVar.wdlType, cVar.ast)
        }.toList
        val scatterFqn = wf.unqualifiedName ++ "." ++ stageName
        val applet = IR.Applet(scatterFqn,
                               inputs,
                               outputDecls,
                               calcInstanceType(None),
                               None,
                               cState.destination,
                               IR.AppletKind.Scatter,
                               genScatterWorklow(scatter),
                               scatter.ast)
    }

    def genScatterWorklow(name: String,
                          scatter: Scatter) : String = {
        throw new Exception("Unimplemented")
    }

    // Compile a scatter block
    def compileScatter(wf : Workflow,
                       scatter: Scatter,
                       env : CallEnv,
                       cState: State) : (IR.Applet, Closure, String, List[IR.CVar]) = {
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
        val stageName = Utils.SCATTER ++ "___" ++ callUniqueName(calls(0), cState)
        Utils.trace(cState.verbose, s"compiling scatter ${stageName}")

        // Construct the block output by unifying individual call outputs.
        // Each applet output becomes an array of that type. For example,
        // an Int becomes an Array[Int].
        val outputDecls : List[IR.CVar] = calls.map { call =>
            val task = Utils.taskOfCall(call)
            task.outputs.map { tso =>
                IR.CVar(callUniqueName(call, cState) ++ "." ++ tso.unqualifiedName,
                        WdlArrayType(tso.wdlType), tso.ast)
            }
        }.flatten.toList

        // Figure out the closure, we need the expression being looped over.
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

        // Add the declarations at the top of the block
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

        val inputs : List[IR.CVar] = closure.map {
            case (varName, LinkedVar(cVar, _)) => IR.CVar(varName, cVar.wdlType, cVar.ast)
        }.toList
        val scatterFqn = wf.unqualifiedName ++ "." ++ stageName
        val applet = IR.Applet(scatterFqn,
                               inputs,
                               outputDecls,
                               calcInstanceType(None),
                               None,
                               cState.destination,
                               IR.AppletKind.Scatter,
                               genScatterWorklow(scatter),
                               scatter.ast)

        // The calls will be made from the scatter stage at runtime.
        calls.foreach { call =>
            val task = Utils.taskOfCall(call)
            val outputs = task.outputs.map { tso =>
                IR.CVar(callUniqueName(call, cState) ++ "." ++ tso.unqualifiedName,
                        tso.wdlType, tso.ast)
            }
            // Add bindings for all call output variables. This allows later calls to refer
            // to these results. Links to their values are unknown at compile time, which
            // is why they are set to None.
            for (cVar <- outputs) {
                val fqVarName = callUniqueName(call, cState) ++ "." ++ cVar.name
                innerEnv = innerEnv + (fqVarName ->
                                           LinkedVar(IR.CVar(fqVarName, cVar.wdlType, cVar.ast),
                                                        IR.SArgEmpty))
            }
        }

        (applet, closure, stageName, outputDecls)
    }


    // compile the WDL source code into intermediate representation
    def apply(ns : WdlNamespace,
              destination: String,
              cef: CompilerErrorFormatter,
              verbose: Boolean) : IR.Workflow = {
        // compile all the tasks into applets
        val appletDict: Map[String, (IR.Applet, List[IR.CVar])] = ns.tasks.map{ task =>
            val (applet, outputs) = compileTask(task)
            task.name -> (applet, outputs)
        }.toMap

        // extract the workflow
        val wf = ns match {
            case nswf : WdlNamespaceWithWorkflow => nswf.workflow
            case _ => throw new Exception("WDL does not have a workflow")
        }
        val cState = new State(wf, destination, cef, verbose)

        // An environment where variables are defined
        var env : CallEnv = Map.empty[String, LinkedVar]

        // Deal with the toplevel declarations, and leave only
        // children we can deal with. Lift all declarations that can
        // be evaluated at the top of the block. Nested calls to
        // workflows are not supported, neither are any remaining
        // declarations in the middle of the workflow.
        val (topDecls, wfBody) = Utils.splitBlockDeclarations(wf.children.toList)
        val calls : Seq[Scope] = wfCore.map {
            case call: Call => call
            case ssc: Scatter => ssc
            case decl: Declaration =>
                throw new Exception(cState.cef.notCurrentlySupported(
                                        decl.ast,
                                        "Declaration in the middle of workflow could not be lifted"))
            case swf: Workflow =>
                throw new Exception(cState.cef.notCurrentlySupported(
                                        swf.ast, "Nested workflow"))
        }

        // - Create a preliminary stage to handle workflow inputs, and top-level
        //   declarations.
        // - Create a stage per call/scatter-block
        val stgAplPairs = (wf :: calls.toList).map { child =>
            val (appletIr, closure, stageName, outputs) = child match {
                case _ : Workflow =>
                    compileCommon(wf, topDecls, env, cState)
                case call: Call =>
                    compileCall(call, appletDict, env, cState)
                case scatter : Scatter =>
                    compileScatter(wf, scatter, env, cState)
            }

            val inputs = closure.map { case (varName, LinkedVar(cVar,sArg)) => sArg }.toList
            val stageIr = IR.Stage(stageName, appletIr.name, inputs)

            // Add bindings for all call output variables. This allows later calls to refer
            // to these results. In case of scatters, there is no block name to reference.
            for (cVar <- outputs) {
                val fqVarName : String = child match {
                    case call : Call => stageName ++ "." ++ cVar.name
                    case scatter : Scatter => cVar.name
                    case _ : Workflow => cVar.name
                    case _ => throw new Exception("Sanity")
                }
                env = env + (fqVarName ->
                                 LinkedVar(cVar, IR.SArgLink(stageName, cVar.name)))
            }
            (stageIr, appletIr)
        }
        val (stages, applets) = stgAplPairs.unzip
        IR.Workflow(wf.unqualifiedName, stages, applets ++ taskApplets.toVector)
    }
}
