package dxWDL.compiler

import dxWDL.{CompilerErrorFormatter, Utils, Verbose}
import wdl.draft2.model._

case class FreeVarAnalysis(cef: CompilerErrorFormatter,
                           verbose: Verbose) {

    // keep track of variables defined inside the block, and outside
    // the block.
    case class VarTracker(local: Map[String, DVar],   // defined inside the block
                          outside: Map[String, DVar])  // defined outside the block
    {
        // We want to check if a variable usage is local, or if an external variable
        // is required.
        //
        // For example, if the locally defined variables are:
        //   {A, B, C},
        //
        // {E, F} are external
        // {A, B.left, B.right} are local
        private def isLocal(fqn: String,
                            localVarNames: Set[String]) : Boolean = {
            if (localVarNames contains fqn) {
                // exact match
                true
            } else {
                // "xx.yy.zz" is not is not locally defined. Check "xx.yy".
                val pos = fqn.lastIndexOf(".")
                if (pos < 0) {
                    false
                } else {
                    val lhs = fqn.substring(0, pos)
                    isLocal(lhs, localVarNames)
                }
            }
        }

        // A statmement accessed a bunch of variables. Add any newly
        // discovered outside variables.
        def findNewIn(refs: Seq[DVar]) : VarTracker = {
            val localVarNames = local.keys.toSet
            val discovered =
                refs
                    .filter{ dVar => !isLocal(dVar.fullyQualifiedName, localVarNames) }
                    .map{ dVar => dVar.fullyQualifiedName -> dVar}.toMap
            VarTracker(local,
                       outside ++ discovered)
        }

        // Add a binding for a local variable
        def addLocal(dVar: DVar) : VarTracker = {
            VarTracker(local + (dVar.fullyQualifiedName -> dVar),
                       outside)
        }
        def addLocal(dVars: Seq[DVar]) : VarTracker = {
            val newLocals = dVars.map{ dVar => dVar.fullyQualifiedName -> dVar}
            VarTracker(local ++ newLocals,
                       outside)
        }
    }

    object VarTracker {
        val empty = VarTracker(Map.empty, Map.empty)
    }

    private def exprDeps(erv: VarAnalysis,
                         expr: WdlExpression,
                         scope: Scope): Vector[DVar] = {
        val variables = erv.findAllInExpr(expr)
        val retval = variables.map{ varName =>
            val womType =
                try {
                    Utils.lookupType(scope)(varName)
                } catch {
                    case e: Throwable if (varName contains '.') =>
                        // This is a member access, or a reference to a call output.
                        // Try evaluating the type.
                        Utils.evalType(expr, scope, cef, verbose)
                }
            DVar(varName, womType)
        }.toVector
        //Utils.trace(verbose.on, s"exprDeps(${expr.toWomString}) = ${retval}")
        retval
    }

    // Find all the free variables in a block of statements. For example,
    // for the following block:
    //
    //   call add { input: a=y, b=x }
    //   Int base = add.result
    //   call mul { input: a=base, n=x}
    //
    // The free variables are {x, y}.
    //
    private def freeVarsAccu(statements: Seq[Scope], accu: VarTracker) : VarTracker = {
        val erv = VarAnalysis(Set.empty, Map.empty, cef, verbose)
        statements.foldLeft(accu) {
            case (accu, decl:Declaration) =>
                val xtrnVars = decl.expression match {
                    case None => Vector.empty
                    case Some(expr) => exprDeps(erv, expr, decl)
                }
                val declVar = DVar(decl.unqualifiedName, decl.womType)
                accu.findNewIn(xtrnVars).addLocal(declVar)

            case (accu, call:WdlCall) =>
                // If the call has a parent, we want to use the parent scope.
                // The scope lookup algorithm (cromwell-32) gets confused if
                // the callee has an input variable with the same name, but
                // a different type, than the caller.
                //
                // Below, workflow w has "fruit" with type Array[String], but
                // task PTAsays has "fruit" with type String.
                //
                // workflow w {
                //  Array[String] fruit = ["Banana", "Apple"]
                //  scatter (index in indices) {
                //    call PTAsays {
                //        input: fruit = fruit[index], y = " is good to eat"
                //    }
                //    call Add { input:  a = 2, b = 4 }
                //  }
                //
                // task PTAsays {
                //    String fruit
                //    String y
                //    ...
                // }
                val parentScope = call.parent.getOrElse(call)
                val xtrnVars = call.inputMappings.map{
                    case (_, expr) => exprDeps(erv, expr, parentScope)
                }.flatten.toSeq
                val callOutputs = call.outputs.map { cOut: CallOutput =>
                    DVar(call.unqualifiedName + "." ++ cOut.unqualifiedName, cOut.womType)
                }
                accu.findNewIn(xtrnVars).addLocal(callOutputs)

            case (accu, ssc:Scatter) =>
                // Update the tracker based on the scatter collection,
                // then recurse into the children.
                val xtrnVars = exprDeps(erv, ssc.collection, ssc)
                val collectionType = Utils.evalType(ssc.collection, ssc, cef, verbose)
                val item = DVar(ssc.item,
                                Utils.stripArray(collectionType))
                val accu2 = accu.findNewIn(xtrnVars).addLocal(item)
                freeVarsAccu(ssc.children, accu2)

            case (accu, condStmt:If) =>
                // add any free variables in the condition expression,
                // then recurse into the children.
                val xtrnVars = exprDeps(erv, condStmt.condition, condStmt)
                val accu2 = accu.findNewIn(xtrnVars)
                freeVarsAccu(condStmt.children, accu2)

            case (_, x) =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        s"Unimplemented workflow element"))
        }
    }

    def apply(statements: Seq[Scope]) : Vector[DVar] = {
        val varTracker = freeVarsAccu(statements, VarTracker.empty)
        varTracker.outside.values.toVector
    }
}
