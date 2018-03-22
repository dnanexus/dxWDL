/*
 Rename variables in a statment. Iteratate over all the expressions
and rename the variables. For example, examine the expression:
     Add.result + Sum.result

 It is the sum of two calls. The Add call may have
 been moved to a subworkflow, causing Add.result to be renamed
 subwf_Add.out_result. We want to change the expression to:

    (subwf_Add.out_result + Sum.result)
 */

package dxWDL.compiler

import dxWDL.{CompilerErrorFormatter, Verbose}
import scala.util.matching.Regex.Match
import wdl._

// dict: variables to rename. ("Add.result" -> "subwf_Add.out_result")
//
case class StatementRenameVars(wordTranslations: Map[String, String],
                               cef: CompilerErrorFormatter,
                               verbose: Verbose) {

    // split into a list of fully qualified names (A.B.C), separated
    // by expression symbols (+, -, /, ...).
    //
    sealed trait Part
    case class Fqn(value: String) extends Part
    case class Symbols(value: String) extends Part

    case class Accu(tokens: Vector[Part],  // tokens found so far
                    pos: Int)   // position in the string

    private val fqnRegex = raw"""[a-zA-Z][a-zA-Z0-9_.]+""".r
    private def split(exprStr: String) : Vector[Part] = {
        val matches: List[Match] = fqnRegex.findAllMatchIn(exprStr).toList
        if (matches.isEmpty)
            return Vector(Symbols(exprStr))

        // convert every match to a FQN. Add the region between it
        // and the previous FQN as a symbol.
        val accu = matches.foldLeft(Accu(Vector.empty, 0)){
            case (accu, m) =>
                val fqn = Fqn(m.toString)
                val tokens =
                    if (m.start > accu.pos) {
                        val symb = Symbols(exprStr.substring(accu.pos, m.start))
                        Vector (symb, fqn)
                    } else {
                        Vector(fqn)
                    }
                Accu(accu.tokens ++ tokens, m.end)
        }

        // handle the symbols after the last match
        if (matches.last.end < exprStr.length) {
            val last = matches.last
            val lastSym =  Symbols(exprStr.substring(last.end, exprStr.length))
            accu.tokens :+ lastSym
        } else
            accu.tokens
    }

    private def exprRenameVars(expr: WdlExpression) : WdlExpression = {
        val parts = split(expr.toWomString)
//        Utils.trace(verbose.on,
//                    s"expr=${expr.toWomString}    parts=${parts}")

        val translatedParts = parts.map {
            case Fqn(fqn) =>
                // if the identifer is of the form A.x or A,
                // where A is in the traslation list, then convert into:
                // F(A).x
                val translation = wordTranslations.find{
                    case (org,_) =>
                        if (!fqn.startsWith(org)) false
                        else if (fqn.length == org.length) true
                        else (fqn(org.length) == '.')
                }
                translation match {
                    case None =>
                        // no need to replace this FQN
                        fqn
                    case Some((org, replacement)) =>
                        // we want to replace the beginning of this identifier
                        val ending = fqn.substring(org.length)
                        replacement + ending
                }
            case Symbols(sym) => sym
        }
        val tr = translatedParts.mkString("")
        WdlExpression.fromString(tr)
    }


    // rename all the variables from used inside a statement
    def apply(stmt: Scope) : Scope = {
        stmt match {
            case decl:Declaration =>
                WdlRewrite.declaration(decl.womType,
                                       decl.unqualifiedName,
                                       decl.expression.map(e => exprRenameVars(e)))

            case tc:WdlTaskCall =>
                val inputsRn = tc.inputMappings.map{
                    case (name, expr) =>  name -> exprRenameVars(expr)
                }.toMap
                WdlRewrite.taskCall(tc, inputsRn)

            case wfc:WdlWorkflowCall =>
                val inputsRn = wfc.inputMappings.map{
                    case (name, expr) =>  name -> exprRenameVars(expr)
                }.toMap
                WdlRewrite.workflowCall(wfc, inputsRn)

            case ssc:Scatter =>
                val children = ssc.children.map {
                    case child:Scope => apply(child)
                }.toVector
                WdlRewrite.scatter(ssc,
                                   children,
                                   exprRenameVars(ssc.collection))

            case cond:If =>
                val children = cond.children.map {
                    case child:Scope => apply(child)
                }.toVector
                WdlRewrite.cond(cond,
                                children,
                                exprRenameVars(cond.condition))

            case wo:WorkflowOutput =>
                new WorkflowOutput(wo.unqualifiedName,
                                   wo.womType,
                                   exprRenameVars(wo.requiredExpression),
                                   wo.ast,
                                   wo.parent)
            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        "Unimplemented workflow element"))
        }
    }


    private def findInExpr(expr: WdlExpression) : Set[String] = {
        val parts = split(expr.toWomString)
        parts.flatMap {
            case Fqn(fqn) =>
                // if the identifer is of the form A.x or A,
                // where A is in the traslation list, then convert into:
                // F(A).x
                val translation = wordTranslations.find{
                    case (org,_) =>
                        if (!fqn.startsWith(org)) false
                        else if (fqn.length == org.length) true
                        else (fqn(org.length) == '.')
                }
                translation match {
                    case None => None
                    case Some((org, _)) => Some(org)
                }
            case Symbols(sym) => None
        }.toSet
    }



    // Find all the references to the
    def find(stmt: Scope) : Set[String] = {
        stmt match {
            case decl:Declaration =>
                decl.expression match  {
                    case None => Set.empty
                    case Some(e) => findInExpr(e)
                }

            case tc:WdlTaskCall =>
                tc.inputMappings.map{
                    case (_, expr) =>  findInExpr(expr)
                }.toSet.flatten

            case wfc:WdlWorkflowCall =>
                wfc.inputMappings.map{
                    case (_, expr) =>  findInExpr(expr)
                }.toSet.flatten

            case ssc:Scatter =>
                val s1 = ssc.children.flatMap {
                    case child:Scope => find(child)
                }.toSet
                s1 ++ findInExpr(ssc.collection)

            case cond:If =>
                val s1 = cond.children.flatMap {
                    case child:Scope => find(child)
                }.toSet
                s1 ++ findInExpr(cond.condition)

            case wot:WorkflowOutput =>
                findInExpr(wot.requiredExpression)

            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        "Unimplemented workflow element"))
        }
    }
}
