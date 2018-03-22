/*
 * Rename variables in an expression. For example, examine
 * the expression:
 *     Add.result + Sum.result
 *
 * It is the sum of two calls. The Add call may have
 * been moved to a subworkflow, causing Add.result to be renamed
 * subwf_Add.out_result. We want to change the expression to:
 *
 *    (subwf_Add.out_result + Sum.result)
 */
package dxWDL.compiler

import dxWDL.{Verbose}
import scala.util.matching.Regex.Match
import wdl._

// dict: variables to rename. ("Add.result" -> "subwf_Add.out_result")
//
case class ExpressionRenameVars(dict: Map[String, String],
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

    def apply(expr: WdlExpression) : WdlExpression = {
        val parts = split(expr.toWomString)
//        Utils.trace(verbose.on,
//                    s"expr=${expr.toWomString}    parts=${parts}")

        val translatedParts = parts.map {
            case Fqn(fqn) =>
                // if the identifer is of the form A.x or A,
                // where A is in the traslation list, then convert into:
                // F(A).x
                val translation = dict.find{
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

}
