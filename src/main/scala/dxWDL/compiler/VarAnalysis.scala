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

import dxWDL.{CompilerErrorFormatter, Utils, Verbose}
import scala.util.matching.Regex
import scala.util.matching.Regex.Match
import wdl.draft2.model._

// dict: variables to rename. ("Add.result" -> "subwf_Add.out_result")
//
case class VarAnalysis(doNotModify: Set[Scope],
                       wordTranslations: Map[String, String],
                       cef: CompilerErrorFormatter,
                       verbose: Verbose) {
    case class Accu(tokens: Vector[String],  // tokens found so far
                    pos: Int)   // position in the string

    // Split a a string by a regular expression into tokens. Include
    // the extents in between matches as individual tokens.
    private def splitWithRangesAsTokens(buf: String, regex: Regex) : Vector[String] = {
        val matches: List[Match] = regex.findAllMatchIn(buf).toList
        if (matches.isEmpty) {
            return Vector(buf)
        }

        // Add the extents between matches.
        val accu = matches.foldLeft(Accu(Vector.empty, 0)){
            case (accu, m) =>
                val tokens =
                    if (m.start > accu.pos) {
                        val before = buf.substring(accu.pos, m.start)
                        Vector (before, m.toString)
                    } else {
                        Vector(m.toString)
                    }
                Accu(accu.tokens ++ tokens, m.end)
        }

        // handle the symbols after the last match
        if (matches.last.end < buf.length) {
            val last = matches.last
            val lastToken =  buf.substring(last.end, buf.length)
            accu.tokens :+ lastToken
        } else {
            accu.tokens
        }
    }

    // split into a list of three kinds of elements:
    //  1) fully qualified names (A.B.C)
    //  2) expression symbols (+, -, /, ...).
    //  3) quoted strings ("Tamara likes ${fruit}s and ${ice}")
    //
    // A string can contain an interpolation
    sealed trait Part
    case class Fqn(value: String) extends Part
    case class Symbols(value: String) extends Part

    private val fqnRegex = raw"""[a-zA-Z][a-zA-Z0-9_.]*""".r
    private val quotedStringRegex = raw""""([^"]*)"""".r

    // From a string like: "Tamara likes ${fruit}s and ${ice}s"
    // extract the variables {fruit, ice}. In general, they
    // could be full fledged expressions
    //
    // from: wdl4s.wdl.expression.ValueEvaluator.InterpolationTagPattern
    private val interpolationRegex = "\\$\\{\\s*([^\\}]*)\\s*\\}".r

    private def isIdentifer(token: String,
                            nextToken: Option[String]) : Boolean = {
        if (token contains '"')
            return false
        if (!fqnRegex.pattern.matcher(token).matches)
            return false
        if (nextToken == None)
            return true
        val nextWord = nextToken.get.trim
        if (nextWord(0) == '(') {
            // Must be a call to a standard library function
            if (!(Utils.STDLIB_FUNCTIONS contains token))
                throw new Exception(s"""|${token} is followed by parentheses, but is not a WDL
                                        |standard library function""".stripMargin.replaceAll("\n", " "))
            return false
        }
        if (Utils.RESERVED_WORDS contains token)
            return false
        return true
    }

    private def isInterpolation(token: String) : Boolean = {
        if (token.length <= 3)
            return false
        if (!interpolationRegex.pattern.matcher(token).matches)
            return false
        return true
    }

    private def split(buf: String) : Vector[Part] = {
        // split into sub-strings
        val ranges = splitWithRangesAsTokens(buf, quotedStringRegex)
        val ranges2 = ranges.map{ token =>
            if (token(0) == '"' &&
                    token(token.length - 1) == '"')
                // look for ${...} elements inside the string
                splitWithRangesAsTokens(token, interpolationRegex)
            else
                // Look for fully qualified names
                splitWithRangesAsTokens(token, fqnRegex)
        }.flatten

        // identify each sub-string; which kind of token is it?
        ranges2.zipWithIndex.map{ case (token,i) =>
            val nextToken =
                if (i < (ranges2.length - 1))
                    Some(ranges2(i+1))
                else
                    None
            if (isIdentifer(token, nextToken))
                Vector(Fqn(token))
            else if (isInterpolation(token))
                Vector(Symbols("${"),
                       Fqn(token.substring(2, token.length - 1)),
                       Symbols("}"))
            else
                Vector(Symbols(token))
        }.flatten.toVector
    }

    private def exprRenameVars(expr: WdlExpression) : WdlExpression = {
        val parts = split(expr.toWomString)
        /*Utils.trace(verbose.on,
                    s"expr=${expr.toWomString}    parts=${parts}")*/

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


    // rename all the variables defined in the dictionary
    def rename(stmt: Scope) : Scope = {
        if (doNotModify contains stmt) {
            // These are statements whose ASTs are not entirely
            // valid. We do not want to risk applying WDL
            // code to them.
            return stmt
        }
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
                    case child:Scope => rename(child)
                }.toVector
                WdlRewrite.scatter(ssc,
                                   children,
                                   exprRenameVars(ssc.collection))

            case cond:If =>
                val children = cond.children.map {
                    case child:Scope => rename(child)
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

            case wf:WdlWorkflow =>
                val children = wf.children.map {
                    case child:Scope => rename(child)
                }.toVector
                WdlRewrite.workflow(wf, children)

            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        "Unimplemented workflow element"))
        }
    }


    private def findInExpr(expr: WdlExpression): Set[String] = {
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

    // Find only variable references inside the dictionary
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

    def findAllInExpr(expr: WdlExpression): Set[String] = {
        val parts = split(expr.toWomString)
        parts.flatMap {
            case Fqn(fqn) => Some(fqn)
            case Symbols(sym) => None
        }.toSet
    }

    // Find all variable references
    def findAll(stmt: Scope) : Set[String] = {
        stmt match {
            case decl:Declaration =>
                decl.expression match  {
                    case None => Set.empty
                    case Some(e) => findAllInExpr(e)
                }

            case tc:WdlTaskCall =>
                tc.inputMappings.map{
                    case (_, expr) =>  findAllInExpr(expr)
                }.toSet.flatten

            case wfc:WdlWorkflowCall =>
                wfc.inputMappings.map{
                    case (_, expr) =>  findAllInExpr(expr)
                }.toSet.flatten

            case ssc:Scatter =>
                val s1 = ssc.children.flatMap {
                    case child:Scope => findAll(child)
                }.toSet
                s1 ++ findAllInExpr(ssc.collection)

            case cond:If =>
                val s1 = cond.children.flatMap {
                    case child:Scope => findAll(child)
                }.toSet
                s1 ++ findAllInExpr(cond.condition)

            case wot:WorkflowOutput =>
                findAllInExpr(wot.requiredExpression)

            case x =>
                throw new Exception(cef.notCurrentlySupported(
                                        x.ast,
                                        "Unimplemented workflow element"))
        }
    }

}
