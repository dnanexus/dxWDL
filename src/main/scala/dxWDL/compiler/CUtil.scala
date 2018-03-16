/** Utilities used by several compiler modules
  */
package dxWDL.compiler

import dxWDL.Utils
import IR.{CallEnv, LinkedVar}
import wdl._
import wdl.AstTools.EnhancedAstNode
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl.WdlExpression.AstForExpressions

object CUtil {
    // Check if the environment has A.B.C, A.B, or A.
    def trailSearch(env: CallEnv, ast: Ast) : Option[(String, LinkedVar)] = {
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
    def updateClosure(closure : CallEnv,
                      env : CallEnv,
                      expr : WdlExpression) : CallEnv = {
        val deps:Set[String] = envDeps(env, expr.ast)
        // Utils.trace(verbose2, s"updateClosure deps=${deps}  expr=${expr.toWomString}")
        closure ++ deps.map{ v => v -> env(v) }
    }
}
