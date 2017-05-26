/** Utilities for modifying WDL classes, used in rewritting
  * WDL code.
  *
  * For example, there is no constructor for creating a new task from
  * an existing one. Sometimes, it is quite tricky to create valid
  * classes.
  *
  *  Caveat: the ASTs are incorrect for these resulting instances.
  */
package dxWDL

import wdl4s._
import wdl4s.AstTools
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._

object WdlRewrite {

    // Create a declaration.
    def newDeclaration(wdlType: WdlType,
                       name: String,
                       expr: Option[WdlExpression]) : Declaration = {
        val textualRepr = expr match {
            case None => s"${wdlType.toWdlString} ${name}"
            case Some(e) => s"${wdlType.toWdlString} ${name} = ${e.toWdlString}"
        }
        val ast: Ast = AstTools.getAst(textualRepr, "")
        Declaration(wdlType, name, expr, None, ast)
    }

    // Modify the inputs in a task-call
    def taskCall(tc: TaskCall,
                 inputMappings: Map[String, WdlExpression]) : TaskCall = {
        val tc1 = TaskCall(tc.alias, tc.task, inputMappings, tc.ast)
        tc1.namespace = tc.namespace
        tc1.children = tc.children
        tc.parent match {
            case Some(x) => tc1.parent = x
            case None => ()
        }
        tc1
    }

    // modify the children in a workflow
    def workflow [Child <: Scope] (wf: Workflow,
                                   children: Seq[Child]): Workflow = {
        val wf1 = new Workflow(wf.unqualifiedName, wf.workflowOutputWildcards,
                               wf.wdlSyntaxErrorFormatter, wf.meta, wf.parameterMeta,
                               wf.ast)
        wf1.children = children
        wf1.namespace = wf.namespace
        wf.parent match {
            case Some(x) => wf1.parent = x
            case None => ()
        }
        wf1
    }

    // modify the children in a scatter
    def scatter [Child <: Scope] (ssc: Scatter,
                                  children: Seq[Child]): Scatter = {
        val ssc1 = Scatter(ssc.index, ssc.item, ssc.collection, ssc.ast)
        ssc1.children = children
        ssc1.namespace = ssc.namespace
        ssc.parent match {
            case Some(x) => ssc1.parent = x
            case None => ()
        }
        ssc1
    }

}
