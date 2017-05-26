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

    // copy references from old scope to newly minted scope.
    private def updateScope(old: Scope, fresh: Scope) : Unit = {
        fresh.namespace = old.namespace
        old.parent match {
            case Some(x) => fresh.parent = x
            case None => ()
        }
    }

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
        tc1.children = tc.children
        updateScope(tc, tc1)
        tc1
    }

    // Modify a task call to the callee namespace. This is used
    // when generating scatter code, because we virtually move
    // the callees into the local namespace
    //
    // For example:
    //    call lib.Inc as inc {input: i=i}
    // =>
    //    call Inc as inc {input: i=i}
/*    def taskCallStripImports(tc: TaskCall,
                             inputMappings: Map[String, WdlExpression]) : TaskCall = {
        val localTask = taskGenStub(tc.task.name, tc, tc.task.children)
        val tc1 = TaskCall(tc.alias, localTask, inputMappings, tc.ast)
        tc1.children = tc.children
        updateScope(tc, tc1)
        tc1
    }*/

    // create a stub, similar to a header in C/C++, for a task.
    // This is a minimal task that has the correct name, inputs,
    // and outputs, but does nothing.
    def taskGenStub [Child <: Scope]  (name: String,
                                       scope: Scope,
                                       children: Seq[Child]) : Task = {
        val task = new Task(name,
                            Vector.empty,  // command Template
                            new RuntimeAttributes(Map.empty[String,WdlExpression]),
                            Map.empty[String, String], // meta
                            Map.empty[String, String], // parameter meta
                            scope.ast)
        task.children = children
        updateScope(scope, task)
        task
    }

    // modify the children in a workflow
    def workflow [Child <: Scope] (wf: Workflow,
                                   children: Seq[Child]): Workflow = {
        val wf1 = new Workflow(wf.unqualifiedName, wf.workflowOutputWildcards,
                               wf.wdlSyntaxErrorFormatter, wf.meta, wf.parameterMeta,
                               wf.ast)
        wf1.children = children
        updateScope(wf, wf1)
        wf1
    }

    // modify the children in a scatter
    def scatter [Child <: Scope] (ssc: Scatter,
                                  children: Seq[Child]): Scatter = {
        val ssc1 = Scatter(ssc.index, ssc.item, ssc.collection, ssc.ast)
        ssc1.children = children
        updateScope(ssc, ssc1)
        ssc1
    }

/*    def scatter [Child <: Scope] (ssc: Scatter,
                                  collection: WdlExpression,
                                  children: Seq[Child]): Scatter = {
    }*/
}
