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

    // Create an empty task.
    def taskGenEmpty(name: String, scope: Scope) : Task = {
        val task = new Task(name,
                            Vector.empty,  // command Template
                            new RuntimeAttributes(Map.empty[String,WdlExpression]),
                            Map.empty[String, String], // meta
                            Map.empty[String, String], // parameter meta
                            scope.ast)
        updateScope(scope, task)
        task
    }


    private def genDefaultValueOfType(wdlType: WdlType) : WdlValue = {
        wdlType match {
            case WdlArrayType(x) => WdlArray(WdlArrayType(x), List())  // an empty array
            case WdlBooleanType => WdlBoolean(true)
            case WdlIntegerType => WdlInteger(0)
            case WdlFloatType => WdlFloat(0.0)
            case WdlStringType => WdlString("")
            case WdlFileType => WdlFile("/tmp/X.txt")
            case _ => throw new Exception(s"Unhandled type ${wdlType.toWdlString}")
        }
    }


    def taskOutput(name: String, wdlType: WdlType, scope: Scope) = {
        val invalidAst = AstTools.getAst("", "")

        // We need to provide a default value, in the form of a Wdl
        // expression
        val defaultVal:WdlValue = genDefaultValueOfType(wdlType)
        val defaultExpr:WdlExpression = WdlExpression.fromString(defaultVal.toWdlString)

        new TaskOutput(name, wdlType, defaultExpr, invalidAst, Some(scope))
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
