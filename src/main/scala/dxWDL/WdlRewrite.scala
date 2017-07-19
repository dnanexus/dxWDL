/** Utilities for modifying WDL classes, used in rewritting
  * WDL code.
  *
  * For example, there is no constructor for creating a new task from
  * an existing one. Sometimes, it is quite tricky to create valid
  * classes.
  *
  * Caveat: the ASTs are incorrect for the resulting instances.
  */
package dxWDL

import wdl4s._
import wdl4s.AstTools
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._

object WdlRewrite {
    val INVALID_AST = AstTools.getAst("", "")
    val INVALID_ERR_FORMATTER = WdlSyntaxErrorFormatter(Map.empty[Terminal, WdlSource])

    // copy references from old scope to newly minted scope.
    private def updateScope(old: Scope, fresh: Scope) : Unit = {
        fresh.namespace = old.namespace
        old.parent match {
            case Some(x) => fresh.parent = x
            case None => ()
        }
    }

    // Create a declaration.
    def declaration(wdlType: WdlType,
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
        // We need to provide a default value, in the form of a Wdl
        // expression
        val defaultVal:WdlValue = genDefaultValueOfType(wdlType)
        val defaultExpr:WdlExpression = WdlExpression.fromString(defaultVal.toWdlString)

        new TaskOutput(name, wdlType, defaultExpr, INVALID_AST, Some(scope))
    }

    // Modify the children in a workflow.
    def workflow[Child <: Scope] (wfOld: Workflow,
                                  children: Seq[Child]): Workflow = {
        val wf = new Workflow(wfOld.unqualifiedName,
                              Seq.empty, // no output wildcards
                              wfOld.wdlSyntaxErrorFormatter,
                              wfOld.meta, wfOld.parameterMeta,
                              wfOld.ast)
        wf.children = children
        updateScope(wfOld, wf)
        wf
    }

    def workflowGenEmpty(wfName: String) : Workflow = {
        new Workflow(wfName,
                     List.empty,
                     INVALID_ERR_FORMATTER,
                     Map.empty[String, String],
                     Map.empty[String, String],
                     INVALID_AST)
    }

    def workflowOutput(varName: String,
                       wdlType: WdlType,
                       scope: Scope) = {
        new WorkflowOutput("out_" + varName,
                           wdlType,
                           WdlExpression.fromString(varName),
                           INVALID_AST,
                           Some(scope))
    }

    def workflowOutput(varName: String,
                       wdlType: WdlType,
                       expr: WdlExpression) = {
        new WorkflowOutput("out_" + varName,
                           wdlType,
                           expr,
                           INVALID_AST,
                           None)
    }

    // modify the children in a scatter
    def scatter [Child <: Scope] (ssc: Scatter,
                                  children: Seq[Child]): Scatter = {
        val ssc1 = Scatter(ssc.index, ssc.item, ssc.collection, ssc.ast)
        ssc1.children = children
        updateScope(ssc, ssc1)
        ssc1
    }

    // modify the children in a scatter, and modify the collection
    // expression. This is useful for a rewrite step like this:
    //
    // scatter (i in [1,2,3]) { ... }
    // ->
    // Array[Int] xtmp5 = [1,2,3]
    // scatter (i in xtmp5) { ... }
    def scatter [Child <: Scope] (ssc: Scatter,
                                  children: Seq[Child],
                                  expr: WdlExpression): Scatter = {
        val ssc1 = Scatter(ssc.index, ssc.item, expr, INVALID_AST)
        ssc1.children = children
        updateScope(ssc, ssc1)
        ssc1
    }

    def namespace(wf: Workflow, tasks: Seq[Task]) : WdlNamespaceWithWorkflow = {
        new WdlNamespaceWithWorkflow(None, wf,
                                     Vector.empty, Vector.empty,
                                     tasks,
                                     Map.empty,
                                     WdlRewrite.INVALID_ERR_FORMATTER,
                                     WdlRewrite.INVALID_AST)
    }

    def namespace(task:Task) : WdlNamespaceWithoutWorkflow = {
        new WdlNamespaceWithoutWorkflow(None,
                                        Vector.empty,
                                        Vector.empty,
                                        Vector(task),
                                        Map.empty,
                                        WdlRewrite.INVALID_AST)
    }
}
