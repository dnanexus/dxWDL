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

import com.dnanexus.{DXRecord}
import wdl._
import wdl.AstTools
import wdl4s.parser.WdlParser.{Ast, Terminal}
import wom.core.WorkflowSource
import wom.types._
import wom.values._

object WdlRewrite {
    val INVALID_AST = AstTools.getAst("", "")
    val INVALID_ERR_FORMATTER = WdlSyntaxErrorFormatter(Map.empty[Terminal, WorkflowSource])

    // copy references from old scope to newly minted scope.
    private def updateScope(old: Scope, fresh: Scope) : Unit = {
        fresh.namespace = old.namespace
        old.parent match {
            case Some(x) => fresh.parent = x
            case None => ()
        }
    }

    // Create a declaration.
    def declaration(wdlType: WomType,
                    name: String,
                    expr: Option[WdlExpression]) : Declaration = {
        val textualRepr = expr match {
            case None => s"${wdlType.toDisplayString} ${name}"
            case Some(e) => s"${wdlType.toDisplayString} ${name} = ${e.toWomString}"
        }
        val ast: Ast = AstTools.getAst(textualRepr, "")
        Declaration(wdlType, name, expr, None, ast)
    }

    // Modify the inputs in a task-call
    def taskCall(tc: WdlTaskCall,
                 inputMappings: Map[String, WdlExpression]) : WdlTaskCall = {
        val tc1 = WdlTaskCall(tc.alias, tc.task, inputMappings, tc.ast)
        tc1.children = tc.children
        updateScope(tc, tc1)
        tc1
    }

    // Create an empty task.
    def taskGenEmpty(name: String,
                     meta: Map[String, String],
                     scope: Scope) : WdlTask = {
        val task = new WdlTask(name,
                               Vector.empty,  // command Template
                               new WdlRuntimeAttributes(Map.empty[String,WdlExpression]),
                               meta,
                               Map.empty[String, String], // parameter meta
                               scope.ast)
        updateScope(scope, task)
        task
    }

    // Replace the docker string with a dxURL that does not require
    // runtime search. For example:
    //
    //   dx://dxWDL_playground:/glnexus_internal  ->   dx://record-xxxx
    def taskReplaceDockerValue(task:WdlTask, dxRecord:DXRecord) : WdlTask = {
        val attrs:Map[String, WdlExpression] = task.runtimeAttributes.attrs
        val dxid = dxRecord.getId()
        val url:String = s""" "${Utils.DX_URL_PREFIX}${dxid}" """
        val urlExpr:WdlExpression = WdlExpression.fromString(url)
        val cleaned = attrs + ("docker" -> urlExpr)
        val task2 = new WdlTask(task.name,
                                task.commandTemplate,
                                WdlRuntimeAttributes(cleaned),
                                task.meta,
                                task.parameterMeta,
                                INVALID_AST)
        task2.children = task.children
        updateScope(task, task2)
        task2
    }

    private def genDefaultValueOfType(wdlType: WomType) : WomValue = {
        wdlType match {
            case WomBooleanType => WomBoolean(true)
            case WomIntegerType => WomInteger(0)
            case WomFloatType => WomFloat(0.0)
            case WomStringType => WomString("")
            case WomFileType => WomFile("")

            case WomOptionalType(t) => genDefaultValueOfType(t)
            case WomMaybeEmptyArrayType(t) =>
                // an empty array
                WomArray(WomMaybeEmptyArrayType(t), List())
            case WomNonEmptyArrayType(t) =>
                // Non empty array
                WomArray(WomNonEmptyArrayType(t), List(genDefaultValueOfType(t)))
            case WomMapType(keyType, valueType) =>
                WomMap(WomMapType(keyType, valueType), Map.empty)
            case WomObjectType => WomObject(Map.empty)
            case WomPairType(lType, rType) => WomPair(genDefaultValueOfType(lType),
                                                      genDefaultValueOfType(rType))
            case _ => throw new Exception(s"Unhandled type ${wdlType.toDisplayString}")
        }
    }


    def taskOutput(name: String, wdlType: WomType, scope: Scope) = {
        // We need to provide a default value, in the form of a Wdl
        // expression
        val defaultVal:WomValue = genDefaultValueOfType(wdlType)
        val defaultExpr:WdlExpression = WdlExpression.fromString(defaultVal.toWomString)

        new TaskOutput(name, wdlType, defaultExpr, INVALID_AST, Some(scope))
    }

    // Modify the children in a workflow.
    def workflow[Child <: Scope] (wfOld: WdlWorkflow,
                                  children: Seq[Child]): WdlWorkflow = {
        val wf = new WdlWorkflow(wfOld.unqualifiedName,
                                 Seq.empty, // no output wildcards
                                 wfOld.wdlSyntaxErrorFormatter,
                                 wfOld.meta, wfOld.parameterMeta,
                                 wfOld.ast)
        wf.children = children
        updateScope(wfOld, wf)
        wf
    }

    def workflowGenEmpty(wfName: String) : WdlWorkflow = {
        new WdlWorkflow(wfName,
                        List.empty,
                        INVALID_ERR_FORMATTER,
                        Map.empty[String, String],
                        Map.empty[String, String],
                        INVALID_AST)
    }

    def workflowOutput(varName: String,
                       wdlType: WomType,
                       scope: Scope) = {
        new WorkflowOutput("out_" + varName,
                           wdlType,
                           WdlExpression.fromString(varName),
                           INVALID_AST,
                           Some(scope))
    }

    def workflowOutput(varName: String,
                       wdlType: WomType,
                       expr: WdlExpression) : WorkflowOutput = {
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

    def cond [Child <: Scope] (old: If,
                               children: Seq[Child],
                               condition: WdlExpression): If = {
        val fresh = wdl.If(old.index, condition, INVALID_AST)
        fresh.children = children
        updateScope(old, fresh)
        fresh
    }

    def namespace(wf: WdlWorkflow, tasks: Seq[WdlTask]) : WdlNamespaceWithWorkflow = {
        new WdlNamespaceWithWorkflow(None, wf,
                                     Vector.empty, Vector.empty,
                                     tasks,
                                     Map.empty,
                                     WdlRewrite.INVALID_ERR_FORMATTER,
                                     WdlRewrite.INVALID_AST,
                                     "", // sourceString: What does this argument do?
                                     None)
    }

    def namespace(old: WdlNamespaceWithWorkflow, wf: WdlWorkflow) : WdlNamespaceWithWorkflow = {
        val fresh = new WdlNamespaceWithWorkflow(old.importedAs,
                                                 wf,
                                                 old.imports,
                                                 old.namespaces,
                                                 old.tasks,
                                                 Map.empty,
                                                 WdlRewrite.INVALID_ERR_FORMATTER,
                                                 WdlRewrite.INVALID_AST,
                                                 "", // sourceString: What does this argument do?
                                                 None)
        updateScope(old, fresh)
        fresh
    }

    def namespace(task:WdlTask) : WdlNamespaceWithoutWorkflow = {
        new WdlNamespaceWithoutWorkflow(None,
                                        Vector.empty,
                                        Vector.empty,
                                        Vector(task),
                                        Map.empty,
                                        WdlRewrite.INVALID_AST,
                                        "", // sourceString: What does this argument do?
                                        None)
    }

    def namespace(tasks:Vector[WdlTask]) : WdlNamespaceWithoutWorkflow = {
        new WdlNamespaceWithoutWorkflow(None,
                                        Vector.empty,
                                        Vector.empty,
                                        tasks,
                                        Map.empty,
                                        WdlRewrite.INVALID_AST,
                                        "", // sourceString: What does this argument do?
                                        None)
    }

    def namespaceEmpty() : WdlNamespaceWithoutWorkflow = {
        new WdlNamespaceWithoutWorkflow(None,
                                        Vector.empty,
                                        Vector.empty,
                                        Vector.empty,
                                        Map.empty,
                                        WdlRewrite.INVALID_AST,
                                        "", // sourceString: What does this argument do?
                                        None)
    }

}
