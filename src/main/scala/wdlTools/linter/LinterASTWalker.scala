package wdlTools.linter

import wdlTools.syntax.ASTVisitor.Context
import wdlTools.syntax.{ASTVisitor, ASTWalker}
import wdlTools.syntax.AbstractSyntax._
import wdlTools.util.Options

case class LinterASTWalker(opts: Options, visitors: Vector[ASTVisitor]) extends ASTWalker(opts) {
  def visitEveryRule(ctx: Context[Element]): Unit = {}

  override def visitDocument(ctx: Context[Document]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitDocument(ctx))
    super.visitDocument(ctx)
  }

  override def visitIdentifier[P <: Element](identifier: String, parent: Context[P]): Unit = {
    visitors.foreach(_.visitIdentifier(identifier, parent))
    super.visitIdentifier(identifier, parent)
  }

  override def visitVersion(ctx: Context[Version]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitVersion(ctx))
    super.visitVersion(ctx)
  }

  override def visitImportAlias(ctx: Context[ImportAlias]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitImportAlias(ctx))
    super.visitImportAlias(ctx)
  }

  override def visitImportDoc(ctx: Context[ImportDoc]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitImportDoc(ctx))
    super.visitImportDoc(ctx)
  }

  override def visitStruct(ctx: Context[TypeStruct]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitStruct(ctx))
    super.visitStruct(ctx)
  }

  override def visitDataType(ctx: Context[Type]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitDataType(ctx))
    super.visitDataType(ctx)
  }

  override def visitStructMember(ctx: Context[StructMember]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitStructMember(ctx))
    super.visitStructMember(ctx)
  }

  override def visitExpression(ctx: Context[Expr]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitExpression(ctx))
    super.visitExpression(ctx)
  }

  override def visitDeclaration(ctx: Context[Declaration]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitDeclaration(ctx))
    super.visitDeclaration(ctx)
  }

  override def visitInputSection(ctx: Context[InputSection]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitInputSection(ctx))
    super.visitInputSection(ctx)
  }

  override def visitOutputSection(ctx: Context[OutputSection]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitOutputSection(ctx))
    super.visitOutputSection(ctx)
  }

  override def visitCallAlias(ctx: Context[CallAlias]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitCallAlias(ctx))
    super.visitCallAlias(ctx)
  }

  override def visitCallInput(ctx: Context[CallInput]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitCallInput(ctx))
    super.visitCallInput(ctx)
  }

  override def visitCall(ctx: Context[Call]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitCall(ctx))
    super.visitCall(ctx)
  }

  override def visitScatter(ctx: Context[Scatter]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitScatter(ctx))
    super.visitScatter(ctx)
  }

  override def visitConditional(ctx: Context[Conditional]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitConditional(ctx))
    super.visitConditional(ctx)
  }

  override def visitBody[P <: Element](body: Vector[WorkflowElement], ctx: Context[P]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitBody(body, ctx))
    super.visitBody(body, ctx)
  }

  override def visitMetaKV(ctx: Context[MetaKV]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitMetaKV(ctx))
    super.visitMetaKV(ctx)
  }

  override def visitMetaSection(ctx: Context[MetaSection]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitMetaSection(ctx))
    super.visitMetaSection(ctx)
  }

  override def visitParameterMetaSection(
      ctx: Context[ParameterMetaSection]
  ): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitParameterMetaSection(ctx))
    super.visitParameterMetaSection(ctx)
  }

  override def visitWorkflow(ctx: Context[Workflow]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitWorkflow(ctx))
    super.visitWorkflow(ctx)
  }

  override def visitCommandSection(ctx: Context[CommandSection]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitCommandSection(ctx))
    super.visitCommandSection(ctx)
  }

  override def visitRuntimeKV(ctx: Context[RuntimeKV]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitRuntimeKV(ctx))
    super.visitRuntimeKV(ctx)
  }

  override def visitRuntimeSection(ctx: Context[RuntimeSection]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitRuntimeSection(ctx))
    super.visitRuntimeSection(ctx)
  }

  override def visitTask(ctx: Context[Task]): Unit = {
    visitEveryRule(ctx.asInstanceOf[Context[Element]])
    visitors.foreach(_.visitTask(ctx))
    super.visitTask(ctx)
  }
}
