package wdlTools.syntax

import wdlTools.syntax.ASTVisitor.Context
import wdlTools.syntax.AbstractSyntax._
import wdlTools.util.Options

import scala.annotation.tailrec
import scala.reflect.ClassTag

class ASTVisitor {
  def visitDocument(ctx: Context[Document]): Unit = {}

  def visitIdentifier[P <: Element](identifier: String, parent: Context[P]): Unit = {}

  def visitVersion(ctx: Context[Version]): Unit = {}

  def visitImportName(ctx: Context[ImportName]): Unit = {}

  def visitImportAlias(ctx: Context[ImportAlias]): Unit = {}

  def visitImportDoc(ctx: Context[ImportDoc]): Unit = {}

  def visitStruct(ctx: Context[TypeStruct]): Unit = {}

  def visitDataType(ctx: Context[Type]): Unit = {}

  def visitStructMember(ctx: Context[StructMember]): Unit = {}

  def visitExpression(ctx: Context[Expr]): Unit = {}

  def visitDeclaration(ctx: Context[Declaration]): Unit = {}

  def visitInputSection(ctx: Context[InputSection]): Unit = {}

  def visitOutputSection(ctx: Context[OutputSection]): Unit = {}

  def visitCallAlias(ctx: Context[CallAlias]): Unit = {}

  def visitCallInput(ctx: Context[CallInput]): Unit = {}

  def visitCall(ctx: Context[Call]): Unit = {}

  def visitScatter(ctx: Context[Scatter]): Unit = {}

  def visitConditional(ctx: Context[Conditional]): Unit = {}

  def visitBody[P <: Element](body: Vector[WorkflowElement], ctx: Context[P]): Unit = {}

  def visitMetaKV(ctx: Context[MetaKV]): Unit = {}

  def visitMetaSection(ctx: Context[MetaSection]): Unit = {}

  def visitParameterMetaSection(ctx: Context[ParameterMetaSection]): Unit = {}

  def visitWorkflow(ctx: Context[Workflow]): Unit = {}

  def visitCommandSection(ctx: Context[CommandSection]): Unit = {}

  def visitRuntimeKV(ctx: Context[RuntimeKV]): Unit = {}

  def visitRuntimeSection(ctx: Context[RuntimeSection]): Unit = {}

  def visitTask(ctx: Context[Task]): Unit = {}
}

object ASTVisitor {
  class Context[T <: Element](val element: T, val parent: Option[Context[_]] = None) {
    def getParent[P <: Element]: Context[P] = {
      if (parent.isDefined) {
        parent.get.asInstanceOf[Context[P]]
      } else {
        throw new Exception("Context does not have a parent")
      }
    }

    def getParentExecutable: Option[Element] = {
      @tailrec
      def getExecutable(ctx: Context[_]): Option[Element] = {
        ctx.element match {
          case t: Task                   => Some(t)
          case w: Workflow               => Some(w)
          case _ if ctx.parent.isDefined => getExecutable(ctx.parent.get)
          case _                         => None
        }
      }
      getExecutable(this)
    }

    def findParent[P <: Element](implicit tag: ClassTag[P]): Option[Context[P]] = {
      if (parent.isDefined) {
        @tailrec
        def find(ctx: Context[_]): Option[Context[P]] = {
          ctx.element match {
            case _: P                      => Some(ctx.asInstanceOf[Context[P]])
            case _ if ctx.parent.isDefined => find(ctx.parent.get)
            case _                         => None
          }
        }
        find(this.parent.get)
      } else {
        None
      }
    }
  }
}

class ASTWalker(opts: Options) extends ASTVisitor {
  override def visitDocument(ctx: Context[Document]): Unit = {
    visitVersion(createContext[Version, Document](ctx.element.version, ctx))

    ctx.element.elements.collect { case imp: ImportDoc => imp }.foreach { imp =>
      visitImportDoc(createContext[ImportDoc, Document](imp, ctx))
    }

    ctx.element.elements.collect { case struct: TypeStruct => struct }.foreach { imp =>
      visitStruct(createContext[TypeStruct, Document](imp, ctx))
    }

    if (ctx.element.workflow.isDefined) {
      visitWorkflow(createContext[Workflow, Document](ctx.element.workflow.get, ctx))
    }

    ctx.element.elements.collect { case task: Task => task }.foreach { task =>
      visitTask(createContext[Task, Document](task, ctx))
    }
  }

  override def visitImportName(ctx: Context[ImportName]): Unit = {
    visitIdentifier[ImportName](ctx.element.value, ctx)
  }

  override def visitImportAlias(ctx: Context[ImportAlias]): Unit = {
    visitIdentifier[ImportAlias](ctx.element.id1, ctx)
    visitIdentifier[ImportAlias](ctx.element.id2, ctx)
  }

  override def visitImportDoc(ctx: Context[ImportDoc]): Unit = {
    if (ctx.element.name.isDefined) {
      visitImportName(createContext[ImportName, ImportDoc](ctx.element.name.get, ctx))
    }
    ctx.element.aliases.foreach { alias =>
      visitImportAlias(createContext[ImportAlias, ImportDoc](alias, ctx))
    }
    if (opts.followImports) {
      visitDocument(createContext[Document, ImportDoc](ctx.element.doc.get, ctx))
    }
  }

  override def visitStruct(ctx: Context[TypeStruct]): Unit = {
    ctx.element.members.foreach { member =>
      visitStructMember(createContext[StructMember, TypeStruct](member, ctx))
    }
  }

  override def visitStructMember(ctx: Context[StructMember]): Unit = {
    visitDataType(createContext[Type, StructMember](ctx.element.dataType, ctx))
    visitIdentifier[StructMember](ctx.element.name, ctx)
  }

  override def visitDeclaration(ctx: Context[Declaration]): Unit = {
    visitDataType(createContext[Type, Declaration](ctx.element.wdlType, ctx))
    visitIdentifier[Declaration](ctx.element.name, ctx)
    if (ctx.element.expr.isDefined) {
      visitExpression(createContext[Expr, Declaration](ctx.element.expr.get, ctx))
    }
  }

  override def visitInputSection(ctx: Context[InputSection]): Unit = {
    ctx.element.declarations.foreach { decl =>
      visitDeclaration(createContext[Declaration, InputSection](decl, ctx))
    }
  }

  override def visitOutputSection(ctx: Context[OutputSection]): Unit = {
    ctx.element.declarations.foreach { decl =>
      visitDeclaration(createContext[Declaration, OutputSection](decl, ctx))
    }
  }

  override def visitCallAlias(ctx: Context[CallAlias]): Unit = {
    visitIdentifier[CallAlias](ctx.element.name, ctx)
  }

  override def visitCallInput(ctx: Context[CallInput]): Unit = {
    visitIdentifier[CallInput](ctx.element.name, ctx)
    visitExpression(createContext[Expr, CallInput](ctx.element.expr, ctx))
  }

  override def visitCall(ctx: Context[Call]): Unit = {
    visitIdentifier[Call](ctx.element.name, ctx)
    if (ctx.element.alias.isDefined) {
      visitCallAlias(createContext[CallAlias, Call](ctx.element.alias.get, ctx))
    }
    if (ctx.element.inputs.isDefined) {
      ctx.element.inputs.get.value.foreach { inp =>
        visitCallInput(createContext[CallInput, Call](inp, ctx))
      }
    }
  }

  override def visitScatter(ctx: Context[Scatter]): Unit = {
    visitIdentifier[Scatter](ctx.element.identifier, ctx)
    visitExpression(createContext[Expr, Scatter](ctx.element.expr, ctx))
    visitBody[Scatter](ctx.element.body, ctx)
  }

  override def visitConditional(ctx: Context[Conditional]): Unit = {
    visitExpression(createContext[Expr, Conditional](ctx.element.expr, ctx))
    visitBody[Conditional](ctx.element.body, ctx)
  }

  override def visitBody[P <: Element](body: Vector[WorkflowElement], ctx: Context[P]): Unit = {
    body.foreach {
      case decl: Declaration => visitDeclaration(createContext[Declaration, P](decl, ctx))
      case call: Call        => visitCall(createContext[Call, P](call, ctx))
      case scatter: Scatter  => visitScatter(createContext[Scatter, P](scatter, ctx))
      case conditional: Conditional =>
        visitConditional(createContext[Conditional, P](conditional, ctx))
      case other => throw new Exception(s"Unexpected workflow element ${other}")
    }
  }

  override def visitMetaKV(ctx: Context[MetaKV]): Unit = {
    visitIdentifier[MetaKV](ctx.element.id, ctx)
    visitExpression(createContext[Expr, MetaKV](ctx.element.expr, ctx))
  }

  override def visitMetaSection(ctx: Context[MetaSection]): Unit = {
    ctx.element.kvs.foreach { kv =>
      visitMetaKV(createContext[MetaKV, MetaSection](kv, ctx))
    }
  }

  override def visitParameterMetaSection(ctx: Context[ParameterMetaSection]): Unit = {
    ctx.element.kvs.foreach { kv =>
      visitMetaKV(createContext[MetaKV, ParameterMetaSection](kv, ctx))
    }
  }

  override def visitWorkflow(ctx: Context[Workflow]): Unit = {
    if (ctx.element.input.isDefined) {
      visitInputSection(createContext[InputSection, Workflow](ctx.element.input.get, ctx))
    }

    visitBody[Workflow](ctx.element.body, ctx)

    if (ctx.element.output.isDefined) {
      visitOutputSection(createContext[OutputSection, Workflow](ctx.element.output.get, ctx))
    }

    if (ctx.element.meta.isDefined) {
      visitMetaSection(createContext[MetaSection, Workflow](ctx.element.meta.get, ctx))
    }

    if (ctx.element.parameterMeta.isDefined) {
      visitParameterMetaSection(
          createContext[ParameterMetaSection, Workflow](ctx.element.parameterMeta.get, ctx)
      )
    }
  }

  override def visitCommandSection(ctx: Context[CommandSection]): Unit = {
    ctx.element.parts.foreach { expr =>
      visitExpression(createContext[Expr, CommandSection](expr, ctx))
    }
  }

  override def visitRuntimeKV(ctx: Context[RuntimeKV]): Unit = {
    visitIdentifier[RuntimeKV](ctx.element.id, ctx)
    visitExpression(createContext[Expr, RuntimeKV](ctx.element.expr, ctx))
  }

  override def visitRuntimeSection(ctx: Context[RuntimeSection]): Unit = {
    ctx.element.kvs.foreach { kv =>
      visitRuntimeKV(createContext[RuntimeKV, RuntimeSection](kv, ctx))
    }
  }

  override def visitTask(ctx: Context[Task]): Unit = {
    if (ctx.element.input.isDefined) {
      visitInputSection(createContext[InputSection, Task](ctx.element.input.get, ctx))
    }

    visitCommandSection(createContext[CommandSection, Task](ctx.element.command, ctx))

    if (ctx.element.output.isDefined) {
      visitOutputSection(createContext[OutputSection, Task](ctx.element.output.get, ctx))
    }

    if (ctx.element.runtime.isDefined) {
      visitRuntimeSection(createContext[RuntimeSection, Task](ctx.element.runtime.get, ctx))
    }

    if (ctx.element.meta.isDefined) {
      visitMetaSection(createContext[MetaSection, Task](ctx.element.meta.get, ctx))
    }

    if (ctx.element.parameterMeta.isDefined) {
      visitParameterMetaSection(
          createContext[ParameterMetaSection, Task](ctx.element.parameterMeta.get, ctx)
      )
    }
  }

  def apply(doc: Document): Unit = {
    val ctx = createContext[Document](doc)
    visitDocument(ctx)
  }

  def createContext[T <: Element](element: T): Context[T] = {
    new Context[T](element)
  }

  def createContext[T <: Element, P <: Element](element: T, parent: Context[P]): Context[T] = {
    new Context[T](element, Some(parent.asInstanceOf[Context[Element]]))
  }
}
