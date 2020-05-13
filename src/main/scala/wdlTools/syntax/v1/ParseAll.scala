package wdlTools.syntax.v1

import java.net.URL

import wdlTools.syntax.Antlr4Util.ParseTreeListenerFactory
import wdlTools.syntax.v1.{ConcreteSyntax => CST}
import wdlTools.syntax.{SyntaxException, WdlParser, AbstractSyntax => AST}
import wdlTools.util.{Options, SourceCode}

// parse and follow imports
case class ParseAll(opts: Options,
                    listenerFactories: Vector[ParseTreeListenerFactory] = Vector.empty)
    extends WdlParser(opts) {

  private case class Translator(docSourceUrl: Option[URL] = None) {
    def translateType(t: CST.Type): AST.Type = {
      t match {
        case CST.TypeOptional(t, srcText) =>
          AST.TypeOptional(translateType(t), srcText)
        case CST.TypeArray(t, nonEmpty, srcText) =>
          AST.TypeArray(translateType(t), nonEmpty, srcText)
        case CST.TypeMap(k, v, srcText) =>
          AST.TypeMap(translateType(k), translateType(v), srcText)
        case CST.TypePair(l, r, srcText) =>
          AST.TypePair(translateType(l), translateType(r), srcText)
        case CST.TypeString(srcText)         => AST.TypeString(srcText)
        case CST.TypeFile(srcText)           => AST.TypeFile(srcText)
        case CST.TypeBoolean(srcText)        => AST.TypeBoolean(srcText)
        case CST.TypeInt(srcText)            => AST.TypeInt(srcText)
        case CST.TypeFloat(srcText)          => AST.TypeFloat(srcText)
        case CST.TypeIdentifier(id, srcText) => AST.TypeIdentifier(id, srcText)
        case CST.TypeObject(srcText)         => AST.TypeObject(srcText)
        case CST.TypeStruct(name, members, srcText) =>
          AST.TypeStruct(name, members.map {
            case CST.StructMember(name, t, text) =>
              AST.StructMember(name, translateType(t), text)
          }, srcText)
      }
    }

    def translateExpr(e: CST.Expr): AST.Expr = {
      e match {
        // values
        case CST.ExprString(value, srcText)  => AST.ValueString(value, srcText)
        case CST.ExprBoolean(value, srcText) => AST.ValueBoolean(value, srcText)
        case CST.ExprInt(value, srcText)     => AST.ValueInt(value, srcText)
        case CST.ExprFloat(value, srcText)   => AST.ValueFloat(value, srcText)

        // compound values
        case CST.ExprIdentifier(id, srcText) => AST.ExprIdentifier(id, srcText)
        case CST.ExprCompoundString(vec, srcText) =>
          AST.ExprCompoundString(vec.map(translateExpr), srcText)
        case CST.ExprPair(l, r, srcText) =>
          AST.ExprPair(translateExpr(l), translateExpr(r), srcText)
        case CST.ExprArrayLiteral(vec, srcText) =>
          AST.ExprArray(vec.map(translateExpr), srcText)
        case CST.ExprMapLiteral(m, srcText) =>
          AST.ExprMap(m.map { item =>
            AST.ExprMapItem(translateExpr(item.key), translateExpr(item.value), item.text)
          }, srcText)
        case CST.ExprObjectLiteral(m, srcText) =>
          AST.ExprObject(m.map { member =>
            AST.ExprObjectMember(member.key, translateExpr(member.value), member.text)
          }, srcText)

        // string place holders
        case CST.ExprPlaceholderEqual(t, f, value, srcText) =>
          AST.ExprPlaceholderEqual(translateExpr(t),
                                   translateExpr(f),
                                   translateExpr(value),
                                   srcText)
        case CST.ExprPlaceholderDefault(default, value, srcText) =>
          AST.ExprPlaceholderDefault(translateExpr(default), translateExpr(value), srcText)
        case CST.ExprPlaceholderSep(sep, value, srcText) =>
          AST.ExprPlaceholderSep(translateExpr(sep), translateExpr(value), srcText)

        // operators on one argument
        case CST.ExprUniraryPlus(value, srcText) =>
          AST.ExprUniraryPlus(translateExpr(value), srcText)
        case CST.ExprUniraryMinus(value, srcText) =>
          AST.ExprUniraryMinus(translateExpr(value), srcText)
        case CST.ExprNegate(value, srcText) =>
          AST.ExprNegate(translateExpr(value), srcText)

        // operators on two arguments
        case CST.ExprLor(a, b, srcText) =>
          AST.ExprLor(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprLand(a, b, srcText) =>
          AST.ExprLand(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprEqeq(a, b, srcText) =>
          AST.ExprEqeq(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprLt(a, b, srcText) =>
          AST.ExprLt(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprGte(a, b, srcText) =>
          AST.ExprGte(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprNeq(a, b, srcText) =>
          AST.ExprNeq(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprLte(a, b, srcText) =>
          AST.ExprLte(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprGt(a, b, srcText) =>
          AST.ExprGt(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprAdd(a, b, srcText) =>
          AST.ExprAdd(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprSub(a, b, srcText) =>
          AST.ExprSub(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprMod(a, b, srcText) =>
          AST.ExprMod(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprMul(a, b, srcText) =>
          AST.ExprMul(translateExpr(a), translateExpr(b), srcText)
        case CST.ExprDivide(a, b, srcText) =>
          AST.ExprDivide(translateExpr(a), translateExpr(b), srcText)

        // Access an array element at [index]
        case CST.ExprAt(array, index, srcText) =>
          AST.ExprAt(translateExpr(array), translateExpr(index), srcText)

        case CST.ExprIfThenElse(cond, tBranch, fBranch, srcText) =>
          AST.ExprIfThenElse(translateExpr(cond),
                             translateExpr(tBranch),
                             translateExpr(fBranch),
                             srcText)
        case CST.ExprApply(funcName, elements, srcText) =>
          AST.ExprApply(funcName, elements.map(translateExpr), srcText)
        case CST.ExprGetName(e, id, srcText) =>
          AST.ExprGetName(translateExpr(e), id, srcText)

        case other =>
          throw new Exception(s"invalid concrete syntax element ${other}")
      }
    }

    // The meta values are a subset of the expression syntax.
    //
    // $meta_value = $string | $number | $boolean | 'null' | $meta_object | $meta_array
    // $meta_object = '{}' | '{' $parameter_meta_kv (, $parameter_meta_kv)* '}'
    // $meta_array = '[]' |  '[' $meta_value (, $meta_value)* ']'
    //
    private def translateMetaValue(value: CST.Expr): AST.Expr = {
      value match {
        // values
        case CST.ExprString(value, srcText)  => AST.ValueString(value, srcText)
        case CST.ExprBoolean(value, srcText) => AST.ValueBoolean(value, srcText)
        case CST.ExprInt(value, srcText)     => AST.ValueInt(value, srcText)
        case CST.ExprFloat(value, srcText)   => AST.ValueFloat(value, srcText)

        // special handling for null. It appears as an identifier here, even though
        // it has not been defined, and it is no identifier.
        case CST.ExprIdentifier(id, srcText) if id == "null" =>
          AST.ExprIdentifier(id, srcText)
        case CST.ExprIdentifier(id, srcText) =>
          throw new SyntaxException(s"cannot access identifier (${id}) in a meta section",
                                    srcText,
                                    docSourceUrl)

        // compound values
        case CST.ExprPair(l, r, srcText) =>
          AST.ExprPair(translateMetaValue(l), translateMetaValue(r), srcText)
        case CST.ExprArrayLiteral(vec, srcText) =>
          AST.ExprArray(vec.map(translateMetaValue), srcText)
        case CST.ExprMapLiteral(m, srcText) =>
          AST.ExprMap(
              m.map {
                case CST.ExprMapItem(CST.ExprIdentifier(k, text2), value, text) =>
                  AST.ExprMapItem(AST.ExprIdentifier(k, text2), translateMetaValue(value), text)
                case _ =>
                  throw new SyntaxException(s"Illegal meta field value", srcText, docSourceUrl)
              },
              srcText
          )
        case CST.ExprObjectLiteral(m, srcText) =>
          AST.ExprObject(m.map {
            case CST.ExprObjectMember(fieldName, v, text) =>
              AST.ExprObjectMember(fieldName, translateMetaValue(v), text)
          }, srcText)

        case other =>
          throw new SyntaxException("illegal expression in meta section", other.text, docSourceUrl)
      }
    }

    def translateMetaKV(kv: CST.MetaKV): AST.MetaKV = {
      AST.MetaKV(kv.id, translateMetaValue(kv.expr), kv.text)
    }

    def translateInputSection(
        inp: CST.InputSection
    ): AST.InputSection = {
      AST.InputSection(inp.declarations.map(translateDeclaration), inp.text)
    }

    def translateOutputSection(
        output: CST.OutputSection
    ): AST.OutputSection = {
      AST.OutputSection(output.declarations.map(translateDeclaration), output.text)
    }

    def translateCommandSection(
        cs: CST.CommandSection
    ): AST.CommandSection = {
      AST.CommandSection(cs.parts.map(translateExpr), cs.text)
    }

    def translateDeclaration(decl: CST.Declaration): AST.Declaration = {
      AST.Declaration(decl.name,
                      translateType(decl.wdlType),
                      decl.expr.map(translateExpr),
                      decl.text)
    }

    def translateMetaSection(meta: CST.MetaSection): AST.MetaSection = {
      AST.MetaSection(meta.kvs.map(translateMetaKV), meta.text)
    }

    def translateParameterMetaSection(
        paramMeta: CST.ParameterMetaSection
    ): AST.ParameterMetaSection = {
      AST.ParameterMetaSection(paramMeta.kvs.map(translateMetaKV), paramMeta.text)
    }

    def translateRuntimeSection(
        runtime: CST.RuntimeSection
    ): AST.RuntimeSection = {
      // check for duplicate ids
      runtime.kvs.foldLeft(Set.empty[String]) {
        case (accu, kv) if !accu.contains(kv.id) => accu + kv.id
        case (_, kv) =>
          throw new SyntaxException(s"key ${kv.id} defined twice in runtime section",
                                    kv.text,
                                    docSourceUrl)
      }

      AST.RuntimeSection(
          runtime.kvs.map {
            case CST.RuntimeKV(id, expr, text) =>
              AST.RuntimeKV(id, translateExpr(expr), text)
          },
          runtime.text
      )
    }

    def translateWorkflowElement(
        elem: CST.WorkflowElement
    ): AST.WorkflowElement = {
      elem match {
        case CST.Declaration(name, wdlType, expr, text) =>
          AST.Declaration(name, translateType(wdlType), expr.map(translateExpr), text)

        case CST.Call(name, alias, inputs, text) =>
          AST.Call(
              name,
              alias.map {
                case CST.CallAlias(callName, callText) =>
                  AST.CallAlias(callName, callText)
              },
              Vector.empty,
              inputs.map {
                case CST.CallInputs(inputsMap, inputsText) =>
                  AST.CallInputs(inputsMap.map { inp =>
                    AST.CallInput(inp.name, translateExpr(inp.expr), inp.text)
                  }, inputsText)
              },
              text
          )

        case CST.Scatter(identifier, expr, body, text) =>
          AST.Scatter(identifier, translateExpr(expr), body.map(translateWorkflowElement), text)

        case CST.Conditional(expr, body, text) =>
          AST.Conditional(translateExpr(expr), body.map(translateWorkflowElement), text)
      }
    }

    def translateWorkflow(wf: CST.Workflow): AST.Workflow = {
      AST.Workflow(
          wf.name,
          wf.input.map(translateInputSection),
          wf.output.map(translateOutputSection),
          wf.meta.map(translateMetaSection),
          wf.parameterMeta.map(translateParameterMetaSection),
          wf.body.map(translateWorkflowElement),
          wf.text
      )
    }

    def translateStruct(struct: CST.TypeStruct): AST.TypeStruct = {
      AST.TypeStruct(
          struct.name,
          struct.members.map {
            case CST.StructMember(name, t, memberText) =>
              AST.StructMember(name, translateType(t), memberText)
          },
          struct.text
      )
    }

    def translateImportDoc(importDoc: CST.ImportDoc,
                           importedDoc: Option[AST.Document]): AST.ImportDoc = {
      val addrAbst = AST.ImportAddr(importDoc.addr.value, importDoc.text)
      val nameAbst = importDoc.name.map {
        case CST.ImportName(value, text) => AST.ImportName(value, text)
      }
      val aliasesAbst: Vector[AST.ImportAlias] = importDoc.aliases.map {
        case CST.ImportAlias(x, y, alText) => AST.ImportAlias(x, y, alText)
      }

      // Replace the original statement with a new one
      AST.ImportDoc(nameAbst, aliasesAbst, addrAbst, importedDoc, importDoc.text)
    }

    def translateTask(task: CST.Task): AST.Task = {
      AST.Task(
          task.name,
          task.input.map(translateInputSection),
          task.output.map(translateOutputSection),
          translateCommandSection(task.command),
          task.declarations.map(translateDeclaration),
          task.meta.map(translateMetaSection),
          task.parameterMeta.map(translateParameterMetaSection),
          task.runtime.map(translateRuntimeSection),
          None,
          task.text
      )
    }

    // start from a document [doc], and recursively dive into all the imported
    // documents. Replace all the raw import statements with fully elaborated ones.
    def translateDocument(doc: ConcreteSyntax.Document): AST.Document = {
      // translate all the elements of the document to the abstract syntax
      val elems: Vector[AST.DocumentElement] = doc.elements.map {
        case struct: ConcreteSyntax.TypeStruct => translateStruct(struct)
        case importDoc: ConcreteSyntax.ImportDoc =>
          val importedDoc = if (opts.followImports) {
            Some(followImport(getDocSourceUrl(importDoc.addr.value)))
          } else {
            None
          }
          translateImportDoc(importDoc, importedDoc)
        case task: ConcreteSyntax.Task => translateTask(task)
        case other                     => throw new Exception(s"unrecognized document element ${other}")
      }
      val aWf = doc.workflow.map(translateWorkflow)
      val version = AST.Version(doc.version.value, doc.version.text)
      AST.Document(doc.docSourceUrl, doc.docSource, version, elems, aWf, doc.text, doc.comments)
    }
  }
  private val versionRegexp = "version\\s+1.0.*".r

  override def canParse(sourceCode: SourceCode): Boolean = {
    sourceCode.lines.foreach { line =>
      if (!(line.trim.isEmpty || line.startsWith("#"))) {
        return versionRegexp.matches(line.trim)
      }
    }
    false
  }

  override def parseDocument(sourceCode: SourceCode): AST.Document = {
    val grammar = WdlV1Grammar.newInstance(sourceCode, listenerFactories, opts)
    val visitor = ParseTop(opts, grammar)
    val top: ConcreteSyntax.Document = visitor.parseDocument
    val errorListener = grammar.errListener
    if (errorListener.hasErrors) {
      throw new SyntaxException(errorListener.getErrors)
    }
    val translator = Translator(Some(sourceCode.url))
    translator.translateDocument(top)
  }

  override def parseExpr(text: String): AST.Expr = {
    val parser = ParseTop(opts, WdlV1Grammar.newInstance(text, listenerFactories, opts = opts))
    val translator = Translator()
    translator.translateExpr(parser.parseExpr)
  }

  override def parseType(text: String): AST.Type = {
    val parser = ParseTop(opts, WdlV1Grammar.newInstance(text, listenerFactories, opts = opts))
    val translator = Translator()
    translator.translateType(parser.parseWdlType)
  }
}
