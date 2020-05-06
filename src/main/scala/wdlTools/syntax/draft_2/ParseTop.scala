package wdlTools.syntax.draft_2

// Parse one document. Do not follow imports.

import java.net.URL

import scala.jdk.CollectionConverters._
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.TerminalNode
import org.openwdl.wdl.parser.draft_2.WdlDraft2Parser.{
  Task_command_elementContext,
  Task_meta_elementContext,
  Task_output_elementContext,
  Task_parameter_meta_elementContext,
  Task_runtime_elementContext
}
import org.openwdl.wdl.parser.draft_2._
import wdlTools.syntax.Antlr4Util.{Grammar, getTextSource}
import wdlTools.syntax.draft_2.ConcreteSyntax._
import wdlTools.syntax.{Comment, CommentMap, SyntaxException, TextSource}
import wdlTools.util.Options

import scala.collection.mutable

case class ParseTop(opts: Options,
                    grammar: Grammar[WdlDraft2Lexer, WdlDraft2Parser],
                    docSource: String,
                    docSourceURL: Option[URL] = None)
    extends WdlDraft2ParserBaseVisitor[Element] {
  private def getIdentifierText(identifier: TerminalNode, ctx: ParserRuleContext): String = {
    if (identifier == null) {
      throw new SyntaxException("missing identifier", getTextSource(ctx), docSourceURL)
    }
    identifier.getText
  }

  /*
map_type
	: MAP LBRACK wdl_type COMMA wdl_type RBRACK
	;
   */
  override def visitMap_type(ctx: WdlDraft2Parser.Map_typeContext): Type = {
    val kt: Type = visitWdl_type(ctx.wdl_type(0))
    val vt: Type = visitWdl_type(ctx.wdl_type(1))
    TypeMap(kt, vt, getTextSource(ctx))
  }

  /*
array_type
	: ARRAY LBRACK wdl_type RBRACK PLUS?
	;
   */
  override def visitArray_type(ctx: WdlDraft2Parser.Array_typeContext): Type = {
    val t: Type = visitWdl_type(ctx.wdl_type())
    val nonEmpty = ctx.PLUS() != null
    TypeArray(t, nonEmpty, getTextSource(ctx))
  }

  /*
pair_type
	: PAIR LBRACK wdl_type COMMA wdl_type RBRACK
	;
   */
  override def visitPair_type(ctx: WdlDraft2Parser.Pair_typeContext): Type = {
    val lt: Type = visitWdl_type(ctx.wdl_type(0))
    val rt: Type = visitWdl_type(ctx.wdl_type(1))
    TypePair(lt, rt, getTextSource(ctx))
  }

  /*
type_base
	: array_type
	| map_type
	| pair_type
	| (STRING | FILE | BOOLEAN | OBJECT | INT | FLOAT | Identifier)
	;
   */
  override def visitType_base(ctx: WdlDraft2Parser.Type_baseContext): Type = {
    if (ctx.array_type() != null)
      return visitArray_type(ctx.array_type())
    if (ctx.map_type() != null)
      return visitMap_type(ctx.map_type())
    if (ctx.pair_type() != null)
      return visitPair_type(ctx.pair_type())
    if (ctx.STRING() != null)
      return TypeString(getTextSource(ctx))
    if (ctx.FILE() != null)
      return TypeFile(getTextSource(ctx))
    if (ctx.BOOLEAN() != null)
      return TypeBoolean(getTextSource(ctx))
    if (ctx.OBJECT() != null)
      return TypeObject(getTextSource(ctx))
    if (ctx.INT() != null)
      return TypeInt(getTextSource(ctx))
    if (ctx.FLOAT() != null)
      return TypeFloat(getTextSource(ctx))
    throw new SyntaxException("sanity: unrecgonized type case", getTextSource(ctx), docSourceURL)
  }

  /*
wdl_type
  : (type_base OPTIONAL | type_base)
  ;
   */
  override def visitWdl_type(ctx: WdlDraft2Parser.Wdl_typeContext): Type = {
    val t = visitType_base(ctx.type_base())
    if (ctx.OPTIONAL() != null) {
      TypeOptional(t, getTextSource(ctx))
    } else {
      t
    }
  }

  // EXPRESSIONS

  override def visitNumber(ctx: WdlDraft2Parser.NumberContext): Expr = {
    if (ctx.IntLiteral() != null) {
      return ExprInt(ctx.getText.toInt, getTextSource(ctx))
    }
    if (ctx.FloatLiteral() != null) {
      return ExprFloat(ctx.getText.toDouble, getTextSource(ctx))
    }
    throw new SyntaxException("Not an integer nor a float", getTextSource(ctx), docSourceURL)
  }

  /* expression_placeholder_option
  : BoolLiteral EQUAL (string | number)
  | DEFAULT EQUAL (string | number)
  | SEP EQUAL (string | number)
  ; */
  override def visitExpression_placeholder_option(
      ctx: WdlDraft2Parser.Expression_placeholder_optionContext
  ): PlaceHolderPart = {
    val expr: Expr =
      if (ctx.string() != null)
        visitString(ctx.string())
      else if (ctx.number() != null)
        visitNumber(ctx.number())
      else
        throw new SyntaxException("sanity: not a string or a number",
                                  getTextSource(ctx),
                                  docSourceURL)

    if (ctx.BoolLiteral() != null) {
      val b = ctx.BoolLiteral().getText.toLowerCase() == "true"
      return ExprPlaceholderPartEqual(b, expr, getTextSource(ctx))
    }
    if (ctx.DEFAULT() != null) {
      return ExprPlaceholderPartDefault(expr, getTextSource(ctx))
    }
    if (ctx.SEP() != null) {
      return ExprPlaceholderPartSep(expr, getTextSource(ctx))
    }
    throw new SyntaxException(s"Not one of three known variants of a placeholder",
                              getTextSource(ctx),
                              docSourceURL)
  }

  // These are full expressions of the same kind
  //
  // ${true="--yes" false="--no" boolean_value}
  // ${default="foo" optional_value}
  // ${sep=", " array_value}
  private def parseEntirePlaceHolderExpression(placeHolders: Vector[PlaceHolderPart],
                                               expr: Expr,
                                               ctx: ParserRuleContext): Expr = {
    if (placeHolders.isEmpty) {
      // This is just an expression inside braces
      // ${1}
      // ${x + 3}
      return expr
    }
    val source = getTextSource(ctx)

    // This is a place-holder such as
    //   ${default="foo" optional_value}
    //   ${sep=", " array_value}
    if (placeHolders.size == 1) {
      placeHolders.head match {
        case ExprPlaceholderPartDefault(default, _) =>
          return ExprPlaceholderDefault(default, expr, source)
        case ExprPlaceholderPartSep(sep, _) =>
          return ExprPlaceholderSep(sep, expr, source)
        case _ =>
          throw new SyntaxException("invalid place holder", getTextSource(ctx), docSourceURL)
      }
    }

    //   ${true="--yes" false="--no" boolean_value}
    if (placeHolders.size == 2) {
      (placeHolders(0), placeHolders(1)) match {
        case (ExprPlaceholderPartEqual(true, x, _), ExprPlaceholderPartEqual(false, y, _)) =>
          return ExprPlaceholderEqual(x, y, expr, source)
        case (ExprPlaceholderPartEqual(false, x, _), ExprPlaceholderPartEqual(true, y, _)) =>
          return ExprPlaceholderEqual(y, x, expr, source)
        case (_: ExprPlaceholderPartEqual, _: ExprPlaceholderPartEqual) =>
          throw new SyntaxException("invalid boolean place holder",
                                    getTextSource(ctx),
                                    docSourceURL)
        case (_, _) =>
          throw new SyntaxException("invalid place holder", getTextSource(ctx), docSourceURL)
      }
    }

    throw new SyntaxException("invalid place holder", getTextSource(ctx), docSourceURL)
  }

  /* string_part
  : StringPart*
  ; */
  override def visitString_part(ctx: WdlDraft2Parser.String_partContext): ExprCompoundString = {
    val parts: Vector[Expr] = ctx
      .StringPart()
      .asScala
      .map(x => ExprString(x.getText, getTextSource(x)))
      .toVector
    ExprCompoundString(parts, getTextSource(ctx))
  }

  /* string_expr_part
  : StringCommandStart (expression_placeholder_option)* expr RBRACE
  ; */
  override def visitString_expr_part(ctx: WdlDraft2Parser.String_expr_partContext): Expr = {
    val pHolder: Vector[PlaceHolderPart] = ctx
      .expression_placeholder_option()
      .asScala
      .map(visitExpression_placeholder_option)
      .toVector
    val expr = visitExpr(ctx.expr())
    parseEntirePlaceHolderExpression(pHolder, expr, ctx)
  }

  /* string_expr_with_string_part
  : string_expr_part string_part
  ; */
  override def visitString_expr_with_string_part(
      ctx: WdlDraft2Parser.String_expr_with_string_partContext
  ): ExprCompoundString = {
    val exprPart = visitString_expr_part(ctx.string_expr_part())
    val strPart = visitString_part(ctx.string_part())
    ExprCompoundString(Vector(exprPart, strPart), getTextSource(ctx))
  }

  /*
string
  : DQUOTE string_part string_expr_with_string_part* DQUOTE
  | SQUOTE string_part string_expr_with_string_part* SQUOTE
  ;
   */
  override def visitString(ctx: WdlDraft2Parser.StringContext): Expr = {
    val stringPart = ExprString(ctx.string_part().getText, getTextSource(ctx.string_part()))
    val exprPart: Vector[ExprCompoundString] = ctx
      .string_expr_with_string_part()
      .asScala
      .map(visitString_expr_with_string_part)
      .toVector
    val exprPart2: Vector[Expr] = exprPart.flatMap(_.value)
    if (exprPart2.isEmpty) {
      // A string  literal
      stringPart
    } else {
      // A string that includes interpolation
      ExprCompoundString(Vector(stringPart) ++ exprPart2, getTextSource(ctx))
    }
  }

  /* primitive_literal
	: BoolLiteral
	| number
	| string
	| Identifier
	; */
  override def visitPrimitive_literal(ctx: WdlDraft2Parser.Primitive_literalContext): Expr = {
    if (ctx.BoolLiteral() != null) {
      val value = ctx.getText.toLowerCase() == "true"
      return ExprBoolean(value, getTextSource(ctx))
    }
    if (ctx.number() != null) {
      return visitNumber(ctx.number())
    }
    if (ctx.string() != null) {
      return visitString(ctx.string())
    }
    if (ctx.Identifier() != null) {
      return ExprIdentifier(ctx.getText, getTextSource(ctx))
    }
    throw new SyntaxException("Not one of four supported variants of primitive_literal",
                              getTextSource(ctx),
                              docSourceURL)
  }

  override def visitLor(ctx: WdlDraft2Parser.LorContext): Expr = {
    val arg0: Expr = visitExpr_infix0(ctx.expr_infix0())
    val arg1: Expr = visitExpr_infix1(ctx.expr_infix1())
    ExprLor(arg0, arg1, getTextSource(ctx))
  }

  override def visitLand(ctx: WdlDraft2Parser.LandContext): Expr = {
    val arg0 = visitExpr_infix1(ctx.expr_infix1())
    val arg1 = visitExpr_infix2(ctx.expr_infix2())
    ExprLand(arg0, arg1, getTextSource(ctx))
  }

  override def visitEqeq(ctx: WdlDraft2Parser.EqeqContext): Expr = {
    val arg0 = visitExpr_infix2(ctx.expr_infix2())
    val arg1 = visitExpr_infix3(ctx.expr_infix3())
    ExprEqeq(arg0, arg1, getTextSource(ctx))
  }
  override def visitLt(ctx: WdlDraft2Parser.LtContext): Expr = {
    val arg0 = visitExpr_infix2(ctx.expr_infix2())
    val arg1 = visitExpr_infix3(ctx.expr_infix3())
    ExprLt(arg0, arg1, getTextSource(ctx))
  }

  override def visitGte(ctx: WdlDraft2Parser.GteContext): Expr = {
    val arg0 = visitExpr_infix2(ctx.expr_infix2())
    val arg1 = visitExpr_infix3(ctx.expr_infix3())
    ExprGte(arg0, arg1, getTextSource(ctx))
  }

  override def visitNeq(ctx: WdlDraft2Parser.NeqContext): Expr = {
    val arg0 = visitExpr_infix2(ctx.expr_infix2())
    val arg1 = visitExpr_infix3(ctx.expr_infix3())
    ExprNeq(arg0, arg1, getTextSource(ctx))
  }

  override def visitLte(ctx: WdlDraft2Parser.LteContext): Expr = {
    val arg0 = visitExpr_infix2(ctx.expr_infix2())
    val arg1 = visitExpr_infix3(ctx.expr_infix3())
    ExprLte(arg0, arg1, getTextSource(ctx))
  }

  override def visitGt(ctx: WdlDraft2Parser.GtContext): Expr = {
    val arg0 = visitExpr_infix2(ctx.expr_infix2())
    val arg1 = visitExpr_infix3(ctx.expr_infix3())
    ExprGt(arg0, arg1, getTextSource(ctx))
  }

  override def visitAdd(ctx: WdlDraft2Parser.AddContext): Expr = {
    val arg0 = visitExpr_infix3(ctx.expr_infix3())
    val arg1 = visitExpr_infix4(ctx.expr_infix4())
    ExprAdd(arg0, arg1, getTextSource(ctx))
  }

  override def visitSub(ctx: WdlDraft2Parser.SubContext): Expr = {
    val arg0 = visitExpr_infix3(ctx.expr_infix3())
    val arg1 = visitExpr_infix4(ctx.expr_infix4())
    ExprSub(arg0, arg1, getTextSource(ctx))
  }

  override def visitMod(ctx: WdlDraft2Parser.ModContext): Expr = {
    val arg0 = visitExpr_infix4(ctx.expr_infix4())
    val arg1 = visitExpr_infix5(ctx.expr_infix5())
    ExprMod(arg0, arg1, getTextSource(ctx))
  }

  override def visitMul(ctx: WdlDraft2Parser.MulContext): Expr = {
    val arg0 = visitExpr_infix4(ctx.expr_infix4())
    val arg1 = visitExpr_infix5(ctx.expr_infix5())
    ExprMul(arg0, arg1, getTextSource(ctx))
  }

  override def visitDivide(ctx: WdlDraft2Parser.DivideContext): Expr = {
    val arg0 = visitExpr_infix4(ctx.expr_infix4())
    val arg1 = visitExpr_infix5(ctx.expr_infix5())
    ExprDivide(arg0, arg1, getTextSource(ctx))
  }

  // | LPAREN expr RPAREN #expression_group
  override def visitExpression_group(ctx: WdlDraft2Parser.Expression_groupContext): Expr = {
    visitExpr(ctx.expr())
  }

  // | LBRACK (expr (COMMA expr)*)* RBRACK #array_literal
  override def visitArray_literal(ctx: WdlDraft2Parser.Array_literalContext): Expr = {
    val elements: Vector[Expr] = ctx
      .expr()
      .asScala
      .map(x => visitExpr(x))
      .toVector
    ExprArrayLiteral(elements, getTextSource(ctx))
  }

  // | LPAREN expr COMMA expr RPAREN #pair_literal
  override def visitPair_literal(ctx: WdlDraft2Parser.Pair_literalContext): Expr = {
    val arg0 = visitExpr(ctx.expr(0))
    val arg1 = visitExpr(ctx.expr(1))
    ExprPair(arg0, arg1, getTextSource(ctx))
  }

  //| LBRACE (expr COLON expr (COMMA expr COLON expr)*)* RBRACE #map_literal
  override def visitMap_literal(ctx: WdlDraft2Parser.Map_literalContext): Expr = {
    val elements = ctx
      .expr()
      .asScala
      .map(x => visitExpr(x))
      .toVector

    val n = elements.size
    if (n % 2 != 0)
      throw new SyntaxException("the expressions in a map must come in pairs",
                                getTextSource(ctx),
                                docSourceURL)

    val m: Vector[ExprMapItem] = Vector.tabulate(n / 2) { i =>
      val key = elements(2 * i)
      val value = elements(2 * i + 1)
      ExprMapItem(key,
                  value,
                  TextSource(key.text.line, key.text.col, value.text.endLine, value.text.endCol))
    }
    ExprMapLiteral(m, getTextSource(ctx))
  }

  // | OBJECT_LITERAL LBRACE (Identifier COLON expr (COMMA Identifier COLON expr)*)* RBRACE #object_literal
  override def visitObject_literal(ctx: WdlDraft2Parser.Object_literalContext): Expr = {
    val ids: Vector[TerminalNode] = ctx
      .Identifier()
      .asScala
      .toVector
    val elements: Vector[Expr] = ctx
      .expr()
      .asScala
      .map(x => visitExpr(x))
      .toVector
    val members = ids.zip(elements).map { pair =>
      val id = pair._1
      val expr = pair._2
      val textSource = TextSource(id.getSymbol.getLine,
                                  id.getSymbol.getCharPositionInLine,
                                  expr.text.endLine,
                                  expr.text.endCol)
      ExprObjectMember(id.getText, expr, textSource)
    }
    ExprObjectLiteral(members, getTextSource(ctx))
  }

  // | NOT expr #negate
  override def visitNegate(ctx: WdlDraft2Parser.NegateContext): Expr = {
    val expr = visitExpr(ctx.expr())
    ExprNegate(expr, getTextSource(ctx))
  }

  // | (PLUS | MINUS) expr #unirarysigned
  override def visitUnirarysigned(ctx: WdlDraft2Parser.UnirarysignedContext): Expr = {
    val expr = visitExpr(ctx.expr())

    if (ctx.PLUS() != null)
      ExprUniraryPlus(expr, getTextSource(ctx))
    else if (ctx.MINUS() != null)
      ExprUniraryMinus(expr, getTextSource(ctx))
    else
      throw new SyntaxException("sanity", getTextSource(ctx), docSourceURL)
  }

  // | expr_core LBRACK expr RBRACK #at
  override def visitAt(ctx: WdlDraft2Parser.AtContext): Expr = {
    val array = visitExpr_core(ctx.expr_core())
    val index = visitExpr(ctx.expr())
    ExprAt(array, index, getTextSource(ctx))
  }

  // | Identifier LPAREN (expr (COMMA expr)*)? RPAREN #apply
  override def visitApply(ctx: WdlDraft2Parser.ApplyContext): Expr = {
    val funcName = ctx.Identifier().getText
    val elements = ctx
      .expr()
      .asScala
      .map(x => visitExpr(x))
      .toVector
    ExprApply(funcName, elements, getTextSource(ctx))
  }

  // | IF expr THEN expr ELSE expr #ifthenelse
  override def visitIfthenelse(ctx: WdlDraft2Parser.IfthenelseContext): Expr = {
    val elements = ctx
      .expr()
      .asScala
      .map(x => visitExpr(x))
      .toVector
    ExprIfThenElse(elements(0), elements(1), elements(2), getTextSource(ctx))
  }

  override def visitLeft_name(ctx: WdlDraft2Parser.Left_nameContext): Expr = {
    val id = getIdentifierText(ctx.Identifier(), ctx)
    ExprIdentifier(id, getTextSource(ctx))
  }

  // | expr_core DOT Identifier #get_name
  override def visitGet_name(ctx: WdlDraft2Parser.Get_nameContext): Expr = {
    val e = visitExpr_core(ctx.expr_core())
    val id = ctx.Identifier.getText
    ExprGetName(e, id, getTextSource(ctx))
  }

  /*expr_infix0
	: expr_infix0 OR expr_infix1 #lor
	| expr_infix1 #infix1
	; */

  private def visitExpr_infix0(ctx: WdlDraft2Parser.Expr_infix0Context): Expr = {
    ctx match {
      case lor: WdlDraft2Parser.LorContext       => visitLor(lor)
      case infix1: WdlDraft2Parser.Infix1Context => visitInfix1(infix1).asInstanceOf[Expr]
    }
  }

  /* expr_infix1
	: expr_infix1 AND expr_infix2 #land
	| expr_infix2 #infix2
	; */
  private def visitExpr_infix1(ctx: WdlDraft2Parser.Expr_infix1Context): Expr = {
    ctx match {
      case land: WdlDraft2Parser.LandContext     => visitLand(land)
      case infix2: WdlDraft2Parser.Infix2Context => visitInfix2(infix2).asInstanceOf[Expr]
    }
  }

  /* expr_infix2
	: expr_infix2 EQUALITY expr_infix3 #eqeq
	| expr_infix2 NOTEQUAL expr_infix3 #neq
	| expr_infix2 LTE expr_infix3 #lte
	| expr_infix2 GTE expr_infix3 #gte
	| expr_infix2 LT expr_infix3 #lt
	| expr_infix2 GT expr_infix3 #gt
	| expr_infix3 #infix3
	; */

  private def visitExpr_infix2(ctx: WdlDraft2Parser.Expr_infix2Context): Expr = {
    ctx match {
      case eqeq: WdlDraft2Parser.EqeqContext     => visitEqeq(eqeq)
      case neq: WdlDraft2Parser.NeqContext       => visitNeq(neq)
      case lte: WdlDraft2Parser.LteContext       => visitLte(lte)
      case gte: WdlDraft2Parser.GteContext       => visitGte(gte)
      case lt: WdlDraft2Parser.LtContext         => visitLt(lt)
      case gt: WdlDraft2Parser.GtContext         => visitGt(gt)
      case infix3: WdlDraft2Parser.Infix3Context => visitInfix3(infix3).asInstanceOf[Expr]
    }
  }

  /* expr_infix3
	: expr_infix3 PLUS expr_infix4 #add
	| expr_infix3 MINUS expr_infix4 #sub
	| expr_infix4 #infix4
	; */
  private def visitExpr_infix3(ctx: WdlDraft2Parser.Expr_infix3Context): Expr = {
    ctx match {
      case add: WdlDraft2Parser.AddContext       => visitAdd(add)
      case sub: WdlDraft2Parser.SubContext       => visitSub(sub)
      case infix4: WdlDraft2Parser.Infix4Context => visitInfix4(infix4).asInstanceOf[Expr]
    }
  }

  /* expr_infix4
	: expr_infix4 STAR expr_infix5 #mul
	| expr_infix4 DIVIDE expr_infix5 #divide
	| expr_infix4 MOD expr_infix5 #mod
	| expr_infix5 #infix5
	;  */
  private def visitExpr_infix4(ctx: WdlDraft2Parser.Expr_infix4Context): Expr = {
    ctx match {
      case mul: WdlDraft2Parser.MulContext       => visitMul(mul)
      case divide: WdlDraft2Parser.DivideContext => visitDivide(divide)
      case mod: WdlDraft2Parser.ModContext       => visitMod(mod)
      case infix5: WdlDraft2Parser.Infix5Context => visitInfix5(infix5).asInstanceOf[Expr]
    }
  }

  /* expr_infix5
	: expr_core
	; */

  override def visitExpr_infix5(ctx: WdlDraft2Parser.Expr_infix5Context): Expr = {
    visitExpr_core(ctx.expr_core())
  }

  /* expr
	: expr_infix
	; */
  override def visitExpr(ctx: WdlDraft2Parser.ExprContext): Expr = {
    try {
      visitChildren(ctx).asInstanceOf[Expr]
    } catch {
      case _: NullPointerException =>
        throw new SyntaxException("bad expression", getTextSource(ctx), docSourceURL)
    }
  }

  /* expr_core
	: LPAREN expr RPAREN #expression_group
	| primitive_literal #primitives
	| LBRACK (expr (COMMA expr)*)* RBRACK #array_literal
	| LPAREN expr COMMA expr RPAREN #pair_literal
	| LBRACE (expr COLON expr (COMMA expr COLON expr)*)* RBRACE #map_literal
	| OBJECT_LITERAL LBRACE (Identifier COLON expr (COMMA Identifier COLON expr)*)* RBRACE #object_literal
	| NOT expr #negate
	| (PLUS | MINUS) expr #unirarysigned
	| expr_core LBRACK expr RBRACK #at
	| IF expr THEN expr ELSE expr #ifthenelse
	| Identifier LPAREN (expr (COMMA expr)*)? RPAREN #apply
	| Identifier #left_name
	| expr_core DOT Identifier #get_name
	; */
  private def visitExpr_core(ctx: WdlDraft2Parser.Expr_coreContext): Expr = {
    ctx match {
      case group: WdlDraft2Parser.Expression_groupContext => visitExpression_group(group)
      case primitives: WdlDraft2Parser.PrimitivesContext =>
        visitPrimitive_literal(primitives.primitive_literal())
      case array_literal: WdlDraft2Parser.Array_literalContext => visitArray_literal(array_literal)
      case pair_literal: WdlDraft2Parser.Pair_literalContext   => visitPair_literal(pair_literal)
      case map_literal: WdlDraft2Parser.Map_literalContext     => visitMap_literal(map_literal)
      case obj_literal: WdlDraft2Parser.Object_literalContext  => visitObject_literal(obj_literal)
      case negate: WdlDraft2Parser.NegateContext               => visitNegate(negate)
      case unirarysigned: WdlDraft2Parser.UnirarysignedContext => visitUnirarysigned(unirarysigned)
      case at: WdlDraft2Parser.AtContext                       => visitAt(at)
      case ifthenelse: WdlDraft2Parser.IfthenelseContext       => visitIfthenelse(ifthenelse)
      case apply: WdlDraft2Parser.ApplyContext                 => visitApply(apply)
      case left_name: WdlDraft2Parser.Left_nameContext         => visitLeft_name(left_name)
      case get_name: WdlDraft2Parser.Get_nameContext           => visitGet_name(get_name)
    }
  }

  /*
unbound_decls
	: wdl_type Identifier
	;
   */
  override def visitUnbound_decls(ctx: WdlDraft2Parser.Unbound_declsContext): Declaration = {
    if (ctx.wdl_type() == null)
      throw new SyntaxException("type missing in declaration", getTextSource(ctx), docSourceURL)
    val wdlType: Type = visitWdl_type(ctx.wdl_type())
    val name: String = getIdentifierText(ctx.Identifier(), ctx)
    Declaration(name, wdlType, None, getTextSource(ctx))
  }

  /*
bound_decls
	: wdl_type Identifier EQUAL expr
	;
   */
  override def visitBound_decls(ctx: WdlDraft2Parser.Bound_declsContext): Declaration = {
    if (ctx.wdl_type() == null)
      throw new SyntaxException("type missing in declaration", getTextSource(ctx), docSourceURL)
    val wdlType: Type = visitWdl_type(ctx.wdl_type())
    val name: String = getIdentifierText(ctx.Identifier(), ctx)
    if (ctx.expr() == null)
      return Declaration(name, wdlType, None, getTextSource(ctx))
    val expr: Expr = visitExpr(ctx.expr())
    Declaration(name, wdlType, Some(expr), getTextSource(ctx))
  }

  /*
any_decls
	: unbound_decls
	| bound_decls
	;
   */
  override def visitAny_decls(ctx: WdlDraft2Parser.Any_declsContext): Declaration = {
    if (ctx.unbound_decls() != null)
      return visitUnbound_decls(ctx.unbound_decls())
    if (ctx.bound_decls() != null)
      return visitBound_decls(ctx.bound_decls())
    throw new SyntaxException("bad declaration format", getTextSource(ctx), docSourceURL)
  }

  /* meta_kv
   : Identifier COLON expr
   ; */
  override def visitMeta_kv(ctx: WdlDraft2Parser.Meta_kvContext): MetaKV = {
    val id = getIdentifierText(ctx.Identifier(), ctx)
    val value = ctx.string().string_part().getText
    MetaKV(id, value, getTextSource(ctx))
  }

  //  PARAMETERMETA LBRACE meta_kv* RBRACE #parameter_meta
  override def visitParameter_meta(
      ctx: WdlDraft2Parser.Parameter_metaContext
  ): ParameterMetaSection = {
    val kvs: Vector[MetaKV] = ctx
      .meta_kv()
      .asScala
      .map(x => visitMeta_kv(x))
      .toVector
    ParameterMetaSection(kvs, getTextSource(ctx))
  }

  //  META LBRACE meta_kv* RBRACE #meta
  override def visitMeta(ctx: WdlDraft2Parser.MetaContext): MetaSection = {
    val kvs: Vector[MetaKV] = ctx
      .meta_kv()
      .asScala
      .map(x => visitMeta_kv(x))
      .toVector
    MetaSection(kvs, getTextSource(ctx))
  }

  /* task_runtime_kv
 : Identifier COLON expr
 ; */
  override def visitTask_runtime_kv(ctx: WdlDraft2Parser.Task_runtime_kvContext): RuntimeKV = {
    val id: String = ctx.Identifier.getText
    val expr: Expr = visitExpr(ctx.expr())
    RuntimeKV(id, expr, getTextSource(ctx))
  }

  /* task_runtime
 : RUNTIME LBRACE (task_runtime_kv)* RBRACE
 ; */
  override def visitTask_runtime(ctx: WdlDraft2Parser.Task_runtimeContext): RuntimeSection = {
    val kvs = ctx
      .task_runtime_kv()
      .asScala
      .map(x => visitTask_runtime_kv(x))
      .toVector
    RuntimeSection(kvs, getTextSource(ctx))
  }

  /* task_output
	: OUTPUT LBRACE (bound_decls)* RBRACE
	; */
  override def visitTask_output(ctx: WdlDraft2Parser.Task_outputContext): OutputSection = {
    val decls = ctx
      .bound_decls()
      .asScala
      .map(x => visitBound_decls(x))
      .toVector
    OutputSection(decls, getTextSource(ctx))
  }

  /* task_command_string_part
    : CommandStringPart*
    ; */
  override def visitTask_command_string_part(
      ctx: WdlDraft2Parser.Task_command_string_partContext
  ): ExprString = {
    val text: String = ctx
      .CommandStringPart()
      .asScala
      .map(x => x.getText)
      .mkString("")
    ExprString(text, getTextSource(ctx))
  }

  /* task_command_expr_part
    : StringCommandStart  (expression_placeholder_option)* expr RBRACE
    ; */
  override def visitTask_command_expr_part(
      ctx: WdlDraft2Parser.Task_command_expr_partContext
  ): Expr = {
    val placeHolders: Vector[PlaceHolderPart] = ctx
      .expression_placeholder_option()
      .asScala
      .map(x => visitExpression_placeholder_option(x))
      .toVector
    val expr = visitExpr(ctx.expr())
    parseEntirePlaceHolderExpression(placeHolders, expr, ctx)
  }

  /* task_command_expr_with_string
    : task_command_expr_part task_command_string_part
    ; */
  override def visitTask_command_expr_with_string(
      ctx: WdlDraft2Parser.Task_command_expr_with_stringContext
  ): ExprCompoundString = {
    val exprPart: Expr = visitTask_command_expr_part(ctx.task_command_expr_part())
    val stringPart: Expr = visitTask_command_string_part(
        ctx.task_command_string_part()
    )
    ExprCompoundString(Vector(exprPart, stringPart), getTextSource(ctx))
  }

  /* task_command
  : COMMAND task_command_string_part task_command_expr_with_string* EndCommand
  | HEREDOC_COMMAND task_command_string_part task_command_expr_with_string* EndCommand
  ; */
  override def visitTask_command(ctx: WdlDraft2Parser.Task_commandContext): CommandSection = {
    val start: Expr = visitTask_command_string_part(ctx.task_command_string_part())
    val parts: Vector[Expr] = ctx
      .task_command_expr_with_string()
      .asScala
      .map(x => visitTask_command_expr_with_string(x))
      .toVector

    val allParts: Vector[Expr] = start +: parts

    // discard empty strings, and flatten compound vectors of strings
    val cleanedParts = allParts.flatMap {
      case ExprString(x, _) if x.isEmpty => Vector.empty
      case ExprCompoundString(v, _)      => v
      case other                         => Vector(other)
    }

    // TODO: do the above until reaching a fixed point

    CommandSection(cleanedParts, getTextSource(ctx))
  }

  // A that should appear zero or once. Make sure this is the case.
  private def atMostOneSection[T](sections: Vector[T],
                                  sectionName: String,
                                  ctx: ParserRuleContext): Option[T] = {
    sections.size match {
      case 0 => None
      case 1 => Some(sections.head)
      case n =>
        throw new SyntaxException(
            s"section ${sectionName} appears ${n} times, it cannot appear more than once",
            getTextSource(ctx),
            docSourceURL
        )
    }
  }

  // A section that must appear exactly once
  private def exactlyOneSection[T](sections: Vector[T],
                                   sectionName: String,
                                   ctx: ParserRuleContext): T = {
    sections.size match {
      case 1 => sections.head
      case n =>
        throw new SyntaxException(
            s"section ${sectionName} appears ${n} times, it must appear exactly once",
            getTextSource(ctx),
            docSourceURL
        )
    }
  }

  // check that the parameter meta section references only has variables declared in
  // the input or output sections.
  private def validateParamMeta(paramMeta: ParameterMetaSection,
                                inputSection: Option[InputSection],
                                outputSection: Option[OutputSection],
                                ctx: ParserRuleContext): Unit = {
    val inputVarNames: Set[String] =
      inputSection
        .map(_.declarations.map(_.name).toSet)
        .getOrElse(Set.empty)
    val outputVarNames: Set[String] =
      outputSection
        .map(_.declarations.map(_.name).toSet)
        .getOrElse(Set.empty)

    // make sure the input and output sections to not intersect
    val both = inputVarNames intersect outputVarNames
    if (both.nonEmpty)
      throw new SyntaxException(s"${both} appears in both input and output sections",
                                getTextSource(ctx),
                                docSourceURL)

    val ioVarNames = inputVarNames ++ outputVarNames

    paramMeta.kvs.foreach {
      case MetaKV(k, _, _) =>
        if (!(ioVarNames contains k))
          throw new SyntaxException(
              s"parameter ${k} does not appear in the input or output sections",
              getTextSource(ctx),
              docSourceURL
          )
    }
  }

  def requiresEvaluation(expr: Expr): Boolean = {
    expr match {
      case _: ExprString | _: ExprFile | _: ExprBoolean | _: ExprInt | _: ExprFloat => false
      case ExprPair(l, r, _)                                                        => requiresEvaluation(l) || requiresEvaluation(r)
      case ExprArrayLiteral(value, _)                                               => value.exists(requiresEvaluation)
      case ExprMapLiteral(value, _) =>
        value.exists(elt => requiresEvaluation(elt.key) || requiresEvaluation(elt.value))
      case ExprObjectLiteral(value, _) => value.exists(member => requiresEvaluation(member.value))
      case _                           => true
    }
  }

  def requiresEvaluation(decl: Declaration): Boolean = {
    if (decl.expr.isDefined) {
      requiresEvaluation(decl.expr.get)
    } else {
      false
    }
  }

  /* task
    : TASK Identifier LBRACE (task_element)+ RBRACE
    ;  */
  /** Draft-2 doesn't have a formal input section. Informally, it is specified that inputs must be
    * "at the top of any scope", which is enforced by the Task grammar. There are three types of
    * declarations: 1) unbound, 2) bound with a literal value (i.e. not requiring evaluation), and
    * 3) bound with an expression requiring evaluation. Only the first two types of declarations may
    * be inputs; however, all three types of declarations can be mixed together. This method creates
    * a synthetic InputSection contianing only declarations of type 1 and 2.
    */
  override def visitTask(ctx: WdlDraft2Parser.TaskContext): Task = {
    val name = getIdentifierText(ctx.Identifier(), ctx)
    val elems = ctx.task_element().asScala.toVector
    val output: Option[OutputSection] = atMostOneSection(elems.collect {
      case x: Task_output_elementContext => visitTask_output(x.task_output())
    }, "output", ctx)
    val command: CommandSection = exactlyOneSection(elems.collect {
      case x: Task_command_elementContext => visitTask_command(x.task_command())
    }, "command", ctx)
    val meta: Option[MetaSection] = atMostOneSection(elems.collect {
      case x: Task_meta_elementContext => visitMeta(x.meta())
    }, "meta", ctx)
    val parameterMeta: Option[ParameterMetaSection] = atMostOneSection(elems.collect {
      case x: Task_parameter_meta_elementContext => visitParameter_meta(x.parameter_meta())
    }, "parameter_meta", ctx)
    val runtime: Option[RuntimeSection] = atMostOneSection(elems.collect {
      case x: Task_runtime_elementContext => visitTask_runtime(x.task_runtime())
    }, "runtime", ctx)

    val textSource = getTextSource(ctx)
    // We treat as an input any unbound declaration as well as any bound declaration
    // that doesn't require evaluation.
    val (decls, inputDecls) = ctx
      .task_input()
      .any_decls()
      .asScala
      .map(x => visitAny_decls(x))
      .toVector
      .partition(requiresEvaluation)
    val input = if (inputDecls.nonEmpty) {
      Some(
          InputSection(inputDecls, TextSource.fromSpan(inputDecls.head.text, inputDecls.last.text))
      )
    } else {
      None
    }

    parameterMeta.foreach(validateParamMeta(_, input, output, ctx))

    Task(
        name,
        input = input,
        output = output,
        command = command,
        declarations = decls,
        meta = meta,
        parameterMeta = parameterMeta,
        runtime = runtime,
        text = textSource
    )
  }

  def visitImport_addr(ctx: WdlDraft2Parser.StringContext): ImportAddr = {
    val addr = ctx.getText.replaceAll("\"", "")
    ImportAddr(addr, getTextSource(ctx))
  }

  /*
  import_as
      : AS Identifier
      ;
   */
  override def visitImport_as(ctx: WdlDraft2Parser.Import_asContext): ImportName = {
    ImportName(ctx.Identifier().getText, getTextSource(ctx))
  }

  /*
   import_doc
    : IMPORT string import_as?
    ;
   */
  override def visitImport_doc(ctx: WdlDraft2Parser.Import_docContext): ImportDoc = {
    val addr = visitImport_addr(ctx.string())
    val name =
      if (ctx.import_as() == null)
        None
      else
        Some(visitImport_as(ctx.import_as()))

    ImportDoc(name, addr, getTextSource(ctx))
  }

  /* call_alias
	: AS Identifier
	; */
  override def visitCall_alias(ctx: WdlDraft2Parser.Call_aliasContext): CallAlias = {
    CallAlias(getIdentifierText(ctx.Identifier(), ctx), getTextSource(ctx))
  }

  /* call_input
	: Identifier EQUAL expr
	; */
  override def visitCall_input(ctx: WdlDraft2Parser.Call_inputContext): CallInput = {
    val expr = visitExpr(ctx.expr())
    CallInput(getIdentifierText(ctx.Identifier(), ctx), expr, getTextSource(ctx))
  }

  /* call_inputs
	: INPUT COLON (call_input (COMMA call_input)*)
	; */
  override def visitCall_inputs(ctx: WdlDraft2Parser.Call_inputsContext): CallInputs = {
    val inputs: Vector[CallInput] = ctx
      .call_input()
      .asScala
      .map { x =>
        visitCall_input(x)
      }
      .toVector
    CallInputs(inputs, getTextSource(ctx))
  }

  /* call_body
	: LBRACE call_inputs? RBRACE
	; */
  override def visitCall_body(ctx: WdlDraft2Parser.Call_bodyContext): CallInputs = {
    if (ctx.call_inputs() == null)
      CallInputs(Vector.empty, getTextSource(ctx))
    else
      visitCall_inputs(ctx.call_inputs())
  }

  /* call
	: CALL Identifier call_alias?  call_body?
	; */
  override def visitCall(ctx: WdlDraft2Parser.CallContext): Call = {
    val name = ctx.call_name().getText

    val alias: Option[CallAlias] =
      if (ctx.call_alias() == null) {
        None
      } else {
        Some(visitCall_alias(ctx.call_alias()))
      }

    val inputs: Option[CallInputs] =
      if (ctx.call_body() == null) {
        None
      } else {
        Some(visitCall_body(ctx.call_body()))
      }

    Call(name, alias, inputs, getTextSource(ctx))
  }

  /*
scatter
	: SCATTER LPAREN Identifier In expr RPAREN LBRACE inner_workflow_element* RBRACE
 ; */
  override def visitScatter(ctx: WdlDraft2Parser.ScatterContext): Scatter = {
    val id = ctx.Identifier.getText
    val expr = visitExpr(ctx.expr())
    val body = ctx
      .inner_workflow_element()
      .asScala
      .map(visitInner_workflow_element)
      .toVector
    Scatter(id, expr, body, getTextSource(ctx))
  }

  /* conditional
	: IF LPAREN expr RPAREN LBRACE inner_workflow_element* RBRACE
	; */
  override def visitConditional(ctx: WdlDraft2Parser.ConditionalContext): Conditional = {
    val expr = visitExpr(ctx.expr())
    val body = ctx
      .inner_workflow_element()
      .asScala
      .map(visitInner_workflow_element)
      .toVector
    Conditional(expr, body, getTextSource(ctx))
  }

  /* workflow_output
	: OUTPUT LBRACE (bound_decls)* RBRACE
	;
   */
  override def visitWorkflow_output(ctx: WdlDraft2Parser.Workflow_outputContext): OutputSection = {
    val decls = ctx
      .bound_decls()
      .asScala
      .map(x => visitBound_decls(x))
      .toVector
    OutputSection(decls, getTextSource(ctx))
  }

  /* inner_workflow_element
	: bound_decls
	| call
	| scatter
	| conditional
	; */
  override def visitInner_workflow_element(
      ctx: WdlDraft2Parser.Inner_workflow_elementContext
  ): WorkflowElement = {
    if (ctx.bound_decls() != null)
      return visitBound_decls(ctx.bound_decls())
    if (ctx.call() != null)
      return visitCall(ctx.call())
    if (ctx.scatter() != null)
      return visitScatter(ctx.scatter())
    if (ctx.conditional() != null)
      return visitConditional(ctx.conditional())
    throw new Exception("sanity")
  }

  /*
  workflow_element
    : workflow_input #input
    | workflow_output #output
    | inner_workflow_element #inner_element
    | parameter_meta #parameter_meta_element
    | meta #meta_element
    ;

  workflow
    : WORKFLOW Identifier LBRACE workflow_element* RBRACE
    ;
   */
  /** Draft-2 doesn't have a formal input section. Informally, it is specified that inputs must be
    * "at the top of any scope" - this is enforced by the Task grammar, but the workflow grammar allows
    * declarations to appear anywhere in the workflow body.
    *
    * There are three types of declarations: 1) unbound, 2) bound with a literal value (i.e. not requiring
    * evaluation), and 3) bound with an expression requiring evaluation. Only the first two types of
    * declarations may be inputs; however, all three types of declarations can be mixed together. This method
    * creates a synthetic InputSection contianing only declarations of type 1 and 2.
    */
  override def visitWorkflow(ctx: WdlDraft2Parser.WorkflowContext): Workflow = {
    val name = getIdentifierText(ctx.Identifier(), ctx)
    val elems: Vector[WdlDraft2Parser.Workflow_elementContext] =
      ctx.workflow_element().asScala.toVector
    val unboundDecls = elems.collect {
      case x: WdlDraft2Parser.Wf_decl_elementContext =>
        visitUnbound_decls(x.unbound_decls())
    }
    val output: Option[OutputSection] = atMostOneSection(elems.collect {
      case x: WdlDraft2Parser.Wf_output_elementContext =>
        visitWorkflow_output(x.workflow_output())
    }, "output", ctx)
    val meta: Option[MetaSection] = atMostOneSection(elems.collect {
      case x: WdlDraft2Parser.Wf_meta_elementContext =>
        visitMeta(x.meta())
    }, "meta", ctx)
    val parameterMeta: Option[ParameterMetaSection] = atMostOneSection(elems.collect {
      case x: WdlDraft2Parser.Wf_parameter_meta_elementContext =>
        visitParameter_meta(x.parameter_meta())
    }, "parameter_meta", ctx)

    val wfElems: Vector[WorkflowElement] = elems.collect {
      case x: WdlDraft2Parser.Wf_inner_elementContext =>
        visitInner_workflow_element(x.inner_workflow_element())
    }

    // We treat as an input any unbound declaration as well as any bound declaration
    // that doesn't require evaluation.
    val (noEvalDecls, wfBody) = wfElems.partition {
      case d: Declaration if !requiresEvaluation(d) => true
      case _                                        => false
    }
    val noEvalDecls2 = noEvalDecls.map(_.asInstanceOf[Declaration])
    val inputDecls = unboundDecls ++ noEvalDecls2
    val input = if (inputDecls.nonEmpty) {
      Some(
          InputSection(inputDecls, TextSource.fromSpan(inputDecls.head.text, inputDecls.last.text))
      )
    } else {
      None
    }

    parameterMeta.foreach(validateParamMeta(_, input, output, ctx))

    Workflow(name, input, output, meta, parameterMeta, wfBody, getTextSource(ctx))
  }

  /*
document_element
	: import_doc
	| struct
	| task
	;
   */
  override def visitDocument_element(
      ctx: WdlDraft2Parser.Document_elementContext
  ): DocumentElement = {
    visitChildren(ctx).asInstanceOf[DocumentElement]
  }

  /*
document
	: version document_element* (workflow document_element*)?
	;
   */
  def visitDocument(ctx: WdlDraft2Parser.DocumentContext,
                    comments: mutable.Map[Int, Comment]): Document = {
    val elems: Vector[DocumentElement] =
      ctx
        .document_element()
        .asScala
        .map(e => visitDocument_element(e))
        .toVector

    val workflow =
      if (ctx.workflow() == null)
        None
      else
        Some(visitWorkflow(ctx.workflow()))

    Document(docSourceURL.get,
             docSource,
             elems,
             workflow,
             getTextSource(ctx),
             CommentMap(comments.toMap))
  }

  def parseDocument: Document = {
    val ctx = grammar.parser.document
    if (ctx == null) {
      throw new Exception("WDL file does not contain a valid document")
    }
    val result = visitDocument(ctx, grammar.comments)
    grammar.verify()
    result
  }

  def parseExpr: Expr = {
    val ctx = grammar.parser.expr_document
    if (ctx == null) {
      throw new Exception("Not a Valid expression")
    }
    val result = visitExpr(ctx.expr())
    grammar.verify()
    result
  }

  def parseWdlType: Type = {
    val ctx = grammar.parser.type_document
    if (ctx == null) {
      throw new Exception("Not a valid WDL type")
    }
    val result = visitWdl_type(ctx.wdl_type())
    grammar.verify()
    result
  }
}
