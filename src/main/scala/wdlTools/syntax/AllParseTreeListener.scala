package wdlTools.syntax
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ErrorNode, TerminalNode}
import org.openwdl.wdl.parser.draft_2.{WdlDraft2Parser, WdlDraft2ParserListener}
import org.openwdl.wdl.parser.v1.{WdlV1Parser, WdlV1ParserListener}

class AllParseTreeListener extends WdlDraft2ParserListener with WdlV1ParserListener {
  def enterInput(ctx: ParserRuleContext): Unit = {}
  def exitInput(ctx: ParserRuleContext): Unit = {}
  def enterImport_alias(ctx: ParserRuleContext): Unit = {}
  def exitImport_alias(ctx: ParserRuleContext): Unit = {}
  def enterVersion(ctx: ParserRuleContext): Unit = {}
  def exitVersion(ctx: ParserRuleContext): Unit = {}
  def enterStruct(ctx: ParserRuleContext): Unit = {}
  def exitStruct(ctx: ParserRuleContext): Unit = {}
  def enterMap_type(ctx: ParserRuleContext): Unit = {}
  def exitMap_type(ctx: ParserRuleContext): Unit = {}
  def enterArray_type(ctx: ParserRuleContext): Unit = {}
  def exitArray_type(ctx: ParserRuleContext): Unit = {}
  def enterPair_type(ctx: ParserRuleContext): Unit = {}
  def exitPair_type(ctx: ParserRuleContext): Unit = {}
  def enterType_base(ctx: ParserRuleContext): Unit = {}
  def exitType_base(ctx: ParserRuleContext): Unit = {}
  def enterWdl_type(ctx: ParserRuleContext): Unit = {}
  def exitWdl_type(ctx: ParserRuleContext): Unit = {}
  def enterUnbound_decls(ctx: ParserRuleContext): Unit = {}
  def exitUnbound_decls(ctx: ParserRuleContext): Unit = {}
  def enterBound_decls(ctx: ParserRuleContext): Unit = {}
  def exitBound_decls(ctx: ParserRuleContext): Unit = {}
  def enterAny_decls(ctx: ParserRuleContext): Unit = {}
  def exitAny_decls(ctx: ParserRuleContext): Unit = {}
  def enterNumber(ctx: ParserRuleContext): Unit = {}
  def exitNumber(ctx: ParserRuleContext): Unit = {}
  def enterExpression_placeholder_option(ctx: ParserRuleContext): Unit = {}
  def exitExpression_placeholder_option(ctx: ParserRuleContext): Unit = {}
  def enterString_part(ctx: ParserRuleContext): Unit = {}
  def exitString_part(ctx: ParserRuleContext): Unit = {}
  def enterString_expr_part(ctx: ParserRuleContext): Unit = {}
  def exitString_expr_part(ctx: ParserRuleContext): Unit = {}
  def enterString_expr_with_string_part(ctx: ParserRuleContext): Unit = {}
  def exitString_expr_with_string_part(ctx: ParserRuleContext): Unit = {}
  def enterString(ctx: ParserRuleContext): Unit = {}
  def exitString(ctx: ParserRuleContext): Unit = {}
  def enterPrimitive_literal(ctx: ParserRuleContext): Unit = {}
  def exitPrimitive_literal(ctx: ParserRuleContext): Unit = {}
  def enterExpr(ctx: ParserRuleContext): Unit = {}
  def exitExpr(ctx: ParserRuleContext): Unit = {}
  def enterInfix0(ctx: ParserRuleContext): Unit = {}
  def exitInfix0(ctx: ParserRuleContext): Unit = {}
  def enterInfix1(ctx: ParserRuleContext): Unit = {}
  def exitInfix1(ctx: ParserRuleContext): Unit = {}
  def enterLor(ctx: ParserRuleContext): Unit = {}
  def exitLor(ctx: ParserRuleContext): Unit = {}
  def enterInfix2(ctx: ParserRuleContext): Unit = {}
  def exitInfix2(ctx: ParserRuleContext): Unit = {}
  def enterLand(ctx: ParserRuleContext): Unit = {}
  def exitLand(ctx: ParserRuleContext): Unit = {}
  def enterEqeq(ctx: ParserRuleContext): Unit = {}
  def exitEqeq(ctx: ParserRuleContext): Unit = {}
  def enterLt(ctx: ParserRuleContext): Unit = {}
  def exitLt(ctx: ParserRuleContext): Unit = {}
  def enterInfix3(ctx: ParserRuleContext): Unit = {}
  def exitInfix3(ctx: ParserRuleContext): Unit = {}
  def enterGte(ctx: ParserRuleContext): Unit = {}
  def exitGte(ctx: ParserRuleContext): Unit = {}
  def enterNeq(ctx: ParserRuleContext): Unit = {}
  def exitNeq(ctx: ParserRuleContext): Unit = {}
  def enterLte(ctx: ParserRuleContext): Unit = {}
  def exitLte(ctx: ParserRuleContext): Unit = {}
  def enterGt(ctx: ParserRuleContext): Unit = {}
  def exitGt(ctx: ParserRuleContext): Unit = {}
  def enterAdd(ctx: ParserRuleContext): Unit = {}
  def exitAdd(ctx: ParserRuleContext): Unit = {}
  def enterSub(ctx: ParserRuleContext): Unit = {}
  def exitSub(ctx: ParserRuleContext): Unit = {}
  def enterInfix4(ctx: ParserRuleContext): Unit = {}
  def exitInfix4(ctx: ParserRuleContext): Unit = {}
  def enterMod(ctx: ParserRuleContext): Unit = {}
  def exitMod(ctx: ParserRuleContext): Unit = {}
  def enterMul(ctx: ParserRuleContext): Unit = {}
  def exitMul(ctx: ParserRuleContext): Unit = {}
  def enterDivide(ctx: ParserRuleContext): Unit = {}
  def exitDivide(ctx: ParserRuleContext): Unit = {}
  def enterInfix5(ctx: ParserRuleContext): Unit = {}
  def exitInfix5(ctx: ParserRuleContext): Unit = {}
  def enterExpr_infix5(ctx: ParserRuleContext): Unit = {}
  def exitExpr_infix5(ctx: ParserRuleContext): Unit = {}
  def enterPair_literal(ctx: ParserRuleContext): Unit = {}
  def exitPair_literal(ctx: ParserRuleContext): Unit = {}
  def enterApply(ctx: ParserRuleContext): Unit = {}
  def exitApply(ctx: ParserRuleContext): Unit = {}
  def enterExpression_group(ctx: ParserRuleContext): Unit = {}
  def exitExpression_group(ctx: ParserRuleContext): Unit = {}
  def enterPrimitives(ctx: ParserRuleContext): Unit = {}
  def exitPrimitives(ctx: ParserRuleContext): Unit = {}
  def enterLeft_name(ctx: ParserRuleContext): Unit = {}
  def exitLeft_name(ctx: ParserRuleContext): Unit = {}
  def enterAt(ctx: ParserRuleContext): Unit = {}
  def exitAt(ctx: ParserRuleContext): Unit = {}
  def enterNegate(ctx: ParserRuleContext): Unit = {}
  def exitNegate(ctx: ParserRuleContext): Unit = {}
  def enterUnirarysigned(ctx: ParserRuleContext): Unit = {}
  def exitUnirarysigned(ctx: ParserRuleContext): Unit = {}
  def enterMap_literal(ctx: ParserRuleContext): Unit = {}
  def exitMap_literal(ctx: ParserRuleContext): Unit = {}
  def enterIfthenelse(ctx: ParserRuleContext): Unit = {}
  def exitIfthenelse(ctx: ParserRuleContext): Unit = {}
  def enterGet_name(ctx: ParserRuleContext): Unit = {}
  def exitGet_name(ctx: ParserRuleContext): Unit = {}
  def enterObject_literal(ctx: ParserRuleContext): Unit = {}
  def exitObject_literal(ctx: ParserRuleContext): Unit = {}
  def enterArray_literal(ctx: ParserRuleContext): Unit = {}
  def exitArray_literal(ctx: ParserRuleContext): Unit = {}
  def enterImport_as(ctx: ParserRuleContext): Unit = {}
  def exitImport_as(ctx: ParserRuleContext): Unit = {}
  def enterImport_doc(ctx: ParserRuleContext): Unit = {}
  def exitImport_doc(ctx: ParserRuleContext): Unit = {}
  def enterMeta_kv(ctx: ParserRuleContext): Unit = {}
  def exitMeta_kv(ctx: ParserRuleContext): Unit = {}
  def enterParameter_meta(ctx: ParserRuleContext): Unit = {}
  def exitParameter_meta(ctx: ParserRuleContext): Unit = {}
  def enterMeta(ctx: ParserRuleContext): Unit = {}
  def exitMeta(ctx: ParserRuleContext): Unit = {}
  def enterTask_runtime_kv(ctx: ParserRuleContext): Unit = {}
  def exitTask_runtime_kv(ctx: ParserRuleContext): Unit = {}
  def enterTask_runtime(ctx: ParserRuleContext): Unit = {}
  def exitTask_runtime(ctx: ParserRuleContext): Unit = {}
  def enterTask_input(ctx: ParserRuleContext): Unit = {}
  def exitTask_input(ctx: ParserRuleContext): Unit = {}
  def enterTask_output(ctx: ParserRuleContext): Unit = {}
  def exitTask_output(ctx: ParserRuleContext): Unit = {}
  def enterTask_command_string_part(ctx: ParserRuleContext): Unit = {}
  def exitTask_command_string_part(ctx: ParserRuleContext): Unit = {}
  def enterTask_command_expr_part(ctx: ParserRuleContext): Unit = {}
  def exitTask_command_expr_part(ctx: ParserRuleContext): Unit = {}
  def enterTask_command_expr_with_string(ctx: ParserRuleContext): Unit = {}
  def exitTask_command_expr_with_string(ctx: ParserRuleContext): Unit = {}
  def enterTask_command(ctx: ParserRuleContext): Unit = {}
  def exitTask_command(ctx: ParserRuleContext): Unit = {}
  def enterTask_element(ctx: ParserRuleContext): Unit = {}
  def exitTask_element(ctx: ParserRuleContext): Unit = {}
  def enterTask(ctx: ParserRuleContext): Unit = {}
  def exitTask(ctx: ParserRuleContext): Unit = {}
  def enterInner_workflow_element(ctx: ParserRuleContext): Unit = {}
  def exitInner_workflow_element(ctx: ParserRuleContext): Unit = {}
  def enterCall_alias(ctx: ParserRuleContext): Unit = {}
  def exitCall_alias(ctx: ParserRuleContext): Unit = {}
  def enterCall_input(ctx: ParserRuleContext): Unit = {}
  def exitCall_input(ctx: ParserRuleContext): Unit = {}
  def enterCall_inputs(ctx: ParserRuleContext): Unit = {}
  def exitCall_inputs(ctx: ParserRuleContext): Unit = {}
  def enterCall_body(ctx: ParserRuleContext): Unit = {}
  def exitCall_body(ctx: ParserRuleContext): Unit = {}
  def enterCall_name(ctx: ParserRuleContext): Unit = {}
  def exitCall_name(ctx: ParserRuleContext): Unit = {}
  def enterCall(ctx: ParserRuleContext): Unit = {}
  def exitCall(ctx: ParserRuleContext): Unit = {}
  def enterScatter(ctx: ParserRuleContext): Unit = {}
  def exitScatter(ctx: ParserRuleContext): Unit = {}
  def enterConditional(ctx: ParserRuleContext): Unit = {}
  def exitConditional(ctx: ParserRuleContext): Unit = {}
  def enterWorkflow_input(ctx: ParserRuleContext): Unit = {}
  def exitWorkflow_input(ctx: ParserRuleContext): Unit = {}
  def enterWorkflow_output(ctx: ParserRuleContext): Unit = {}
  def exitWorkflow_output(ctx: ParserRuleContext): Unit = {}
  def enterOutput(ctx: ParserRuleContext): Unit = {}
  def exitOutput(ctx: ParserRuleContext): Unit = {}
  def enterInner_element(ctx: ParserRuleContext): Unit = {}
  def exitInner_element(ctx: ParserRuleContext): Unit = {}
  def enterParameter_meta_element(ctx: ParserRuleContext): Unit = {}
  def exitParameter_meta_element(ctx: ParserRuleContext): Unit = {}
  def enterMeta_element(ctx: ParserRuleContext): Unit = {}
  def exitMeta_element(ctx: ParserRuleContext): Unit = {}
  def enterWorkflow(ctx: ParserRuleContext): Unit = {}
  def exitWorkflow(ctx: ParserRuleContext): Unit = {}
  def enterDocument_element(ctx: ParserRuleContext): Unit = {}
  def exitDocument_element(ctx: ParserRuleContext): Unit = {}
  def enterTask_output_element(ctx: ParserRuleContext): Unit = {}
  def exitTask_output_element(ctx: ParserRuleContext): Unit = {}
  def enterTask_command_element(ctx: ParserRuleContext): Unit = {}
  def exitTask_command_element(ctx: ParserRuleContext): Unit = {}
  def enterTask_runtime_element(ctx: ParserRuleContext): Unit = {}
  def exitTask_runtime_element(ctx: ParserRuleContext): Unit = {}
  def enterTask_parameter_meta_element(ctx: ParserRuleContext): Unit = {}
  def exitTask_parameter_meta_element(ctx: ParserRuleContext): Unit = {}
  def enterTask_meta_element(ctx: ParserRuleContext): Unit = {}
  def exitTask_meta_element(ctx: ParserRuleContext): Unit = {}
  def enterWf_decl_element(ctx: ParserRuleContext): Unit = {}
  def exitWf_decl_element(ctx: ParserRuleContext): Unit = {}
  def enterWf_output_element(ctx: ParserRuleContext): Unit = {}
  def exitWf_output_element(ctx: ParserRuleContext): Unit = {}
  def enterWf_inner_element(ctx: ParserRuleContext): Unit = {}
  def exitWf_inner_element(ctx: ParserRuleContext): Unit = {}
  def enterWf_parameter_meta_element(ctx: ParserRuleContext): Unit = {}
  def exitWf_parameter_meta_element(ctx: ParserRuleContext): Unit = {}
  def enterWf_meta_element(ctx: ParserRuleContext): Unit = {}
  def exitWf_meta_element(ctx: ParserRuleContext): Unit = {}
  def enterDocument(ctx: ParserRuleContext): Unit = {}
  def exitDocument(ctx: ParserRuleContext): Unit = {}
  def enterType_document(ctx: ParserRuleContext): Unit = {}
  def exitType_document(ctx: ParserRuleContext): Unit = {}
  def enterExpr_document(ctx: ParserRuleContext): Unit = {}
  def exitExpr_document(ctx: ParserRuleContext): Unit = {}
  override def enterMap_type(ctx: WdlDraft2Parser.Map_typeContext): Unit = {
    enterMap_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMap_type(ctx: WdlDraft2Parser.Map_typeContext): Unit = {
    exitMap_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterArray_type(ctx: WdlDraft2Parser.Array_typeContext): Unit = {
    enterArray_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitArray_type(ctx: WdlDraft2Parser.Array_typeContext): Unit = {
    exitArray_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterPair_type(ctx: WdlDraft2Parser.Pair_typeContext): Unit = {
    enterPair_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitPair_type(ctx: WdlDraft2Parser.Pair_typeContext): Unit = {
    exitPair_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterType_base(ctx: WdlDraft2Parser.Type_baseContext): Unit = {
    enterType_base(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitType_base(ctx: WdlDraft2Parser.Type_baseContext): Unit = {
    exitType_base(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWdl_type(ctx: WdlDraft2Parser.Wdl_typeContext): Unit = {
    enterWdl_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWdl_type(ctx: WdlDraft2Parser.Wdl_typeContext): Unit = {
    exitWdl_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterUnbound_decls(ctx: WdlDraft2Parser.Unbound_declsContext): Unit = {
    enterUnbound_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitUnbound_decls(ctx: WdlDraft2Parser.Unbound_declsContext): Unit = {
    exitUnbound_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterBound_decls(ctx: WdlDraft2Parser.Bound_declsContext): Unit = {
    enterBound_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitBound_decls(ctx: WdlDraft2Parser.Bound_declsContext): Unit = {
    exitBound_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterAny_decls(ctx: WdlDraft2Parser.Any_declsContext): Unit = {
    enterAny_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitAny_decls(ctx: WdlDraft2Parser.Any_declsContext): Unit = {
    exitAny_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterNumber(ctx: WdlDraft2Parser.NumberContext): Unit = {
    enterNumber(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitNumber(ctx: WdlDraft2Parser.NumberContext): Unit = {
    exitNumber(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpression_placeholder_option(
      ctx: WdlDraft2Parser.Expression_placeholder_optionContext
  ): Unit = {
    enterExpression_placeholder_option(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpression_placeholder_option(
      ctx: WdlDraft2Parser.Expression_placeholder_optionContext
  ): Unit = {
    exitExpression_placeholder_option(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterString_part(ctx: WdlDraft2Parser.String_partContext): Unit = {
    enterString_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitString_part(ctx: WdlDraft2Parser.String_partContext): Unit = {
    exitString_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterString_expr_part(ctx: WdlDraft2Parser.String_expr_partContext): Unit = {
    enterString_expr_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitString_expr_part(ctx: WdlDraft2Parser.String_expr_partContext): Unit = {
    exitString_expr_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterString_expr_with_string_part(
      ctx: WdlDraft2Parser.String_expr_with_string_partContext
  ): Unit = {
    enterString_expr_with_string_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitString_expr_with_string_part(
      ctx: WdlDraft2Parser.String_expr_with_string_partContext
  ): Unit = {
    exitString_expr_with_string_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterString(ctx: WdlDraft2Parser.StringContext): Unit = {
    enterString(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitString(ctx: WdlDraft2Parser.StringContext): Unit = {
    exitString(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterPrimitive_literal(ctx: WdlDraft2Parser.Primitive_literalContext): Unit = {
    enterPrimitive_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitPrimitive_literal(ctx: WdlDraft2Parser.Primitive_literalContext): Unit = {
    exitPrimitive_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpr(ctx: WdlDraft2Parser.ExprContext): Unit = {
    enterExpr(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpr(ctx: WdlDraft2Parser.ExprContext): Unit = {
    exitExpr(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix0(ctx: WdlDraft2Parser.Infix0Context): Unit = {
    enterInfix0(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix0(ctx: WdlDraft2Parser.Infix0Context): Unit = {
    exitInfix0(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix1(ctx: WdlDraft2Parser.Infix1Context): Unit = {
    enterInfix1(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix1(ctx: WdlDraft2Parser.Infix1Context): Unit = {
    exitInfix1(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLor(ctx: WdlDraft2Parser.LorContext): Unit = {
    enterLor(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLor(ctx: WdlDraft2Parser.LorContext): Unit = {
    exitLor(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix2(ctx: WdlDraft2Parser.Infix2Context): Unit = {
    enterInfix2(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix2(ctx: WdlDraft2Parser.Infix2Context): Unit = {
    exitInfix2(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLand(ctx: WdlDraft2Parser.LandContext): Unit = {
    enterLand(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLand(ctx: WdlDraft2Parser.LandContext): Unit = {
    exitLand(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterEqeq(ctx: WdlDraft2Parser.EqeqContext): Unit = {
    enterEqeq(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitEqeq(ctx: WdlDraft2Parser.EqeqContext): Unit = {
    exitEqeq(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLt(ctx: WdlDraft2Parser.LtContext): Unit = {
    enterLt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLt(ctx: WdlDraft2Parser.LtContext): Unit = {
    exitLt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix3(ctx: WdlDraft2Parser.Infix3Context): Unit = {
    enterInfix3(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix3(ctx: WdlDraft2Parser.Infix3Context): Unit = {
    exitInfix3(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterGte(ctx: WdlDraft2Parser.GteContext): Unit = {
    enterGte(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitGte(ctx: WdlDraft2Parser.GteContext): Unit = {
    exitGte(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterNeq(ctx: WdlDraft2Parser.NeqContext): Unit = {
    enterNeq(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitNeq(ctx: WdlDraft2Parser.NeqContext): Unit = {
    exitNeq(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLte(ctx: WdlDraft2Parser.LteContext): Unit = {
    enterLte(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLte(ctx: WdlDraft2Parser.LteContext): Unit = {
    exitLte(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterGt(ctx: WdlDraft2Parser.GtContext): Unit = {
    enterGt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitGt(ctx: WdlDraft2Parser.GtContext): Unit = {
    exitGt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterAdd(ctx: WdlDraft2Parser.AddContext): Unit = {
    enterAdd(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitAdd(ctx: WdlDraft2Parser.AddContext): Unit = {
    exitAdd(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterSub(ctx: WdlDraft2Parser.SubContext): Unit = {
    enterSub(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitSub(ctx: WdlDraft2Parser.SubContext): Unit = {
    exitSub(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix4(ctx: WdlDraft2Parser.Infix4Context): Unit = {
    enterInfix4(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix4(ctx: WdlDraft2Parser.Infix4Context): Unit = {
    exitInfix4(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMod(ctx: WdlDraft2Parser.ModContext): Unit = {
    enterMod(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMod(ctx: WdlDraft2Parser.ModContext): Unit = {
    exitMod(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMul(ctx: WdlDraft2Parser.MulContext): Unit = {
    enterMul(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMul(ctx: WdlDraft2Parser.MulContext): Unit = {
    exitMul(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterDivide(ctx: WdlDraft2Parser.DivideContext): Unit = {
    enterDivide(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitDivide(ctx: WdlDraft2Parser.DivideContext): Unit = {
    exitDivide(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix5(ctx: WdlDraft2Parser.Infix5Context): Unit = {
    enterInfix5(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix5(ctx: WdlDraft2Parser.Infix5Context): Unit = {
    exitInfix5(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpr_infix5(ctx: WdlDraft2Parser.Expr_infix5Context): Unit = {
    enterExpr_infix5(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpr_infix5(ctx: WdlDraft2Parser.Expr_infix5Context): Unit = {
    exitExpr_infix5(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterPair_literal(ctx: WdlDraft2Parser.Pair_literalContext): Unit = {
    enterPair_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitPair_literal(ctx: WdlDraft2Parser.Pair_literalContext): Unit = {
    exitPair_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterApply(ctx: WdlDraft2Parser.ApplyContext): Unit = {
    enterApply(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitApply(ctx: WdlDraft2Parser.ApplyContext): Unit = {
    exitApply(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpression_group(ctx: WdlDraft2Parser.Expression_groupContext): Unit = {
    enterExpression_group(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpression_group(ctx: WdlDraft2Parser.Expression_groupContext): Unit = {
    exitExpression_group(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterPrimitives(ctx: WdlDraft2Parser.PrimitivesContext): Unit = {
    enterPrimitives(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitPrimitives(ctx: WdlDraft2Parser.PrimitivesContext): Unit = {
    exitPrimitives(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLeft_name(ctx: WdlDraft2Parser.Left_nameContext): Unit = {
    enterLeft_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLeft_name(ctx: WdlDraft2Parser.Left_nameContext): Unit = {
    exitLeft_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterAt(ctx: WdlDraft2Parser.AtContext): Unit = {
    enterAt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitAt(ctx: WdlDraft2Parser.AtContext): Unit = {
    exitAt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterNegate(ctx: WdlDraft2Parser.NegateContext): Unit = {
    enterNegate(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitNegate(ctx: WdlDraft2Parser.NegateContext): Unit = {
    exitNegate(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterUnirarysigned(ctx: WdlDraft2Parser.UnirarysignedContext): Unit = {
    enterUnirarysigned(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitUnirarysigned(ctx: WdlDraft2Parser.UnirarysignedContext): Unit = {
    exitUnirarysigned(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMap_literal(ctx: WdlDraft2Parser.Map_literalContext): Unit = {
    enterMap_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMap_literal(ctx: WdlDraft2Parser.Map_literalContext): Unit = {
    exitMap_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterIfthenelse(ctx: WdlDraft2Parser.IfthenelseContext): Unit = {
    enterIfthenelse(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitIfthenelse(ctx: WdlDraft2Parser.IfthenelseContext): Unit = {
    exitIfthenelse(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterGet_name(ctx: WdlDraft2Parser.Get_nameContext): Unit = {
    enterGet_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitGet_name(ctx: WdlDraft2Parser.Get_nameContext): Unit = {
    exitGet_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterObject_literal(ctx: WdlDraft2Parser.Object_literalContext): Unit = {
    enterObject_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitObject_literal(ctx: WdlDraft2Parser.Object_literalContext): Unit = {
    exitObject_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterArray_literal(ctx: WdlDraft2Parser.Array_literalContext): Unit = {
    enterArray_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitArray_literal(ctx: WdlDraft2Parser.Array_literalContext): Unit = {
    exitArray_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterImport_as(ctx: WdlDraft2Parser.Import_asContext): Unit = {
    enterImport_as(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitImport_as(ctx: WdlDraft2Parser.Import_asContext): Unit = {
    exitImport_as(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterImport_doc(ctx: WdlDraft2Parser.Import_docContext): Unit = {
    enterImport_doc(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitImport_doc(ctx: WdlDraft2Parser.Import_docContext): Unit = {
    exitImport_doc(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMeta_kv(ctx: WdlDraft2Parser.Meta_kvContext): Unit = {
    enterMeta_kv(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMeta_kv(ctx: WdlDraft2Parser.Meta_kvContext): Unit = {
    exitMeta_kv(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterParameter_meta(ctx: WdlDraft2Parser.Parameter_metaContext): Unit = {
    enterParameter_meta(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitParameter_meta(ctx: WdlDraft2Parser.Parameter_metaContext): Unit = {
    exitParameter_meta(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMeta(ctx: WdlDraft2Parser.MetaContext): Unit = {
    enterMeta(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMeta(ctx: WdlDraft2Parser.MetaContext): Unit = {
    exitMeta(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_runtime_kv(ctx: WdlDraft2Parser.Task_runtime_kvContext): Unit = {
    enterTask_runtime_kv(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_runtime_kv(ctx: WdlDraft2Parser.Task_runtime_kvContext): Unit = {
    exitTask_runtime_kv(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_runtime(ctx: WdlDraft2Parser.Task_runtimeContext): Unit = {
    enterTask_runtime(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_runtime(ctx: WdlDraft2Parser.Task_runtimeContext): Unit = {
    exitTask_runtime(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_input(ctx: WdlDraft2Parser.Task_inputContext): Unit = {
    enterTask_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_input(ctx: WdlDraft2Parser.Task_inputContext): Unit = {
    exitTask_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_output(ctx: WdlDraft2Parser.Task_outputContext): Unit = {
    enterTask_output(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_output(ctx: WdlDraft2Parser.Task_outputContext): Unit = {
    exitTask_output(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_command_string_part(
      ctx: WdlDraft2Parser.Task_command_string_partContext
  ): Unit = {
    enterTask_command_string_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_command_string_part(
      ctx: WdlDraft2Parser.Task_command_string_partContext
  ): Unit = {
    exitTask_command_string_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_command_expr_part(
      ctx: WdlDraft2Parser.Task_command_expr_partContext
  ): Unit = {
    enterTask_command_expr_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_command_expr_part(
      ctx: WdlDraft2Parser.Task_command_expr_partContext
  ): Unit = {
    exitTask_command_expr_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_command_expr_with_string(
      ctx: WdlDraft2Parser.Task_command_expr_with_stringContext
  ): Unit = {
    enterTask_command_expr_with_string(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_command_expr_with_string(
      ctx: WdlDraft2Parser.Task_command_expr_with_stringContext
  ): Unit = {
    exitTask_command_expr_with_string(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_command(ctx: WdlDraft2Parser.Task_commandContext): Unit = {
    enterTask_command(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_command(ctx: WdlDraft2Parser.Task_commandContext): Unit = {
    exitTask_command(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask(ctx: WdlDraft2Parser.TaskContext): Unit = {
    enterTask(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask(ctx: WdlDraft2Parser.TaskContext): Unit = {
    exitTask(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInner_workflow_element(
      ctx: WdlDraft2Parser.Inner_workflow_elementContext
  ): Unit = {
    enterInner_workflow_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInner_workflow_element(
      ctx: WdlDraft2Parser.Inner_workflow_elementContext
  ): Unit = {
    exitInner_workflow_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_alias(ctx: WdlDraft2Parser.Call_aliasContext): Unit = {
    enterCall_alias(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_alias(ctx: WdlDraft2Parser.Call_aliasContext): Unit = {
    exitCall_alias(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_input(ctx: WdlDraft2Parser.Call_inputContext): Unit = {
    enterCall_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_input(ctx: WdlDraft2Parser.Call_inputContext): Unit = {
    exitCall_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_inputs(ctx: WdlDraft2Parser.Call_inputsContext): Unit = {
    enterCall_inputs(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_inputs(ctx: WdlDraft2Parser.Call_inputsContext): Unit = {
    exitCall_inputs(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_body(ctx: WdlDraft2Parser.Call_bodyContext): Unit = {
    enterCall_body(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_body(ctx: WdlDraft2Parser.Call_bodyContext): Unit = {
    exitCall_body(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_name(ctx: WdlDraft2Parser.Call_nameContext): Unit = {
    enterCall_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_name(ctx: WdlDraft2Parser.Call_nameContext): Unit = {
    exitCall_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall(ctx: WdlDraft2Parser.CallContext): Unit = {
    enterCall(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall(ctx: WdlDraft2Parser.CallContext): Unit = {
    exitCall(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterScatter(ctx: WdlDraft2Parser.ScatterContext): Unit = {
    enterScatter(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitScatter(ctx: WdlDraft2Parser.ScatterContext): Unit = {
    exitScatter(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterConditional(ctx: WdlDraft2Parser.ConditionalContext): Unit = {
    enterConditional(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitConditional(ctx: WdlDraft2Parser.ConditionalContext): Unit = {
    exitConditional(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWorkflow_output(ctx: WdlDraft2Parser.Workflow_outputContext): Unit = {
    enterWorkflow_output(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWorkflow_output(ctx: WdlDraft2Parser.Workflow_outputContext): Unit = {
    exitWorkflow_output(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWorkflow(ctx: WdlDraft2Parser.WorkflowContext): Unit = {
    enterWorkflow(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWorkflow(ctx: WdlDraft2Parser.WorkflowContext): Unit = {
    exitWorkflow(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterDocument_element(ctx: WdlDraft2Parser.Document_elementContext): Unit = {
    enterDocument_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitDocument_element(ctx: WdlDraft2Parser.Document_elementContext): Unit = {
    exitDocument_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterDocument(ctx: WdlDraft2Parser.DocumentContext): Unit = {
    enterDocument(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitDocument(ctx: WdlDraft2Parser.DocumentContext): Unit = {
    exitDocument(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterType_document(ctx: WdlDraft2Parser.Type_documentContext): Unit = {
    enterType_document(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitType_document(ctx: WdlDraft2Parser.Type_documentContext): Unit = {
    exitType_document(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpr_document(ctx: WdlDraft2Parser.Expr_documentContext): Unit = {
    enterExpr_document(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpr_document(ctx: WdlDraft2Parser.Expr_documentContext): Unit = {
    exitExpr_document(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMap_type(ctx: WdlV1Parser.Map_typeContext): Unit = {
    enterMap_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMap_type(ctx: WdlV1Parser.Map_typeContext): Unit = {
    exitMap_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterArray_type(ctx: WdlV1Parser.Array_typeContext): Unit = {
    enterArray_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitArray_type(ctx: WdlV1Parser.Array_typeContext): Unit = {
    exitArray_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterPair_type(ctx: WdlV1Parser.Pair_typeContext): Unit = {
    enterPair_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitPair_type(ctx: WdlV1Parser.Pair_typeContext): Unit = {
    exitPair_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterType_base(ctx: WdlV1Parser.Type_baseContext): Unit = {
    enterType_base(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitType_base(ctx: WdlV1Parser.Type_baseContext): Unit = {
    exitType_base(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWdl_type(ctx: WdlV1Parser.Wdl_typeContext): Unit = {
    enterWdl_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWdl_type(ctx: WdlV1Parser.Wdl_typeContext): Unit = {
    exitWdl_type(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterUnbound_decls(ctx: WdlV1Parser.Unbound_declsContext): Unit = {
    enterUnbound_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitUnbound_decls(ctx: WdlV1Parser.Unbound_declsContext): Unit = {
    exitUnbound_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterBound_decls(ctx: WdlV1Parser.Bound_declsContext): Unit = {
    enterBound_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitBound_decls(ctx: WdlV1Parser.Bound_declsContext): Unit = {
    exitBound_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterAny_decls(ctx: WdlV1Parser.Any_declsContext): Unit = {
    enterAny_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitAny_decls(ctx: WdlV1Parser.Any_declsContext): Unit = {
    exitAny_decls(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterNumber(ctx: WdlV1Parser.NumberContext): Unit = {
    enterNumber(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitNumber(ctx: WdlV1Parser.NumberContext): Unit = {
    exitNumber(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpression_placeholder_option(
      ctx: WdlV1Parser.Expression_placeholder_optionContext
  ): Unit = {
    enterExpression_placeholder_option(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpression_placeholder_option(
      ctx: WdlV1Parser.Expression_placeholder_optionContext
  ): Unit = {
    exitExpression_placeholder_option(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterString_part(ctx: WdlV1Parser.String_partContext): Unit = {
    enterString_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitString_part(ctx: WdlV1Parser.String_partContext): Unit = {
    exitString_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterString_expr_part(ctx: WdlV1Parser.String_expr_partContext): Unit = {
    enterString_expr_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitString_expr_part(ctx: WdlV1Parser.String_expr_partContext): Unit = {
    exitString_expr_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterString_expr_with_string_part(
      ctx: WdlV1Parser.String_expr_with_string_partContext
  ): Unit = {
    enterString_expr_with_string_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitString_expr_with_string_part(
      ctx: WdlV1Parser.String_expr_with_string_partContext
  ): Unit = {
    exitString_expr_with_string_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterString(ctx: WdlV1Parser.StringContext): Unit = {
    enterString(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitString(ctx: WdlV1Parser.StringContext): Unit = {
    exitString(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterPrimitive_literal(ctx: WdlV1Parser.Primitive_literalContext): Unit = {
    enterPrimitive_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitPrimitive_literal(ctx: WdlV1Parser.Primitive_literalContext): Unit = {
    exitPrimitive_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpr(ctx: WdlV1Parser.ExprContext): Unit = {
    enterExpr(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpr(ctx: WdlV1Parser.ExprContext): Unit = {
    exitExpr(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix0(ctx: WdlV1Parser.Infix0Context): Unit = {
    enterInfix0(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix0(ctx: WdlV1Parser.Infix0Context): Unit = {
    exitInfix0(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix1(ctx: WdlV1Parser.Infix1Context): Unit = {
    enterInfix1(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix1(ctx: WdlV1Parser.Infix1Context): Unit = {
    exitInfix1(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLor(ctx: WdlV1Parser.LorContext): Unit = {
    enterLor(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLor(ctx: WdlV1Parser.LorContext): Unit = {
    exitLor(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix2(ctx: WdlV1Parser.Infix2Context): Unit = {
    enterInfix2(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix2(ctx: WdlV1Parser.Infix2Context): Unit = {
    exitInfix2(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLand(ctx: WdlV1Parser.LandContext): Unit = {
    enterLand(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLand(ctx: WdlV1Parser.LandContext): Unit = {
    exitLand(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterEqeq(ctx: WdlV1Parser.EqeqContext): Unit = {
    enterEqeq(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitEqeq(ctx: WdlV1Parser.EqeqContext): Unit = {
    exitEqeq(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLt(ctx: WdlV1Parser.LtContext): Unit = {
    enterLt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLt(ctx: WdlV1Parser.LtContext): Unit = {
    exitLt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix3(ctx: WdlV1Parser.Infix3Context): Unit = {
    enterInfix3(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix3(ctx: WdlV1Parser.Infix3Context): Unit = {
    exitInfix3(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterGte(ctx: WdlV1Parser.GteContext): Unit = {
    enterGte(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitGte(ctx: WdlV1Parser.GteContext): Unit = {
    exitGte(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterNeq(ctx: WdlV1Parser.NeqContext): Unit = {
    enterNeq(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitNeq(ctx: WdlV1Parser.NeqContext): Unit = {
    exitNeq(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLte(ctx: WdlV1Parser.LteContext): Unit = {
    enterLte(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLte(ctx: WdlV1Parser.LteContext): Unit = {
    exitLte(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterGt(ctx: WdlV1Parser.GtContext): Unit = {
    enterGt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitGt(ctx: WdlV1Parser.GtContext): Unit = {
    exitGt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterAdd(ctx: WdlV1Parser.AddContext): Unit = {
    enterAdd(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitAdd(ctx: WdlV1Parser.AddContext): Unit = {
    exitAdd(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterSub(ctx: WdlV1Parser.SubContext): Unit = {
    enterSub(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitSub(ctx: WdlV1Parser.SubContext): Unit = {
    exitSub(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix4(ctx: WdlV1Parser.Infix4Context): Unit = {
    enterInfix4(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix4(ctx: WdlV1Parser.Infix4Context): Unit = {
    exitInfix4(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMod(ctx: WdlV1Parser.ModContext): Unit = {
    enterMod(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMod(ctx: WdlV1Parser.ModContext): Unit = {
    exitMod(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMul(ctx: WdlV1Parser.MulContext): Unit = {
    enterMul(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMul(ctx: WdlV1Parser.MulContext): Unit = {
    exitMul(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterDivide(ctx: WdlV1Parser.DivideContext): Unit = {
    enterDivide(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitDivide(ctx: WdlV1Parser.DivideContext): Unit = {
    exitDivide(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInfix5(ctx: WdlV1Parser.Infix5Context): Unit = {
    enterInfix5(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInfix5(ctx: WdlV1Parser.Infix5Context): Unit = {
    exitInfix5(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpr_infix5(ctx: WdlV1Parser.Expr_infix5Context): Unit = {
    enterExpr_infix5(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpr_infix5(ctx: WdlV1Parser.Expr_infix5Context): Unit = {
    exitExpr_infix5(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterPair_literal(ctx: WdlV1Parser.Pair_literalContext): Unit = {
    enterPair_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitPair_literal(ctx: WdlV1Parser.Pair_literalContext): Unit = {
    exitPair_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterApply(ctx: WdlV1Parser.ApplyContext): Unit = {
    enterApply(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitApply(ctx: WdlV1Parser.ApplyContext): Unit = {
    exitApply(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpression_group(ctx: WdlV1Parser.Expression_groupContext): Unit = {
    enterExpression_group(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpression_group(ctx: WdlV1Parser.Expression_groupContext): Unit = {
    exitExpression_group(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterPrimitives(ctx: WdlV1Parser.PrimitivesContext): Unit = {
    enterPrimitives(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitPrimitives(ctx: WdlV1Parser.PrimitivesContext): Unit = {
    exitPrimitives(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterLeft_name(ctx: WdlV1Parser.Left_nameContext): Unit = {
    enterLeft_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitLeft_name(ctx: WdlV1Parser.Left_nameContext): Unit = {
    exitLeft_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterAt(ctx: WdlV1Parser.AtContext): Unit = {
    enterAt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitAt(ctx: WdlV1Parser.AtContext): Unit = {
    exitAt(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterNegate(ctx: WdlV1Parser.NegateContext): Unit = {
    enterNegate(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitNegate(ctx: WdlV1Parser.NegateContext): Unit = {
    exitNegate(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterUnirarysigned(ctx: WdlV1Parser.UnirarysignedContext): Unit = {
    enterUnirarysigned(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitUnirarysigned(ctx: WdlV1Parser.UnirarysignedContext): Unit = {
    exitUnirarysigned(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMap_literal(ctx: WdlV1Parser.Map_literalContext): Unit = {
    enterMap_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMap_literal(ctx: WdlV1Parser.Map_literalContext): Unit = {
    exitMap_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterIfthenelse(ctx: WdlV1Parser.IfthenelseContext): Unit = {
    enterIfthenelse(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitIfthenelse(ctx: WdlV1Parser.IfthenelseContext): Unit = {
    exitIfthenelse(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterGet_name(ctx: WdlV1Parser.Get_nameContext): Unit = {
    enterGet_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitGet_name(ctx: WdlV1Parser.Get_nameContext): Unit = {
    exitGet_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterObject_literal(ctx: WdlV1Parser.Object_literalContext): Unit = {
    enterObject_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitObject_literal(ctx: WdlV1Parser.Object_literalContext): Unit = {
    exitObject_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterArray_literal(ctx: WdlV1Parser.Array_literalContext): Unit = {
    enterArray_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitArray_literal(ctx: WdlV1Parser.Array_literalContext): Unit = {
    exitArray_literal(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterVersion(ctx: WdlV1Parser.VersionContext): Unit = {
    enterVersion(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitVersion(ctx: WdlV1Parser.VersionContext): Unit = {
    exitVersion(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterImport_alias(ctx: WdlV1Parser.Import_aliasContext): Unit = {
    enterImport_alias(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitImport_alias(ctx: WdlV1Parser.Import_aliasContext): Unit = {
    exitImport_alias(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterImport_as(ctx: WdlV1Parser.Import_asContext): Unit = {
    enterImport_as(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitImport_as(ctx: WdlV1Parser.Import_asContext): Unit = {
    exitImport_as(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterImport_doc(ctx: WdlV1Parser.Import_docContext): Unit = {
    enterImport_doc(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitImport_doc(ctx: WdlV1Parser.Import_docContext): Unit = {
    exitImport_doc(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterStruct(ctx: WdlV1Parser.StructContext): Unit = {
    enterStruct(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitStruct(ctx: WdlV1Parser.StructContext): Unit = {
    exitStruct(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMeta_kv(ctx: WdlV1Parser.Meta_kvContext): Unit = {
    enterMeta_kv(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMeta_kv(ctx: WdlV1Parser.Meta_kvContext): Unit = {
    exitMeta_kv(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterParameter_meta(ctx: WdlV1Parser.Parameter_metaContext): Unit = {
    enterParameter_meta(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitParameter_meta(ctx: WdlV1Parser.Parameter_metaContext): Unit = {
    exitParameter_meta(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMeta(ctx: WdlV1Parser.MetaContext): Unit = {
    enterMeta(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMeta(ctx: WdlV1Parser.MetaContext): Unit = {
    exitMeta(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_runtime_kv(ctx: WdlV1Parser.Task_runtime_kvContext): Unit = {
    enterTask_runtime_kv(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_runtime_kv(ctx: WdlV1Parser.Task_runtime_kvContext): Unit = {
    exitTask_runtime_kv(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_runtime(ctx: WdlV1Parser.Task_runtimeContext): Unit = {
    enterTask_runtime(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_runtime(ctx: WdlV1Parser.Task_runtimeContext): Unit = {
    exitTask_runtime(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_input(ctx: WdlV1Parser.Task_inputContext): Unit = {
    enterTask_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_input(ctx: WdlV1Parser.Task_inputContext): Unit = {
    exitTask_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_output(ctx: WdlV1Parser.Task_outputContext): Unit = {
    enterTask_output(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_output(ctx: WdlV1Parser.Task_outputContext): Unit = {
    exitTask_output(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_command_string_part(
      ctx: WdlV1Parser.Task_command_string_partContext
  ): Unit = {
    enterTask_command_string_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_command_string_part(
      ctx: WdlV1Parser.Task_command_string_partContext
  ): Unit = {
    exitTask_command_string_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_command_expr_part(ctx: WdlV1Parser.Task_command_expr_partContext): Unit = {
    enterTask_command_expr_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_command_expr_part(ctx: WdlV1Parser.Task_command_expr_partContext): Unit = {
    exitTask_command_expr_part(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_command_expr_with_string(
      ctx: WdlV1Parser.Task_command_expr_with_stringContext
  ): Unit = {
    enterTask_command_expr_with_string(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_command_expr_with_string(
      ctx: WdlV1Parser.Task_command_expr_with_stringContext
  ): Unit = {
    exitTask_command_expr_with_string(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_command(ctx: WdlV1Parser.Task_commandContext): Unit = {
    enterTask_command(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_command(ctx: WdlV1Parser.Task_commandContext): Unit = {
    exitTask_command(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_element(ctx: WdlV1Parser.Task_elementContext): Unit = {
    enterTask_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_element(ctx: WdlV1Parser.Task_elementContext): Unit = {
    exitTask_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask(ctx: WdlV1Parser.TaskContext): Unit = {
    enterTask(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask(ctx: WdlV1Parser.TaskContext): Unit = {
    exitTask(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInner_workflow_element(ctx: WdlV1Parser.Inner_workflow_elementContext): Unit = {
    enterInner_workflow_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInner_workflow_element(ctx: WdlV1Parser.Inner_workflow_elementContext): Unit = {
    exitInner_workflow_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_alias(ctx: WdlV1Parser.Call_aliasContext): Unit = {
    enterCall_alias(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_alias(ctx: WdlV1Parser.Call_aliasContext): Unit = {
    exitCall_alias(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_input(ctx: WdlV1Parser.Call_inputContext): Unit = {
    enterCall_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_input(ctx: WdlV1Parser.Call_inputContext): Unit = {
    exitCall_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_inputs(ctx: WdlV1Parser.Call_inputsContext): Unit = {
    enterCall_inputs(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_inputs(ctx: WdlV1Parser.Call_inputsContext): Unit = {
    exitCall_inputs(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_body(ctx: WdlV1Parser.Call_bodyContext): Unit = {
    enterCall_body(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_body(ctx: WdlV1Parser.Call_bodyContext): Unit = {
    exitCall_body(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall_name(ctx: WdlV1Parser.Call_nameContext): Unit = {
    enterCall_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall_name(ctx: WdlV1Parser.Call_nameContext): Unit = {
    exitCall_name(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterCall(ctx: WdlV1Parser.CallContext): Unit = {
    enterCall(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitCall(ctx: WdlV1Parser.CallContext): Unit = {
    exitCall(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterScatter(ctx: WdlV1Parser.ScatterContext): Unit = {
    enterScatter(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitScatter(ctx: WdlV1Parser.ScatterContext): Unit = {
    exitScatter(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterConditional(ctx: WdlV1Parser.ConditionalContext): Unit = {
    enterConditional(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitConditional(ctx: WdlV1Parser.ConditionalContext): Unit = {
    exitConditional(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWorkflow_input(ctx: WdlV1Parser.Workflow_inputContext): Unit = {
    enterWorkflow_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWorkflow_input(ctx: WdlV1Parser.Workflow_inputContext): Unit = {
    exitWorkflow_input(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWorkflow_output(ctx: WdlV1Parser.Workflow_outputContext): Unit = {
    enterWorkflow_output(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWorkflow_output(ctx: WdlV1Parser.Workflow_outputContext): Unit = {
    exitWorkflow_output(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInput(ctx: WdlV1Parser.InputContext): Unit = {
    enterInput(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInput(ctx: WdlV1Parser.InputContext): Unit = {
    exitInput(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterOutput(ctx: WdlV1Parser.OutputContext): Unit = {
    enterOutput(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitOutput(ctx: WdlV1Parser.OutputContext): Unit = {
    exitOutput(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterInner_element(ctx: WdlV1Parser.Inner_elementContext): Unit = {
    enterInner_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitInner_element(ctx: WdlV1Parser.Inner_elementContext): Unit = {
    exitInner_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterParameter_meta_element(ctx: WdlV1Parser.Parameter_meta_elementContext): Unit = {
    enterParameter_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitParameter_meta_element(ctx: WdlV1Parser.Parameter_meta_elementContext): Unit = {
    exitParameter_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterMeta_element(ctx: WdlV1Parser.Meta_elementContext): Unit = {
    enterMeta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitMeta_element(ctx: WdlV1Parser.Meta_elementContext): Unit = {
    exitMeta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWorkflow(ctx: WdlV1Parser.WorkflowContext): Unit = {
    enterWorkflow(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWorkflow(ctx: WdlV1Parser.WorkflowContext): Unit = {
    exitWorkflow(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterDocument_element(ctx: WdlV1Parser.Document_elementContext): Unit = {
    enterDocument_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitDocument_element(ctx: WdlV1Parser.Document_elementContext): Unit = {
    exitDocument_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterDocument(ctx: WdlV1Parser.DocumentContext): Unit = {
    enterDocument(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitDocument(ctx: WdlV1Parser.DocumentContext): Unit = {
    exitDocument(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterType_document(ctx: WdlV1Parser.Type_documentContext): Unit = {
    enterType_document(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitType_document(ctx: WdlV1Parser.Type_documentContext): Unit = {
    exitType_document(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterExpr_document(ctx: WdlV1Parser.Expr_documentContext): Unit = {
    enterExpr_document(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitExpr_document(ctx: WdlV1Parser.Expr_documentContext): Unit = {
    exitExpr_document(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_output_element(ctx: WdlDraft2Parser.Task_output_elementContext): Unit = {
    enterTask_output_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_output_element(ctx: WdlDraft2Parser.Task_output_elementContext): Unit = {
    exitTask_output_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_command_element(ctx: WdlDraft2Parser.Task_command_elementContext): Unit = {
    enterTask_command_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_command_element(ctx: WdlDraft2Parser.Task_command_elementContext): Unit = {
    exitTask_command_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_runtime_element(ctx: WdlDraft2Parser.Task_runtime_elementContext): Unit = {
    enterTask_runtime_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_runtime_element(ctx: WdlDraft2Parser.Task_runtime_elementContext): Unit = {
    exitTask_runtime_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_parameter_meta_element(
      ctx: WdlDraft2Parser.Task_parameter_meta_elementContext
  ): Unit = {
    enterTask_parameter_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_parameter_meta_element(
      ctx: WdlDraft2Parser.Task_parameter_meta_elementContext
  ): Unit = {
    exitTask_parameter_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterTask_meta_element(ctx: WdlDraft2Parser.Task_meta_elementContext): Unit = {
    enterTask_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitTask_meta_element(ctx: WdlDraft2Parser.Task_meta_elementContext): Unit = {
    exitTask_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWf_decl_element(ctx: WdlDraft2Parser.Wf_decl_elementContext): Unit = {
    enterWf_decl_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWf_decl_element(ctx: WdlDraft2Parser.Wf_decl_elementContext): Unit = {
    exitWf_decl_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWf_output_element(ctx: WdlDraft2Parser.Wf_output_elementContext): Unit = {
    enterWf_output_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWf_output_element(ctx: WdlDraft2Parser.Wf_output_elementContext): Unit = {
    exitWf_output_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWf_inner_element(ctx: WdlDraft2Parser.Wf_inner_elementContext): Unit = {
    enterWf_inner_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWf_inner_element(ctx: WdlDraft2Parser.Wf_inner_elementContext): Unit = {
    exitWf_inner_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWf_parameter_meta_element(
      ctx: WdlDraft2Parser.Wf_parameter_meta_elementContext
  ): Unit = {
    enterWf_parameter_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWf_parameter_meta_element(
      ctx: WdlDraft2Parser.Wf_parameter_meta_elementContext
  ): Unit = {
    exitWf_parameter_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def enterWf_meta_element(ctx: WdlDraft2Parser.Wf_meta_elementContext): Unit = {
    enterWf_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def exitWf_meta_element(ctx: WdlDraft2Parser.Wf_meta_elementContext): Unit = {
    exitWf_meta_element(ctx.asInstanceOf[ParserRuleContext])
  }
  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}
  override def exitEveryRule(ctx: ParserRuleContext): Unit = {}
}
