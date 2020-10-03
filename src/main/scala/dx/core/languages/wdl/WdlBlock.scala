/*
Split a graph into sub-blocks, each of which contains a bunch of toplevel
expressions and one:
   1) call to workflow or task
or 2) conditional with one or more calls inside it
or 3) scatter with one or more calls inside it.

     For example:

     call x
     Int a
     Int b
     call y
     call z
     String buf
     Float x
     scatter {
       call V
     }

     =>

     block
     1        [call x, Int a, Int b]
     2        [call y]
     3        [call z]
     4        [String buf, Float x, scatter]


A block has one toplevel statement, it can
be executed in one job.

Examples for simple blocks

  -- Just expressions
     String s = "hello world"
     Int i = 13
     Float x = z + 4

  -- One top level if
     Int x = k + 1
     if (x > 1) {
       call Add { input: a=1, b=2 }
     }

   -- one top level If, we'll need a subworkflow for the inner
        section
     if (x > 1) {
       call Multiple { ... }
       call Add { ... }
     }

   -- one top level scatter
     scatter (n in names) {
       String full_name = n + " Horowitz"
       call Filter { input: prefix = fullName }
     }

These are not blocks, because we need a subworkflow to run them:

  -- three calls
     call Add { input: a=4, b=14}
     call Sub { input: a=4, b=14}
     call Inc { input: Sub.result }

  -- two consecutive fragments
     if (x > 1) {
        call Inc {input: a=x }
     }
     if (x > 3) {
        call Dec {input: a=x }
     }
 */
package dx.core.languages.wdl

import dx.core.ir.BlockKind.BlockKind
import dx.core.ir.{Block, BlockKind}
import wdlTools.eval.{Eval, EvalException, WdlValues, EvalUtils}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT, TypeUtils}

/**
  * An input to a Block. These are simlar to the TAT.InputParameters, but there is
  * an extra type to disinguish between inputs with constant and dynamic default
  * values, and there is no SourceLocation.
  */
sealed trait WdlBlockInput {
  val name: String
  val wdlType: WdlTypes.T
}

/**
  * A compulsory input that has no default, and must be provided by the caller.
  */
case class RequiredBlockInput(name: String, wdlType: WdlTypes.T) extends WdlBlockInput

/**
  * An input that has a constant default value and may be skipped by the caller.
  */
case class OverridableBlockInputWithStaticDefault(name: String,
                                                  wdlType: WdlTypes.T,
                                                  defaultValue: WdlValues.V)
    extends WdlBlockInput

/**
  * An input that has a default value that is an expression that must be evaluated at runtime,
  * unless a value is specified by the called.
  */
case class OverridableBlockInputWithDynamicDefault(name: String,
                                                   wdlType: WdlTypes.T,
                                                   defaultExpr: TAT.Expr)
    extends WdlBlockInput

/**
  * An input that may be omitted by the caller. In that case the value will
  * be null (or None).
  */
case class OptionalBlockInput(name: String, wdlType: WdlTypes.T) extends WdlBlockInput

object WdlBlockInput {
  private lazy val evaluator: Eval = Eval.empty

  def translate(i: TAT.InputParameter): WdlBlockInput = {
    i match {
      case TAT.RequiredInputParameter(name, wdlType, _) =>
        RequiredBlockInput(name, wdlType)
      case TAT.OverridableInputParameterWithDefault(name, wdlType, defaultExpr, _) =>
        // If the default value is an expression that requires evaluation (i.e. not a
        // constant), treat the input as optional and leave the default value to be
        // calculated at runtime
        try {
          val value = evaluator.applyConstAndCoerce(defaultExpr, wdlType)
          OverridableBlockInputWithStaticDefault(name, wdlType, value)
        } catch {
          case _: EvalException =>
            OverridableBlockInputWithDynamicDefault(
                name,
                TypeUtils.ensureOptional(wdlType),
                defaultExpr
            )
        }
      case TAT.OptionalInputParameter(name, wdlType, _) =>
        OptionalBlockInput(name, wdlType)
    }
  }

  def create(inputs: Map[String, (WdlTypes.T, Boolean)]): Vector[WdlBlockInput] = {
    inputs.map {
      case (name, (wdlType, optional)) =>
        if (optional) {
          OptionalBlockInput(name, wdlType)
        } else {
          RequiredBlockInput(name, wdlType)
        }
    }.toVector
  }

  def isOptional(inputDef: WdlBlockInput): Boolean = {
    inputDef match {
      case _: RequiredBlockInput                      => false
      case _: OverridableBlockInputWithStaticDefault  => true
      case _: OverridableBlockInputWithDynamicDefault => true
      case _: OptionalBlockInput                      => true
    }
  }

  def prettyFormat(input: WdlBlockInput, indent: String = ""): String = {
    input match {
      case RequiredBlockInput(name, wdlType) =>
        s"${indent}${TypeUtils.prettyFormatType(wdlType)} ${name}"
      case OverridableBlockInputWithStaticDefault(name, wdlType, defaultValue) =>
        s"${indent}${TypeUtils.prettyFormatType(wdlType)} ${name} = ${EvalUtils.prettyFormat(defaultValue)}"
      case OverridableBlockInputWithDynamicDefault(name, wdlType, defaultExpr) =>
        s"${indent}${TypeUtils.prettyFormatType(wdlType)} ${name} = ${TypeUtils.prettyFormatExpr(defaultExpr)}"
      case OptionalBlockInput(name, wdlType) =>
        s"${indent}${TypeUtils.prettyFormatType(wdlType)} ${name}"
    }
  }
}

/**
  * A contiguous list of workflow elements from a user workflow.
  *
  * @param inputs all the inputs required for a block (i.e. the Block's closure)
  * @param outputs all the outputs from a sequence of WDL statements - includes
  *                only variables that are used after the block completes.
  * @param elements the elements in the block
  * @example
  * In the workflow:
  *
  *  workflow optionals {
  *    input {
  *      Boolean flag
  *    }
  *    Int? rain = 13
  *    if (flag) {
  *      call opt_MaybeInt as mi3 { input: a=rain }
  *    }
  *  }
  *
  *  The conditional block requires "flag" and "rain".
  *  Note: The type outside a scatter/conditional block is *different* than the
  *  type in the block.  For example, 'Int x' declared inside a scatter, is
  *  'Array[Int] x' outside the scatter.
  */
case class WdlBlock(index: Int,
                    inputs: Vector[WdlBlockInput],
                    outputs: Vector[TAT.OutputParameter],
                    elements: Vector[TAT.WorkflowElement])
    extends Block[WdlBlock] {
  assert(elements.nonEmpty)
  assert(WdlUtils.deepFindCalls(elements.dropRight(1)).isEmpty)

  override lazy val kind: BlockKind = {
    elements.last match {
      case e if WdlUtils.deepFindCalls(Vector(e)).isEmpty =>
        // The block comprises expressions only
        BlockKind.ExpressionsOnly
      case _ if WdlBlock.isTrivialCall(elements) =>
        BlockKind.CallDirect
      case _ if WdlBlock.isSimpleCall(elements) =>
        BlockKind.CallWithSubexpressions
      case _: TAT.Call =>
        BlockKind.CallFragment
      case condNode: TAT.Conditional if WdlBlock.isSimpleCall(condNode.body) =>
        BlockKind.ConditionalOneCall
      case _: TAT.Conditional =>
        BlockKind.ConditionalComplex
      case sctNode: TAT.Scatter if WdlBlock.isSimpleCall(sctNode.body) =>
        BlockKind.ScatterOneCall
      case _: TAT.Scatter =>
        BlockKind.ScatterComplex
    }
  }

  override lazy val getName: Option[String] = {
    elements.collectFirst {
      case TAT.Scatter(id, expr, _, _) =>
        val collection = TypeUtils.prettyFormatExpr(expr)
        s"scatter (${id} in ${collection})"
      case TAT.Conditional(expr, _, _) =>
        val cond = TypeUtils.prettyFormatExpr(expr)
        s"if (${cond})"
      case call: TAT.Call =>
        s"frag ${call.actualName}"
    }
  }

  lazy val (prerequisites, target): (Vector[TAT.WorkflowElement], Option[TAT.WorkflowElement]) = {
    kind match {
      case BlockKind.ExpressionsOnly => (elements, None)
      case _                         => (elements.dropRight(1), Some(elements.last))
    }
  }

  def call: TAT.Call = {
    val calls = (kind, target) match {
      case (BlockKind.CallDirect | BlockKind.CallWithSubexpressions | BlockKind.CallFragment,
            Some(call: TAT.Call)) =>
        Vector(call)
      case (BlockKind.ConditionalOneCall, Some(cond: TAT.Conditional)) =>
        cond.body.collect {
          case call: TAT.Call => call
        }
      case (BlockKind.ScatterOneCall, Some(scatter: TAT.Scatter)) =>
        scatter.body.collect {
          case call: TAT.Call => call
        }
      case _ =>
        throw new Exception(s"block ${this} does not contain a single call")
    }
    assert(calls.size == 1)
    calls.head
  }

  def conditional: TAT.Conditional = {
    (kind, target) match {
      case (BlockKind.ConditionalOneCall | BlockKind.ConditionalComplex,
            Some(cond: TAT.Conditional)) =>
        cond
      case _ =>
        throw new Exception(s"block ${this} is not a conditional")
    }
  }

  def scatter: TAT.Scatter = {
    (kind, target) match {
      case (BlockKind.ScatterOneCall | BlockKind.ScatterComplex, Some(scatter: TAT.Scatter)) =>
        scatter
      case _ =>
        throw new Exception(s"block ${this} is not a scatter")
    }
  }

  def innerElements: Vector[TAT.WorkflowElement] = {
    (kind, target) match {
      case (BlockKind.ConditionalOneCall | BlockKind.ConditionalComplex | BlockKind.ScatterOneCall |
            BlockKind.ScatterComplex,
            Some(block: TAT.BlockElement)) =>
        block.body
      case _ =>
        throw new UnsupportedOperationException(
            s"block ${this} does not have inner elements"
        )
    }
  }

  override def getSubBlock(index: Int): WdlBlock = {
    val innerBlocks = WdlBlock.createBlocks(innerElements)
    innerBlocks(index)
  }

  override lazy val outputNames: Set[String] = outputs.map(_.name).toSet

  override lazy val prettyFormat: String = {
    val inputStr = inputs.map(WdlBlockInput.prettyFormat(_))
    val outputStr = outputs.map(TypeUtils.prettyFormatOutput(_))
    val bodyStr = elements.map(WdlUtils.prettyFormatElement(_)).mkString("\n")
    s"""Block(${index}, ${kind})
       |  Inputs: ${inputStr}
       |  Outputs: ${outputStr}
       |  Body:
       |${bodyStr}""".stripMargin
  }
}

object WdlBlock {

  /**
    * A block of nodes that represents a call with no subexpressions. These
    * can be compiled directly into a dx:workflow stage. For example, the WDL code:
    *  call add { input: a=x, b=y }
    * @param elements WorkflowElements
    * @return
    */
  private def isSimpleCall(elements: Vector[TAT.WorkflowElement]): Boolean = {
    elements match {
      case Vector(_: TAT.Call) => true
      case _                   => false
    }
  }

  /**
    * A simple call that also only has trivial inputs.
    * @param elements WorkflowElements
    * @return
    */
  private def isTrivialCall(elements: Vector[TAT.WorkflowElement]): Boolean = {
    elements match {
      case Vector(call: TAT.Call) =>
        call.inputs.values.forall(expr => WdlUtils.isTrivialExpression(expr))
      case _ => false
    }
  }

  /**
    * Splits a sequence of statements into blocks.
    * @param elements the elements to split
    * @return
    */
  def createBlocks(elements: Vector[TAT.WorkflowElement]): Vector[WdlBlock] = {
    // add to last part
    // if startNew = true, also add a new fresh Vector to the end
    def addToLastPart(parts: Vector[Vector[TAT.WorkflowElement]],
                      elem: TAT.WorkflowElement,
                      startNew: Boolean = false): Vector[Vector[TAT.WorkflowElement]] = {
      val allButLast = parts.dropRight(1)
      val last = parts.last :+ elem
      if (startNew) {
        allButLast ++ Vector(last, Vector.empty[TAT.WorkflowElement])
      } else {
        allButLast :+ last
      }
    }

    // split into sub-sequences (parts). Each part is a vector of workflow elements.
    // We start with a single empty part, which is an empty vector. This ensures that
    // down the line there is at least one part.
    val parts = elements.foldLeft(Vector(Vector.empty[TAT.WorkflowElement])) {
      case (parts, v: TAT.PrivateVariable) =>
        addToLastPart(parts, v)
      case (parts, call: TAT.Call) =>
        addToLastPart(parts, call, startNew = true)
      case (parts, cond: TAT.Conditional) if WdlUtils.deepFindCalls(Vector(cond)).isEmpty =>
        addToLastPart(parts, cond)
      case (parts, cond: TAT.Conditional) =>
        addToLastPart(parts, cond, startNew = true)
      case (parts, sct: TAT.Scatter) if WdlUtils.deepFindCalls(Vector(sct)).isEmpty =>
        addToLastPart(parts, sct)
      case (parts, sct: TAT.Scatter) =>
        addToLastPart(parts, sct, startNew = true)
    }

    // convert to blocks - keep only non-empty blocks
    parts.filter(_.nonEmpty).zipWithIndex.map {
      case (v, index) =>
        val (inputs, outputs) = WdlUtils.getInputOutputClosure(v)
        WdlBlock(index, WdlBlockInput.create(inputs), outputs.values.toVector, v)
    }
  }
}
