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

import wdlTools.eval.{Eval, EvalException, WdlValues}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT, Utils => TUtils}

sealed trait BlockInput {
  val name: String
  val wdlType: WdlTypes.T
}

// A compulsory input that has no default, and must be provided by the caller
case class RequiredBlockInput(name: String, wdlType: WdlTypes.T) extends BlockInput

// An input that has a constant default value and may be skipped by the caller
case class OverridableBlockInputWithConstantDefault(name: String,
                                                    wdlType: WdlTypes.T,
                                                    defaultValue: WdlValues.V)
    extends BlockInput

// An input that has a default value that is an expression that must be evaluated at runtime,
// unless a value is specified by the caller
case class OverridableBlockInputWithDynamicDefault(name: String,
                                                   wdlType: WdlTypes.T,
                                                   defaultExpr: TAT.Expr)
    extends BlockInput

// an input that may be omitted by the caller. In that case the value will
// be null (or None).
case class OptionalBlockInput(name: String, wdlType: WdlTypes.T) extends BlockInput

object BlockInput {
  private lazy val evaluator: Eval = Eval.empty

  def translate(i: TAT.InputDefinition): BlockInput = {
    i match {
      case TAT.RequiredInputDefinition(name, wdlType, _) =>
        RequiredBlockInput(name, wdlType)
      case TAT.OverridableInputDefinitionWithDefault(name, wdlType, defaultExpr, _) =>
        // If the default value is an expression that requires evaluation (i.e. not a
        // constant), treat the input as optional and leave the default value to be
        // calculated at runtime
        try {
          val value = evaluator.applyConstAndCoerce(defaultExpr, wdlType)
          OverridableBlockInputWithConstantDefault(name, wdlType, value)
        } catch {
          case _: EvalException =>
            OverridableBlockInputWithDynamicDefault(
                name,
                TUtils.makeOptional(wdlType),
                defaultExpr
            )
        }
      case TAT.OptionalInputDefinition(name, wdlType, _) =>
        OptionalBlockInput(name, wdlType)
    }
  }

  def isOptional(inputDef: BlockInput): Boolean = {
    inputDef match {
      case _: RequiredBlockInput                       => false
      case _: OverridableBlockInputWithConstantDefault => true
      case _: OverridableBlockInputWithDynamicDefault  => true
      case _: OptionalBlockInput                       => true
    }
  }
}

// Block: a continuous list of workflow elements from a user
// workflow.
//
// INPUTS: all the inputs required for a block. For example,
// in the workflow:
//
// workflow optionals {
//   input {
//     Boolean flag
//   }
//   Int? rain = 13
//   if (flag) {
//     call opt_MaybeInt as mi3 { input: a=rain }
//   }
// }
//
// The conditional block:
//    if (flag) {
//     call opt_MaybeInt as mi3 { input: a=rain }
//    }
// requires "flag" and "rain".
//
// For each input also return whether is has a default. This makes it,
// de facto, optional.
//
// OUTPUTS: all the outputs from a sequence of WDL statements. This includes -only-
// variables that are used after the block completes.
//
// ALL_OUTPUTS : all the outputs, including those that are unused.
//
// Note: The type outside a scatter/conditional block is *different* than the type in
// the block.  For example, 'Int x' declared inside a scatter, is
// 'Array[Int] x' outside the scatter.
//
case class Block(inputs: Vector[BlockInput],
                 outputs: Vector[TAT.OutputDefinition],
                 nodes: Vector[TAT.WorkflowElement]) {
  // Create a human readable name for a block of statements
  //
  // 1. Ignore all declarations
  // 2. If there is a scatter/if, use that
  // 3. if there is at least one call, use the first one.
  //
  // If the entire block is made up of expressions, return None
  def makeName: Option[String] = {
    nodes.collectFirst {
      case TAT.Scatter(id, expr, _, _) =>
        val collection = TUtils.prettyFormatExpr(expr)
        s"scatter (${id} in ${collection})"
      case TAT.Conditional(expr, _, _) =>
        val cond = TUtils.prettyFormatExpr(expr)
        s"if (${cond})"
      case call: TAT.Call =>
        s"frag ${call.actualName}"
    }
  }
}

object Block {
  // Deep search for all calls in a graph
  def deepFindCalls(nodes: Vector[TAT.WorkflowElement]): Vector[TAT.Call] = {
    nodes
      .foldLeft(Vector.empty[TAT.Call]) {
        case (accu, call: TAT.Call) =>
          accu :+ call
        case (accu, ssc: TAT.Scatter) =>
          accu ++ deepFindCalls(ssc.body)
        case (accu, ifStmt: TAT.Conditional) =>
          accu ++ deepFindCalls(ifStmt.body)
        case (accu, _) =>
          accu
      }
  }

  def deepFindCalls(node: TAT.WorkflowElement): Vector[TAT.Call] = {
    deepFindCalls(Vector(node))
  }

  // Check that this block is valid.
  def validate(b: Block): Unit = {
    // The only calls allowed are in the last node
    val allButLast = b.nodes.dropRight(1)
    val allCalls = deepFindCalls(allButLast)
    assert(allCalls.isEmpty)
  }

  // figure out the identifiers used in an expression.
  //
  // For example:
  //   expression   inputs
  //   x + y        Vector(x, y)
  //   x + y + z    Vector(x, y, z)
  //   1 + 9        Vector.empty
  //   "a" + "b"    Vector.empty
  //   foo.y + 3    Vector(foo.y)   [withMember = false]
  //   foo.y + 3    Vector(foo)     [withMember = true]
  //
  private def exprInputs(expr: TAT.Expr, withMember: Boolean = true): Vector[BlockInput] = {
    def inner(expr: TAT.Expr): Vector[BlockInput] = {
      expr match {
        case _: TAT.ValueNull      => Vector.empty
        case _: TAT.ValueNone      => Vector.empty
        case _: TAT.ValueBoolean   => Vector.empty
        case _: TAT.ValueInt       => Vector.empty
        case _: TAT.ValueFloat     => Vector.empty
        case _: TAT.ValueString    => Vector.empty
        case _: TAT.ValueFile      => Vector.empty
        case _: TAT.ValueDirectory => Vector.empty
        case TAT.ExprIdentifier(id, wdlType, _) =>
          val outputDef = wdlType match {
            case optType: WdlTypes.T_Optional =>
              OptionalBlockInput(id, optType)
            case _ =>
              RequiredBlockInput(id, wdlType)
          }
          Vector(outputDef)

        case TAT.ExprCompoundString(valArr, _, _) =>
          valArr.flatMap(elem => inner(elem))
        case TAT.ExprPair(l, r, _, _) =>
          inner(l) ++ inner(r)
        case TAT.ExprArray(arrVal, _, _) =>
          arrVal.flatMap(elem => inner(elem))
        case TAT.ExprMap(valMap, _, _) =>
          valMap
            .map { case (k, v) => inner(k) ++ inner(v) }
            .toVector
            .flatten
        case TAT.ExprObject(fields, _, _) =>
          fields
            .map { case (_, v) => inner(v) }
            .toVector
            .flatten

        case TAT.ExprPlaceholderEqual(t: TAT.Expr, f: TAT.Expr, value: TAT.Expr, _, _) =>
          inner(t) ++ inner(f) ++ inner(value)
        case TAT.ExprPlaceholderDefault(default: TAT.Expr, value: TAT.Expr, _, _) =>
          inner(default) ++ inner(value)
        case TAT.ExprPlaceholderSep(sep: TAT.Expr, value: TAT.Expr, _, _) =>
          inner(sep) ++ inner(value)

        // Access an array element at [index]
        case TAT.ExprAt(value, index, _, _) =>
          inner(value) ++ inner(index)

        // conditional:
        case TAT.ExprIfThenElse(cond, tBranch, fBranch, _, _) =>
          inner(cond) ++ inner(tBranch) ++ inner(fBranch)

        // Apply a standard library function to arguments.
        //
        // TODO: some arguments may be _optional_ we need to take that
        // into account. We need to look into the function type
        // and figure out which arguments are optional.
        case TAT.ExprApply(_, _, elements, _, _) =>
          elements.flatMap(inner)

        // Access the field of a call/struct/etc. What we do here depends on the
        // value of withMember. When we the expression value is a struct and we
        // are generating a closure, we only need the parent struct, not the member.
        // Otherwise, we need to add the member name.

        // Note: this case was added to fix bug/APPS-104 - there may be other expressions
        // besides structs that need to not have the member name added when withMember = false.
        // It may also be the case that the bug is with construction of the environment rather
        // than here with the closure.
        case TAT.ExprGetName(expr, _, _, _)
            if !withMember && TUtils.unwrapOptional(expr.wdlType).isInstanceOf[WdlTypes.T_Struct] =>
          inner(expr)

        // Access a field of an identifier
        //   Int z = eliminateDuplicate.fields
        case TAT.ExprGetName(TAT.ExprIdentifier(id, _, _), fieldName, wdlType, _) =>
          Vector(RequiredBlockInput(s"${id}.${fieldName}", wdlType))

        // Access a field of the result of an expression
        case TAT.ExprGetName(expr, fieldName, wdlType, _) =>
          inner(expr) match {
            case Vector(i: BlockInput) =>
              Vector(RequiredBlockInput(s"${i.name}.${fieldName}", wdlType))
            case _ =>
              throw new Exception(
                  s"Unhandled ExprGetName construction ${TUtils.prettyFormatExpr(expr)}"
              )
          }

        case other =>
          throw new Exception(s"Unhandled expression ${other}")
      }
    }
    inner(expr)
  }

  private def callInputs(call: TAT.Call): Vector[BlockInput] = {
    // What the callee expects
    call.callee.input.flatMap {
      case (name: String, (_: WdlTypes.T, optional: Boolean)) =>
        // provided by the caller
        val actualInput = call.inputs.get(name)
        (actualInput, optional) match {
          case (None, false) =>
            // A required input that will have to be provided at runtime
            Vector.empty[BlockInput]
          case (Some(expr), false) =>
            // required input that is provided
            exprInputs(expr)
          case (None, true) =>
            // a missing optional input, doesn't have to be provided
            Vector.empty[BlockInput]
          case (Some(expr), true) =>
            // an optional input
            exprInputs(expr).map { inpDef: BlockInput =>
              OptionalBlockInput(inpDef.name, inpDef.wdlType)
            }
        }
    }.toVector
  }

  // keep track of the inputs, outputs, and local definitions when
  // traversing a block of workflow elements (declarations, calls, scatters, and conditionals)
  private case class BlockContext(inputs: Map[String, BlockInput],
                                  outputs: Map[String, TAT.OutputDefinition]) {

    // remove from a list of potential inputs those that are locally defined.
    def addInputs(newIdentifiedInputs: Vector[BlockInput]): BlockContext = {
      val reallyNew = newIdentifiedInputs.filter { x: BlockInput =>
        !((inputs.keys.toSet contains x.name) ||
          (outputs.keys.toSet contains x.name))
      }
      this.copy(inputs = inputs ++ reallyNew.map { x =>
        x.name -> x
      }.toMap)
    }

    def addOutputs(newOutputs: Vector[TAT.OutputDefinition]): BlockContext = {
      this.copy(outputs = outputs ++ newOutputs.map { x =>
        x.name -> x
      }.toMap)
    }

    def removeIdentifier(id: String): BlockContext = {
      BlockContext(this.inputs - id, this.outputs - id)
    }
  }

  private object BlockContext {
    val empty: BlockContext = BlockContext(Map.empty, Map.empty)
  }

  // create a block from a group of statements.
  // requires calculating all the inputs and outputs
  //
  private def makeBlock(elements: Vector[TAT.WorkflowElement]): Block = {
    // accumulate the inputs, outputs, and local definitions.
    //
    // start with:
    //  an empty list of inputs
    //  empty list of local definitions
    //  empty list of outputs
    val ctx = elements.foldLeft(BlockContext.empty) {
      case (ctx, elem) =>
        elem match {
          case decl: TAT.Declaration =>
            val declInputs = decl.expr match {
              case None       => Vector.empty
              case Some(expr) => exprInputs(expr)
            }
            val declOutputs =
              decl.expr match {
                case None => Vector.empty
                case Some(expr) =>
                  Vector(TAT.OutputDefinition(decl.name, decl.wdlType, expr, decl.loc))
              }
            ctx
              .addInputs(declInputs)
              .addOutputs(declOutputs)

          case call: TAT.Call =>
            val callOutputs = call.callee.output.map {
              case (name, wdlType) =>
                val fqn = call.actualName + "." + name
                TAT.OutputDefinition(fqn,
                                     wdlType,
                                     TAT.ExprIdentifier(fqn, wdlType, call.loc),
                                     call.loc)
            }.toVector
            ctx
              .addInputs(callInputs(call))
              .addOutputs(callOutputs)

          case cond: TAT.Conditional =>
            // recurse into body of conditional
            val subBlock = makeBlock(cond.body)

            // make outputs optional
            val subBlockOutputs = subBlock.outputs.map {
              case TAT.OutputDefinition(name, wdlType, expr, loc) =>
                TAT.OutputDefinition(name, TUtils.makeOptional(wdlType), expr, loc)
            }
            ctx
              .addInputs(subBlock.inputs)
              .addInputs(exprInputs(cond.expr))
              .addOutputs(subBlockOutputs)

          case sct: TAT.Scatter =>
            // recurse into body of the scatter
            val subBlock = makeBlock(sct.body)

            // make outputs arrays
            val subBlockOutputs = subBlock.outputs.map {
              case TAT.OutputDefinition(name, wdlType, expr, loc) =>
                TAT.OutputDefinition(name, WdlTypes.T_Array(wdlType, nonEmpty = false), expr, loc)
            }
            val ctx2 = ctx
              .addInputs(subBlock.inputs)
              .addInputs(exprInputs(sct.expr))
              .addOutputs(subBlockOutputs)

            // remove the collection iteration variable
            ctx2.removeIdentifier(sct.identifier)
        }
    }
    Block(ctx.inputs.values.toVector, ctx.outputs.values.toVector, elements)
  }

  // split a sequence of statements into blocks
  //
  private def splitToBlocks(elements: Vector[TAT.WorkflowElement]): Vector[Block] = {
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
      case (parts, decl: TAT.Declaration) =>
        addToLastPart(parts, decl)
      case (parts, call: TAT.Call) =>
        addToLastPart(parts, call, startNew = true)
      case (parts, cond: TAT.Conditional) if deepFindCalls(cond).isEmpty =>
        addToLastPart(parts, cond)
      case (parts, cond: TAT.Conditional) =>
        addToLastPart(parts, cond, startNew = true)
      case (parts, sct: TAT.Scatter) if deepFindCalls(sct).isEmpty =>
        addToLastPart(parts, sct)
      case (parts, sct: TAT.Scatter) =>
        addToLastPart(parts, sct, startNew = true)
    }

    // convert to blocks - keep only non-empty blocks
    parts.collect { case v if v.nonEmpty => makeBlock(v) }
  }

  // We are building an applet for the output section of a workflow.
  // The outputs have expressions, and we need to figure out which
  // variables they refer to. This will allow the calculations to proceeed
  // inside a stand alone applet.
  def outputClosure(outputs: Vector[TAT.OutputDefinition]): Map[String, WdlTypes.T] = {
    // create inputs from all the expressions that go into outputs
    outputs
      .flatMap {
        case TAT.OutputDefinition(_, _, expr, _) => Vector(expr)
      }
      .flatMap(e => exprInputs(e, withMember = false))
      .foldLeft(Map.empty[String, WdlTypes.T]) {
        case (accu, inpDef) =>
          accu + (inpDef.name -> inpDef.wdlType)
      }
  }

  // figure out all the outputs from a block of statements
  //
  def allOutputs(elements: Vector[TAT.WorkflowElement]): Map[String, WdlTypes.T] = {
    val b = makeBlock(elements)
    b.outputs.map {
      case TAT.OutputDefinition(name, wdlType, _, _) =>
        name -> wdlType
    }.toMap
  }

  // split a part of a workflow
  def split(
      statements: Vector[TAT.WorkflowElement]
  ): (Vector[BlockInput], Vector[Block], Vector[TAT.OutputDefinition]) = {
    val top: Block = makeBlock(statements)
    val subBlocks = splitToBlocks(statements)
    (top.inputs, subBlocks, top.outputs)
  }

  // Split an entire workflow into blocks.
  //
  def splitWorkflow(wf: TAT.Workflow): Vector[Block] = {
    splitToBlocks(wf.body)
  }

  // A trivial expression has no operators, it is either (1) a
  // constant, (2) a single identifier, or (3) an access to a call
  // field.
  //
  // For example, `5`, `['a', 'b', 'c']`, and `true` are trivial.
  // 'x + y'  is not.
  def isTrivialExpression(expr: TAT.Expr): Boolean = {
    expr match {
      case expr if TUtils.isPrimitiveValue(expr) => true
      case _: TAT.ExprIdentifier                 => true

      // A collection of constants
      case TAT.ExprPair(l, r, _, _)   => Vector(l, r).forall(TUtils.isPrimitiveValue)
      case TAT.ExprArray(value, _, _) => value.forall(TUtils.isPrimitiveValue)
      case TAT.ExprMap(value, _, _) =>
        value.forall {
          case (k, v) => TUtils.isPrimitiveValue(k) && TUtils.isPrimitiveValue(v)
        }
      case TAT.ExprObject(value, _, _) => value.values.forall(TUtils.isPrimitiveValue)

      // Access a field in a call or a struct
      //   Int z = eliminateDuplicate.fields
      case TAT.ExprGetName(_: TAT.ExprIdentifier, _, _, _) => true

      case _ => false
    }
  }

  // A block of nodes that represents a call with no subexpressions. These
  // can be compiled directly into a dx:workflow stage.
  //
  // For example, the WDL code:
  // call add { input: a=x, b=y }
  //
  private def isSimpleCall(nodes: Vector[TAT.WorkflowElement],
                           trivialExpressionsOnly: Boolean): Boolean = {
    assert(nodes.nonEmpty)
    if (nodes.size > 1)
      return false
    nodes.head match {
      case call: TAT.Call if trivialExpressionsOnly =>
        call.inputs.values.forall(expr => isTrivialExpression(expr))
      case _: TAT.Call =>
        // any input expression is allowed
        true
      case _ => false
    }
  }

  private def getOneCall(nodes: Vector[TAT.WorkflowElement]): TAT.Call = {
    val calls = nodes.collect {
      case node: TAT.Call => node
    }
    assert(calls.size == 1)
    calls.head
  }

  // These are the kinds of blocks that are run by the workflow-fragment-runner.
  //
  // A block can have expressions, input ports, and output ports in the beginning.
  // The block can be one of these types:
  //
  //   Purely expressions (no asynchronous calls at any nesting level).
  //
  //   Call
  //      - with no subexpressions to evaluate
  //      - with subexpressions requiring evaluation
  //      - Fragment with expressions and one call
  //
  //   Conditional block
  //      - with exactly one call
  //      - a complex subblock
  //
  //   Scatter block
  //      - with exactly one call
  //      - with a complex subblock
  //
  //   Compound
  //      contains multiple calls and/or dependencies
  sealed trait Category {
    val nodes: Vector[TAT.WorkflowElement]
  }
  case class AllExpressions(nodes: Vector[TAT.WorkflowElement]) extends Category
  case class CallDirect(nodes: Vector[TAT.WorkflowElement], value: TAT.Call) extends Category
  case class CallWithSubexpressions(nodes: Vector[TAT.WorkflowElement], value: TAT.Call)
      extends Category
  case class CallFragment(nodes: Vector[TAT.WorkflowElement], value: TAT.Call) extends Category
  case class CondOneCall(nodes: Vector[TAT.WorkflowElement], value: TAT.Conditional, call: TAT.Call)
      extends Category
  case class CondFullBlock(nodes: Vector[TAT.WorkflowElement], value: TAT.Conditional)
      extends Category
  case class ScatterOneCall(nodes: Vector[TAT.WorkflowElement], value: TAT.Scatter, call: TAT.Call)
      extends Category
  case class ScatterFullBlock(nodes: Vector[TAT.WorkflowElement], value: TAT.Scatter)
      extends Category

  object Category {
    def getInnerGraph(catg: Category): Vector[TAT.WorkflowElement] = {
      catg match {
        case cond: CondFullBlock   => cond.value.body
        case sct: ScatterFullBlock => sct.value.body
        case other =>
          throw new UnsupportedOperationException(
              s"${other.getClass.toString} does not have an inner graph"
          )
      }
    }
    def toString(catg: Category): String = {
      // convert Block$Cond to Cond
      val fullClassName = catg.getClass.toString
      val index = fullClassName.lastIndexOf('$')
      if (index == -1)
        fullClassName
      else
        fullClassName.substring(index + 1)
    }
  }

  def categorize(block: Block): Category = {
    assert(block.nodes.nonEmpty)
    val allButLast: Vector[TAT.WorkflowElement] = block.nodes.dropRight(1)
    assert(deepFindCalls(allButLast).isEmpty)
    val lastNode = block.nodes.last
    lastNode match {
      case _ if deepFindCalls(Vector(lastNode)).isEmpty =>
        // The block comprises expressions only
        AllExpressions(allButLast :+ lastNode)

      case callNode: TAT.Call =>
        if (isSimpleCall(block.nodes, trivialExpressionsOnly = true))
          CallDirect(allButLast, callNode)
        else if (isSimpleCall(block.nodes, trivialExpressionsOnly = false))
          CallWithSubexpressions(allButLast, callNode)
        else
          CallFragment(allButLast, callNode)

      case condNode: TAT.Conditional =>
        if (isSimpleCall(condNode.body, trivialExpressionsOnly = false)) {
          CondOneCall(allButLast, condNode, getOneCall(condNode.body))
        } else {
          CondFullBlock(allButLast, condNode)
        }

      case sctNode: TAT.Scatter =>
        if (isSimpleCall(sctNode.body, trivialExpressionsOnly = false)) {
          ScatterOneCall(allButLast, sctNode, getOneCall(sctNode.body))
        } else {
          ScatterFullBlock(allButLast, sctNode)
        }
    }
  }

  def getSubBlock(path: Vector[Int], nodes: Vector[TAT.WorkflowElement]): Block = {
    assert(path.nonEmpty)

    val blocks = splitToBlocks(nodes)
    var subBlock = blocks(path.head)
    for (i <- path.tail) {
      val catg = categorize(subBlock)
      val innerGraph = Category.getInnerGraph(catg)
      val blocks2 = splitToBlocks(innerGraph)
      subBlock = blocks2(i)
    }
    subBlock
  }
}
