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

package dxWDL.util

import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.types.{WdlTypes, Util => TUtil}

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
case class Block(inputs: Vector[Block.InputDefinition],
                 outputs: Vector[Block.OutputDefinition],
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
        val collection = TUtil.exprToString(expr)
        s"scatter (${id} in ${collection})"
      case TAT.Conditional(expr, _, _) =>
        val cond = TUtil.exprToString(expr)
        s"if (${cond})"
      case call: TAT.Call =>
        s"frag ${call.actualName}"
    }
  }
}

object Block {
  // These are the same definitions as in TypedAbstractSyntax,
  // with the TextSource field stripped out. This is because the inputs
  // and outputs here are compiler constructs, they have not been defined by the user.
  // They are computed by the algorithm and assigned to blocks of
  // statements.
  sealed trait InputDefinition {
    val name: String
    val wdlType: WdlTypes.T
  }

  // A compulsory input that has no default, and must be provided by the caller
  case class RequiredInputDefinition(name: String, wdlType: WdlTypes.T) extends InputDefinition

  // An input that has a default and may be skipped by the caller
  case class OverridableInputDefinitionWithDefault(name: String,
                                                   wdlType: WdlTypes.T,
                                                   defaultExpr: TAT.Expr)
      extends InputDefinition

  // an input that may be omitted by the caller. In that case the value will
  // be null (or None).
  case class OptionalInputDefinition(name: String, wdlType: WdlTypes.T) extends InputDefinition

  case class OutputDefinition(name: String, wdlType: WdlTypes.T, expr: TAT.Expr)

  def translate(i: TAT.InputDefinition): InputDefinition = {
    i match {
      case TAT.RequiredInputDefinition(name, wdlType, _) =>
        RequiredInputDefinition(name, wdlType)
      case TAT.OverridableInputDefinitionWithDefault(name, wdlType, expr, _) =>
        OverridableInputDefinitionWithDefault(name, wdlType, expr)
      case TAT.OptionalInputDefinition(name, wdlType, _) =>
        OptionalInputDefinition(name, wdlType)
    }
  }

  def translate(o: TAT.OutputDefinition): OutputDefinition = {
    OutputDefinition(o.name, o.wdlType, o.expr)
  }

  def isOptional(inputDef: InputDefinition): Boolean = {
    inputDef match {
      case _: RequiredInputDefinition               => false
      case _: OverridableInputDefinitionWithDefault => true
      case _: OptionalInputDefinition               => true
    }
  }

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

  // figure out the inputs from an expression
  //
  // For example:
  //   expression   inputs
  //   x + y        Vector(x, y)
  //   x + y + z    Vector(x, y, z)
  //   foo.y + 3    Vector(foo.y)
  //   1 + 9        Vector.empty
  //   "a" + "b"    Vector.empty
  //
  private def exprInputs(expr: TAT.Expr): Vector[InputDefinition] = {
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
            OptionalInputDefinition(id, optType)
          case _ =>
            RequiredInputDefinition(id, wdlType)
        }
        Vector(outputDef)

      case TAT.ExprCompoundString(valArr, _, _) =>
        valArr.flatMap(elem => exprInputs(elem))
      case TAT.ExprPair(l, r, _, _) =>
        exprInputs(l) ++ exprInputs(r)
      case TAT.ExprArray(arrVal, _, _) =>
        arrVal.flatMap(elem => exprInputs(elem))
      case TAT.ExprMap(valMap, _, _) =>
        valMap
          .map { case (k, v) => exprInputs(k) ++ exprInputs(v) }
          .toVector
          .flatten
      case TAT.ExprObject(fields, _, _) =>
        fields
          .map { case (_, v) => exprInputs(v) }
          .toVector
          .flatten

      case TAT.ExprPlaceholderEqual(t: TAT.Expr, f: TAT.Expr, value: TAT.Expr, _, _) =>
        exprInputs(t) ++ exprInputs(f) ++ exprInputs(value)
      case TAT.ExprPlaceholderDefault(default: TAT.Expr, value: TAT.Expr, _, _) =>
        exprInputs(default) ++ exprInputs(value)
      case TAT.ExprPlaceholderSep(sep: TAT.Expr, value: TAT.Expr, _, _) =>
        exprInputs(sep) ++ exprInputs(value)

      // operators on one argument
      case oper1: TAT.ExprOperator1 => exprInputs(oper1.value)

      // operators on two arguments
      case oper2: TAT.ExprOperator2 => exprInputs(oper2.a) ++ exprInputs(oper2.b)

      // Access an array element at [index]
      case TAT.ExprAt(value, index, _, _) =>
        exprInputs(value) ++ exprInputs(index)

      // conditional:
      case TAT.ExprIfThenElse(cond, tBranch, fBranch, _, _) =>
        exprInputs(cond) ++ exprInputs(tBranch) ++ exprInputs(fBranch)

      // Apply a standard library function to arguments.
      //
      // TODO: some arguments may be _optional_ we need to take that
      // into account. We need to look into the function type
      // and figure out which arguments are optional.
      case TAT.ExprApply(_, _, elements, _, _) =>
        elements.flatMap(exprInputs)

      // Access a field in a call
      //   Int z = eliminateDuplicate.fields
      case TAT.ExprGetName(TAT.ExprIdentifier(id, _, _), fieldName, wdlType, _) =>
        Vector(RequiredInputDefinition(id + "." + fieldName, wdlType))

      case TAT.ExprGetName(expr, _, _, _) =>
        throw new Exception(s"Unhandled ExprGetName construction ${TUtil.exprToString(expr)}")
    }
  }

  private def callInputs(call: TAT.Call): Vector[InputDefinition] = {
    // What the callee expects
    call.callee.input.flatMap {
      case (name: String, (_: WdlTypes.T, optional: Boolean)) =>
        // provided by the caller
        val actualInput = call.inputs.get(name)

        (actualInput, optional) match {
          case (None, false) =>
            // A required input that will have to be provided at runtime
            Vector.empty[InputDefinition]
          case (Some(expr), false) =>
            // required input that is provided
            exprInputs(expr)
          case (None, true) =>
            // a missing optional input, doesn't have to be provided
            Vector.empty[InputDefinition]
          case (Some(expr), true) =>
            // an optional input
            exprInputs(expr).map { inpDef: InputDefinition =>
              OptionalInputDefinition(inpDef.name, inpDef.wdlType)
            }
        }
    }.toVector
  }

  private def makeOptional(t: WdlTypes.T): WdlTypes.T = {
    t match {
      case _: WdlTypes.T_Optional => t
      case _                      => WdlTypes.T_Optional(t)
    }
  }

  // keep track of the inputs, outputs, and local definitions when
  // traversing a block of workflow elements (declarations, calls, scatters, and conditionals)
  private case class BlockContext(inputs: Map[String, InputDefinition],
                                  outputs: Map[String, OutputDefinition]) {

    // remove from a list of potential inputs those that are locally defined.
    def addInputs(newIdentifiedInputs: Vector[InputDefinition]): BlockContext = {
      val reallyNew = newIdentifiedInputs.filter { x: InputDefinition =>
        !((inputs.keys.toSet contains x.name) ||
          (outputs.keys.toSet contains x.name))
      }
      this.copy(inputs = inputs ++ reallyNew.map { x =>
        x.name -> x
      }.toMap)
    }

    def addOutputs(newOutputs: Vector[OutputDefinition]): BlockContext = {
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
                case None       => Vector.empty
                case Some(expr) => Vector(OutputDefinition(decl.name, decl.wdlType, expr))
              }
            ctx
              .addInputs(declInputs)
              .addOutputs(declOutputs)

          case call: TAT.Call =>
            val callOutputs = call.callee.output.map {
              case (name, wdlType) =>
                val fqn = call.actualName + "." + name
                OutputDefinition(fqn, wdlType, TAT.ExprIdentifier(fqn, wdlType, call.text))
            }.toVector
            ctx
              .addInputs(callInputs(call))
              .addOutputs(callOutputs)

          case cond: TAT.Conditional =>
            // recurse into body of conditional
            val subBlock = makeBlock(cond.body)

            // make outputs optional
            val subBlockOutputs = subBlock.outputs.map {
              case OutputDefinition(name, wdlType, expr) =>
                OutputDefinition(name, makeOptional(wdlType), expr)
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
              case OutputDefinition(name, wdlType, expr) =>
                OutputDefinition(name, WdlTypes.T_Array(wdlType, nonEmpty = false), expr)
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
    def addToLastPart(parts: Vector[Vector[TAT.WorkflowElement]],
                      elem: TAT.WorkflowElement): Vector[Vector[TAT.WorkflowElement]] = {
      val allButLast = parts.dropRight(1)
      val last = parts.last :+ elem
      allButLast :+ last
    }
    // add to last part and start a new one.
    def startFresh(parts: Vector[Vector[TAT.WorkflowElement]],
                   elem: TAT.WorkflowElement): Vector[Vector[TAT.WorkflowElement]] = {
      val parts2 = addToLastPart(parts, elem)
      parts2 :+ Vector.empty[TAT.WorkflowElement]
    }

    // split into sub-sequences (parts). Each part is a vector of workflow elements.
    // We start with a single empty part, which is an empty vector. This ensures that
    // down the line there is at least one part.
    val parts = elements.foldLeft(Vector(Vector.empty[TAT.WorkflowElement])) {
      case (parts, decl: TAT.Declaration) =>
        addToLastPart(parts, decl)
      case (parts, call: TAT.Call) =>
        startFresh(parts, call)
      case (parts, cond: TAT.Conditional) if deepFindCalls(cond).isEmpty =>
        addToLastPart(parts, cond)
      case (parts, cond: TAT.Conditional) =>
        startFresh(parts, cond)
      case (parts, sct: TAT.Scatter) if deepFindCalls(sct).isEmpty =>
        addToLastPart(parts, sct)
      case (parts, sct: TAT.Scatter) =>
        startFresh(parts, sct)
    }

    // if the last block is empty, drop it
    val cleanParts =
      if (parts.last.isEmpty)
        parts.dropRight(1)
      else
        parts

    // convert to blocks
    cleanParts.map(makeBlock)
  }

  // We are building an applet for the output section of a workflow.
  // The outputs have expressions, and we need to figure out which
  // variables they refer to. This will allow the calculations to proceeed
  // inside a stand alone applet.
  def outputClosure(outputs: Vector[OutputDefinition]): Map[String, WdlTypes.T] = {
    // create inputs from all the expressions that go into outputs
    outputs
      .flatMap {
        case OutputDefinition(_, _, expr) => Vector(expr)
      }
      .flatMap(exprInputs)
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
      case OutputDefinition(name, wdlType, _) =>
        name -> wdlType
    }.toMap
  }

  // split a part of a workflow
  def split(
      statements: Vector[TAT.WorkflowElement]
  ): (Vector[InputDefinition], Vector[Block], Vector[OutputDefinition]) = {
    val top: Block = makeBlock(statements)
    val subBlocks = splitToBlocks(statements)
    (top.inputs, subBlocks, top.outputs)
  }

  // Split an entire workflow into blocks.
  //
  def splitWorkflow(wf: TAT.Workflow): Vector[Block] = {
    splitToBlocks(wf.body)
  }

  // Does this output require evaluation? If so, we will need to create
  // another applet for this.
  def isSimpleOutput(outputNode: OutputDefinition, definedVars: Set[String]): Boolean = {
    if (definedVars.contains(outputNode.name)) {
      // the environment has a stage with this output node - we can get it by linking
      true
    } else {
      // check if the expression can be resolved without evaluation (i.e. is a constant
      // or a reference to a defined variable
      outputNode.expr match {
        // A constant or a reference to a variable
        case expr if WdlValueAnalysis.isTrivialExpression(expr)      => true
        case TAT.ExprIdentifier(id, _, _) if definedVars contains id => true

        // for example, c1 is call, and the output section is:
        //
        // output {
        //    Int? result1 = c1.result
        //    Int? result2 = c2.result
        // }
        // We don't need a special output applet+stage.
        case TAT.ExprGetName(TAT.ExprIdentifier(id2, _, _), id, _, _) =>
          val fqn = s"$id2.$id"
          definedVars contains fqn

        case _ => false
      }
    }
  }

  // is an output used directly as an input? For example, in the
  // small workflow below, 'lane' is used in such a manner.
  //
  // This makes a difference, because in locked dx:workflows, it is
  // not possible to access a workflow input directly from a workflow
  // output. It is only allowed to access a stage input/output.
  //
  // workflow inner {
  //   input {
  //      String lane
  //   }
  //   output {
  //      String blah = lane
  //   }
  // }
  def inputsUsedAsOutputs(inputNodes: Vector[InputDefinition],
                          outputNodes: Vector[OutputDefinition]): Set[String] = {
    // Figure out all the variables needed to calculate the outputs
    val outputs: Set[String] = outputClosure(outputNodes).keySet
    val inputs: Set[String] = inputNodes.map(_.name).toSet
    inputs.intersect(outputs)
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
    if (nodes.size >= 2)
      return false
    // there is example a single node
    val node = nodes.head
    node match {
      case call: TAT.Call if trivialExpressionsOnly =>
        call.inputs.values.forall(expr => WdlValueAnalysis.isTrivialExpression(expr))
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
