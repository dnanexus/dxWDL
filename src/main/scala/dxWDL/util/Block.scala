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
import wdlTools.types.{Util => TUtil, WdlTypes}

case class BlockInput(name: String, wdlType: WdlTypes.T, optional: Boolean)
case class BlockOutput(name: String, wdlType: WdlTypes.T, expr: TAT.Expr)

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
                 nodes: Vector[TAT.WorkflowElement],
                 outputs: Vector[BlockOutput],
                 allOutputs: Vector[BlockOutput]) {
  // Create a human readable name for a block of statements
  //
  // 1. Ignore all declarations
  // 2. If there is a scatter/if, use that
  // 3. if there is at least one call, use the first one.
  //
  // If the entire block is made up of expressions, return None
  def makeName: Option[String] = {
    nodes.collectFirst {
      case TAT.Scatter(id, expr, body, _) =>
        val collection = TUtil.exprToString(expr)
        s"scatter (${id} in ${collection})"
      case TAT.Conditional(expr, body, _) =>
        val cond = TUtil.exprToString(expr)
        s"if (${cond})"
      case call: TAT.Call =>
        s"frag ${call.actualName}"
    }
  }
}

object Block {
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
  //def exprInputs(expr : TAT.Expr) : Vector[String] = ???

  // The block is a singleton with one statement which is a call. The call
  // has no subexpressions. Note that the call may not provide
  // all the callee's arguments.
  def isCallWithNoSubexpressions(node: TAT.WorkflowElement): Boolean = {
    node match {
      case call: TAT.Call =>
        call.inputs.forall {
          case (name, expr) => WomValueAnalysis.isTrivialExpression(expr)
        }
      case _ => false
    }
  }

  // figure out all the outputs from a block of statements
  //
  def allOutputs(elements: Vector[TAT.WorkflowElement]): Map[String, WdlTypes.T] = ???

  def splitToBlocks(elements: Vector[TAT.WorkflowElement]): Vector[Block] = ???

  // split a part of a workflow
  def split(
      statements: Vector[TAT.WorkflowElement]
  ): (Vector[TAT.InputDefinition], // inputs
      Vector[TAT.InputDefinition], // implicit inputs
      Vector[Block], // blocks
      Vector[TAT.OutputDefinition]) // outputs
  = ???

  // Split an entire workflow into blocks.
  //
  // An easy to use method that takes the workflow source
  def splitWorkflow(wf: TAT.Workflow): (Vector[TAT.InputDefinition], // inputs
                                        Vector[TAT.InputDefinition], // implicit inputs
                                        Vector[Block], // blocks
                                        Vector[TAT.OutputDefinition]) // outputs
  = ???

  // We are building an applet for the output section of a workflow.
  // The outputs have expressions, and we need to figure out which
  // variables they refer to. This will allow the calculations to proceeed
  // inside a stand alone applet.
  def outputClosure(outputs: Vector[TAT.OutputDefinition]): Map[String, WdlTypes.T] = ???

  // Does this output require evaluation? If so, we will need to create
  // another applet for this.
  def isSimpleOutput(outputNode: TAT.OutputDefinition): Boolean = {
    WomValueAnalysis.isTrivialExpression(outputNode.expr)
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
  def inputsUsedAsOutputs(inputNodes: Vector[TAT.InputDefinition],
                          outputNodes: Vector[TAT.OutputDefinition]): Set[String] = {
    // Figure out all the variables needed to calculate the outputs
    val outputs: Set[String] = outputClosure(outputNodes).map(_._1).toSet
    val inputs: Set[String] = inputNodes.map(_.name).toSet
    //System.out.println(s"inputsUsedAsOutputs: ${outputs} ${inputs}")
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
    assert(nodes.size > 0)
    if (nodes.size >= 2)
      return false
    // there is example a single node
    val node = nodes.head
    node match {
      case call: TAT.Call if trivialExpressionsOnly =>
        call.inputs.values.forall {
          case expr => WomValueAnalysis.isTrivialExpression(expr)
        }
      case call: TAT.Call =>
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
      .toVector
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
    assert(!block.nodes.isEmpty)
    val allButLast: Vector[TAT.WorkflowElement] = block.nodes.dropRight(1)
    assert(deepFindCalls(allButLast).isEmpty)
    val lastNode = block.nodes.last
    lastNode match {
      case _ if deepFindCalls(Vector(lastNode)).isEmpty =>
        // The block comprises expressions only
        AllExpressions(allButLast :+ lastNode)

      case callNode: TAT.Call =>
        if (isSimpleCall(block.nodes, true))
          CallDirect(allButLast, callNode)
        else if (isSimpleCall(block.nodes, false))
          CallWithSubexpressions(allButLast, callNode)
        else
          CallFragment(allButLast, callNode)

      case condNode: TAT.Conditional =>
        if (isSimpleCall(condNode.body, false)) {
          CondOneCall(allButLast, condNode, getOneCall(condNode.body))
        } else {
          CondFullBlock(allButLast, condNode)
        }

      case sctNode: TAT.Scatter =>
        if (isSimpleCall(sctNode.body, false)) {
          ScatterOneCall(allButLast, sctNode, getOneCall(sctNode.body))
        } else {
          ScatterFullBlock(allButLast, sctNode)
        }
    }
  }

  def getSubBlock(path: Vector[Int], nodes: Vector[TAT.WorkflowElement]): Block = {
    assert(path.size >= 1)

    val blocks = splitToBlocks(nodes)
    var subBlock = blocks(path.head)
    for (i <- path.tail) {
      val catg = categorize(subBlock)
      val innerGraph = Category.getInnerGraph(catg)
      val blocks2 = splitToBlocks(innerGraph)
      subBlock = blocks2(i)
    }
    return subBlock
  }
}
