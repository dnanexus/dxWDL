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
case class Block(inputs : Vector[Block.InputDefinition],
                 outputs : Vector[Block.OutputDefinition],
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
      case TAT.Scatter(id, expr, body, _) =>
        val collection = TUtil.exprToString(expr)
        s"scatter (${id} in ${collection})"
      case TAT.Conditional(expr, body, _) =>
        val cond = TUtil.exprToString(expr)
        s"if (${cond})"
      case call : TAT.Call =>
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
  case class RequiredInputDefinition(name: String, wdlType: WdlTypes.T)
      extends InputDefinition

  // An input that has a default and may be skipped by the caller
  case class OverridableInputDefinitionWithDefault(name: String,
                                                   wdlType: WdlTypes.T,
                                                   defaultExpr: TAT.Expr)
      extends InputDefinition

  // an input that may be omitted by the caller. In that case the value will
  // be null (or None).
  case class OptionalInputDefinition(name: String, wdlType: WdlTypes.T_Optional)
      extends InputDefinition

  case class OutputDefinition(name: String, wdlType: WdlTypes.T, expr: Option[TAT.Expr])

  def translate(i : TAT.InputDefinition) : InputDefinition = {
    i match {
      case TAT.RequiredInputDefinition(name, wdlType, _) =>
        RequiredInputDefinition(name, wdlType)
      case TAT.OverridableInputDefinitionWithDefault(name, wdlType, expr, _) =>
        OverridableInputDefinitionWithDefault(name, wdlType, expr)
      case TAT.OptionalInputDefinition(name, wdlType, _) =>
        OptionalInputDefinition(name, wdlType)
    }
  }

  def translate(o : TAT.OutputDefinition) : OutputDefinition = {
    OutputDefinition(o.name, o.wdlType, Some(o.expr))
  }

  def isOptional(inputDef : InputDefinition) : Boolean = {
    inputDef match {
      case _ : RequiredInputDefinition => false
      case _ : OverridableInputDefinitionWithDefault => true
      case _ : OptionalInputDefinition => true
    }
  }

  // Deep search for all calls in a graph
  def deepFindCalls(nodes: Vector[TAT.WorkflowElement]): Vector[TAT.Call] = {
    nodes
      .foldLeft(Vector.empty[TAT.Call]) {
        case (accu, call : TAT.Call) =>
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

  def deepFindCalls(node: TAT.WorkflowElement): Vector[TAT.Call] = {
    deepFindCalls(Vector(node))
  }

  // Check that this block is valid.
  def validate(b : Block): Unit = {
    // The only calls allowed are in the last node
    val allButLast = b.nodes.dropRight(1)
    val allCalls = deepFindCalls(allButLast)
    assert(allCalls.size == 0)
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
  private def exprInputs(expr : TAT.Expr) : Vector[InputDefinition] = ???

  private def callInputs(call : TAT.Call) : Vector[InputDefinition] = ???

  private def makeOptional(t : WdlTypes.T) : WdlTypes.T = {
    t match {
      case _ : WdlTypes.T_Optional => t
      case _ => WdlTypes.T_Optional(t)
    }
  }

  private def makeArray(t : WdlTypes.T) : WdlTypes.T = {
    WdlTypes.T_Array(t, false)
  }

  // create a block from a group of statements
  //
  // requires calculating all the inputs and outputs
  private def makeBlock(elements : Vector[TAT.WorkflowElement]) : Block = {
    // accumulate the inputs and outputs.
    // start with an empty list of inputs and outputs.
    val (inputs, outputs) = elements.foldLeft((Vector.empty[InputDefinition],
                                               Vector.empty[OutputDefinition])) {
      case ((inputs, outputs), elem) =>
        elem match {
          case decl : TAT.Declaration =>
            val declInputs = decl.expr match {
              case None => Vector.empty
              case Some(expr) => exprInputs(expr)
            }
            (inputs ++ declInputs,
             outputs :+ OutputDefinition(decl.name, decl.wdlType, decl.expr))

          case call : TAT.Call =>
            (inputs ++ callInputs(call),
             outputs :+ OutputDefinition(call.actualName, call.wdlType, None))

          case cond : TAT.Conditional =>
            // recurse into body of conditional
            val subBlock = makeBlock(cond.body)
            // add an optional to all the the input types
            val subBlockInputs = subBlock.inputs.map{
              case RequiredInputDefinition(name, wdlType) =>
                RequiredInputDefinition(name, makeOptional(wdlType))
              case OverridableInputDefinitionWithDefault(name, wdlType, defaultExpr) =>
                OverridableInputDefinitionWithDefault(name, makeOptional(wdlType), defaultExpr)
              case OptionalInputDefinition(name, wdlType) =>
                OptionalInputDefinition(name, wdlType)
            }
            // make outputs optional
            val subBlockOutputs = subBlock.outputs.map{
              case OutputDefinition(name, wdlType, expr) =>
                OutputDefinition(name, makeOptional(wdlType), expr)
            }
            (inputs ++ subBlockInputs ++ exprInputs(cond.expr),
             outputs ++ subBlockOutputs)

          case sct : TAT.Scatter =>
            // recurse into body of the scatter
            val subBlock = makeBlock(sct.body)
            // add an array to all the the input types
            val subBlockInputs = subBlock.inputs.map{
              case RequiredInputDefinition(name, wdlType) =>
                RequiredInputDefinition(name, makeArray(wdlType))
              case OverridableInputDefinitionWithDefault(name, wdlType, defaultExpr) =>
                OverridableInputDefinitionWithDefault(name, makeArray(wdlType), defaultExpr)
              case OptionalInputDefinition(name, wdlType) =>
                // not sure about this conversion
                RequiredInputDefinition(name, makeArray(wdlType))
            }
            // make outputs arrays
            val subBlockOutputs = subBlock.outputs.map{
              case OutputDefinition(name, wdlType, expr) =>
                OutputDefinition(name, makeArray(wdlType), expr)
            }
            (inputs ++ subBlockInputs ++ exprInputs(sct.expr),
             outputs ++ subBlockOutputs)
        }
    }
    Block(inputs, outputs, elements)
  }

  // split a sequence of statements into blocks
  //
  private def splitToBlocks(elements : Vector[TAT.WorkflowElement]) : Vector[Block] = {
    // add to last part
    def addToLastPart(parts : Vector[Vector[TAT.WorkflowElement]],
                  elem : TAT.WorkflowElement) : Vector[Vector[TAT.WorkflowElement]] = {
      val allButLast = parts.dropRight(1)
      val last = parts.last :+ elem
      allButLast :+ last
    }
    // add to last part and start a new one.
    def startFresh(parts : Vector[Vector[TAT.WorkflowElement]],
                   elem : TAT.WorkflowElement) : Vector[Vector[TAT.WorkflowElement]] = {
      val parts2 = addToLastPart(parts, elem)
      parts2 :+ Vector.empty[TAT.WorkflowElement]
    }

    // split into sub-sequences (parts). Each part is a vector of workflow elements.
    // We start with a single empty part, which is an empty vector. This ensures that
    // down the line there is at least one part.
    val parts = elements.foldLeft(Vector(Vector.empty[TAT.WorkflowElement])) {
      case (parts, decl : TAT.Declaration) =>
        addToLastPart(parts, decl)
      case (parts, call : TAT.Call) =>
        startFresh(parts, call)
      case (parts, cond : TAT.Conditional) if deepFindCalls(cond).size == 0 =>
        addToLastPart(parts, cond)
      case (parts, cond : TAT.Conditional) =>
        startFresh(parts, cond)
      case (parts, sct : TAT.Scatter) if deepFindCalls(sct).size == 0 =>
        addToLastPart(parts, sct)
      case (parts, sct : TAT.Scatter) =>
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
  def outputClosure(outputs : Vector[OutputDefinition]) : Map[String, WdlTypes.T] = ???

  // figure out all the outputs from a block of statements
  //
  def allOutputs(elements : Vector[TAT.WorkflowElement]) : Map[String, WdlTypes.T] = {
    val b = makeBlock(elements)
    b.outputs.map{
      case OutputDefinition(name, wdlType, _) =>
        name -> wdlType
    }.toMap
  }

  // split a part of a workflow
  def split(statements : Vector[TAT.WorkflowElement]): (Vector[InputDefinition],
                                                        Vector[Block],
                                                        Vector[OutputDefinition]) = {
    val top : Block = makeBlock(statements)
    val subBlocks = splitToBlocks(statements)
    (top.inputs, subBlocks, top.outputs)
  }

  // Split an entire workflow into blocks.
  //
  def splitWorkflow(wf : TAT.Workflow): Vector[Block] = {
    splitToBlocks(wf.body)
  }

  // Does this output require evaluation? If so, we will need to create
  // another applet for this.
  def isSimpleOutput(outputNode: OutputDefinition): Boolean = {
    outputNode.expr match {
      case None => true   // What happens here?
      case Some(expr) => WomValueAnalysis.isTrivialExpression(expr)
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
  private def isSimpleCall(nodes: Vector[TAT.WorkflowElement], trivialExpressionsOnly: Boolean): Boolean = {
    assert (nodes.size > 0)
    if (nodes.size >= 2)
      return false
    // there is example a single node
    val node = nodes.head
    node match {
      case call : TAT.Call if trivialExpressionsOnly =>
        call.inputs.values.forall{
          case expr => WomValueAnalysis.isTrivialExpression(expr)
        }
      case call : TAT.Call =>
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
  case class CondOneCall(nodes: Vector[TAT.WorkflowElement],
                         value: TAT.Conditional,
                         call: TAT.Call)
      extends Category
  case class CondFullBlock(nodes: Vector[TAT.WorkflowElement], value: TAT.Conditional)
      extends Category
  case class ScatterOneCall(nodes: Vector[TAT.WorkflowElement],
                            value: TAT.Scatter,
                            call: TAT.Call)
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

  def getSubBlock(path: Vector[Int], nodes : Vector[TAT.WorkflowElement]): Block = {
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
