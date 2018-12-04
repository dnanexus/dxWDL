/*
     Split a block of statements into sub-blocks, each of which contains:
     1) at most one call
     2) at most one toplevel if/scatter block
     Note that the if/scatter block could be deeply nested. Previous compiler passes
     must make sure that there is no more a single call inside an if/scatter block.

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

     [call x, Int a, Int b]
     [call y]
     [call z]
     [String buf, Float x, scatter]
*/

package dxWDL.compiler

import dxWDL.util.{Verbose, Utils}
import wom.expression.WomExpression
import wom.graph.{GraphNode, CallNode, ScatterNode, ConditionalNode}
import wom.graph.GraphNode.GraphNodeWithInnerGraph

// Find all the calls inside a statement block
case class Block(statements: Vector[GraphNode]) {
    def head : GraphNode =
        statements.head

    def size : Int =
        statements.size

    def append(stmt: GraphNode) : Block =
        Block(statements :+ stmt)

    def append(block2: Block) : Block =
        Block(statements ++ block2.statements)

    // Check if one of the statements is a scatter/if block
    def hasSubBlock : Boolean = {
        statements.foldLeft(false) {
            case (accu, _:ScatterNode) => true
            case (accu, _:ConditionalNode) => true
            case (accu, _) => accu
        }
    }

    def findCalls : Vector[CallNode] = {
        def collectCalls(statements1: Seq[GraphNode]) : Vector[CallNode] = {
            statements1.foldLeft(Vector.empty[CallNode]) {
                case (accu, call:CallNode) =>
                    accu :+ call
                case (accu, ssc:ScatterNode) =>
                    accu ++ collectCalls(ssc.innerGraph.nodes.toSeq)
                case (accu, ifStmt:ConditionalNode) =>
                    accu ++ collectCalls(ifStmt.innerGraph.nodes.toSeq)
                  case (accu, _) =>
                    accu
            }.toVector
        }
        collectCalls(this.statements)
    }

    // Count how many calls (task or workflow) there are in a series
    // of statements.
    def countCalls : Int =
        findCalls.length

    // A trivial expression has no operators, it is either a constant WomValue
    // or a single identifier. For example: '5' and 'x' are trivial. 'x + y'
    // is not.
    private def isTrivialExpression(expr: WomExpression) : Boolean = {
        val inputs = expr.inputs
        if (inputs.size > 1)
            return false
        if (Utils.isExpressionConst(expr))
            return true
        // The expression may have one input, but could still have an operator.
        // For example: x+1, x + x.
        expr.sourceString == inputs.head
    }

    // The block is a singleton with one statement which is a call. The call
    // has no subexpressions. Note that the call may not provide
    // all the callee's arguments.
    def isCallWithNoSubexpressions : Boolean = {
        if (statements.size != 1)
            return false
        if (!statements.head.isInstanceOf[CallNode])
            return false
        val call = statements.head.asInstanceOf[CallNode]
        call.inputDefinitionMappings.forall{
            case (_, expr: WomExpression) =>
                isTrivialExpression(expr)
            case (_, _) => true
        }
    }
}

object Block {
    // construct a Block from a single statement
    def apply(node: GraphNode) : Block = {
        Block(Vector(node))
    }

    // Is this a subblock? Declarations aren't subblocks, scatters and if's are.
    private def isSubBlock(node :GraphNode) : Boolean = {
        node match {
            case _ : GraphNodeWithInnerGraph => true
            case _ => false
        }
    }

    private def getInnerNodes(node :GraphNode) : Vector[GraphNode] = {
        node match {
            case iNode : GraphNodeWithInnerGraph => iNode.innerGraph.nodes.toVector
            case _ => throw new Exception(s"Does not have an inner graph ${node}")
        }
    }

    // Split a workflow into blocks, where each block will compile to
    // a stage. When the workflow is unlocked, we create a separate
    // block for each toplevel call that has no subexpressions. These
    // will have their own stages.
    def splitIntoBlocks(children: Vector[GraphNode]) : Vector[Block] = {
        // base cases: zero and one children
        if (children.isEmpty)
            return Vector.empty
        if (children.length == 1)
            return Vector(Block(children))

        // Normal case, recurse into the first N-1 statements,
        // than append the Nth.
        val blocks = splitIntoBlocks(children.dropRight(1))
        val allBlocksButLast = blocks.dropRight(1)

        // see if the last statement should be incorporated into the last block,
        // of if we should open a new block.
        val lastBlock = blocks.last
        val lastStatement = children.last
        val lastChild = Block(Vector(lastStatement))
        var openNewBlock =
            (lastBlock.countCalls, lastChild.countCalls) match {
                case (0, 1) if lastChild.isCallWithNoSubexpressions =>
                    // create a separate block for the last child; it will get its
                    // own stage. We are compiling a WDL call directly into a stage
                    // here.
                    true
                case (0, (0|1)) =>
                    // no calls were seen so far, extend the block
                    false
                case (1, (0|1)) =>
                    // The last block already contains a call, start a new block
                    true
                case (x ,y) =>
                    throw new Exception(s"Internal error: block has ${x} calls, and child has ${y} calls")
            }
        if (isSubBlock(lastStatement)) {
            // If the last statement is an if/scatter, always start a new block
            openNewBlock = true
        }
        val trailing: Vector[Block] =
            if (openNewBlock)
                Vector(lastBlock, lastChild)
            else
                Vector(lastBlock.append(lastChild))
        allBlocksButLast ++ trailing
    }


    // Is there a declaration after a call? For example:
    //
    // scatter (i in numbers) {
    //     call add { input: a=i, b=1 }
    //     Int n = add.result
    // }
    private def isDeclarationAfterCall(statements: Vector[GraphNode]) : Boolean = {
        statements.length match {
            case 0 => false
            case 1 =>
                if (isSubBlock(statements.head)) {
                    // recurse into the only child
                    val nodes = getInnerNodes(statements.head)
                    isDeclarationAfterCall(nodes.toVector)
                } else {
                    // base case: there is just one one child,
                    // and it is a call or a declaration.
                    false
                }
            case _ =>
                val numCalls = Block(statements.head).countCalls
                if (numCalls == 1) true
                else isDeclarationAfterCall(statements.tail)
        }
    }

    // A block is reducible if one of these conditions hold.
    // (1) it has two calls or more.
    // (2) It has one call, but there may be dependent
    // declarations after it. For example:
    //
    // scatter (i in numbers) {
    //     call add { input: a=i, b=1 }
    //     Int n = add.result
    // }
    private def isReducible(statement: GraphNode) : Boolean = {
        statement match {
            case iNode : GraphNodeWithInnerGraph =>
                val children = iNode.innerGraph.nodes.toVector
                val numCalls = Block(children).countCalls
                if (numCalls == 0)
                    return false
                if (numCalls >= 2)
                    return true
                isDeclarationAfterCall(children)
            case _ =>
                false
        }
    }



    // In a tree like:
    //    if ()
    //      if ()
    //        scatter ()
    //           Int i
    //           call A
    //           call B
    //
    // The scatter node is the end of the trunk.
    def findTrunkTop(statement: GraphNode) : GraphNode = {
        statement match {
            case iNode : GraphNodeWithInnerGraph =>
                val children = iNode.innerGraph.nodes.toVector
                children.size match {
                    case 0 => throw new Exception("findTrunkTop on an empty tree")
                    case 1 =>  findTrunkTop(children.head)
                    case _ => statement
                }
            case _ => throw new Exception(s"""|findTrunkTop on node without an inner graph
                                              |${statement}
                                              |""")
        }
    }

    // There is a separable workflow in here, either
    // a fragment with one call, or a call-line with multiple calls.
    //
    // call line example:
    // if (cond) {
    //   call A
    //   call B
    //   call C
    // }
    //
    // a fragment example:
    // if (cond)
    //    if (cond2)
    //       scatter(x in xs)
    //         call A
    //
    def findReducibleChild(statements: Seq[GraphNode],
                           verbose: Verbose) : Option[GraphNode] = {
        val child = statements.find{
            case stmt => isReducible(stmt)
        }
        child match {
            case None =>
                // There is nothing to reduce here. This can happen for a sequence like:
                // workflow w
                //    call C
                //    call D
                //    String s = C.result
                None
            case Some(stmt) =>
                Some(findTrunkTop(stmt))
        }
    }
}
