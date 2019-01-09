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
*/

package dxWDL.util

import wom.expression.WomExpression
import wom.graph._
import wom.graph.expression._

// A sorted group of graph nodes, that match some original
// set of WDL statements.
case class Block(nodes : Vector[GraphNode]) {
    def prettyPrint : String = {
        val desc = nodes.map{ node =>
            "    " + WomPrettyPrint.apply(node) + "\n"
        }.mkString("")
        s"""|Block [
            |${desc}
            |]""".stripMargin
    }
}

object Block {
    // A trivial expression has no operators, it is either a constant WomValue
    // or a single identifier. For example: '5' and 'x' are trivial. 'x + y'
    // is not.
    def isTrivialExpression(expr: WomExpression) : Boolean = {
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
    def isCallWithNoSubexpressions(node: GraphNode) : Boolean = {
        node match {
            case call : CallNode =>
                call.inputDefinitionMappings.forall{
                    case (_, expr: WomExpression) => isTrivialExpression(expr)
                    case (_, _) => true
                }
            case _ => false
        }
    }

    // Deep search for all calls in a graph
    def deepFindCalls(nodes: Seq[GraphNode]) : Vector[CallNode] = {
        nodes.foldLeft(Vector.empty[CallNode]) {
            case (accu: Vector[CallNode], call:CallNode) =>
                accu :+ call
            case (accu, ssc:ScatterNode) =>
                accu ++ deepFindCalls(ssc.innerGraph.nodes.toVector)
            case (accu, ifStmt:ConditionalNode) =>
                accu ++ deepFindCalls(ifStmt.innerGraph.nodes.toVector)
            case (accu, _) =>
                accu
        }.toVector
    }

    // Count how many calls (task or workflow) there are in a series
    // of nodes.
    private def deepCountCalls(nodes: Seq[GraphNode]) : Int = {
        val retval = deepFindCalls(nodes.toVector).size
        //System.out.println(s"deepCountCalls ${nodes.size} nodes = ${retval}")
        retval
    }

    // Here, "top of the graph" is the node that has no dependencies on the rest of
    // the nodes. It -could- depend on nodes in the 'topGroup' set.
    private def pickTopNodes(nodes: Set[GraphNode], topGroup: Set[GraphNode]) : Set[GraphNode] = {
        assert(nodes.size > 0)
        val tops = nodes.flatMap{ node =>
            val ancestors = node.upstreamAncestry
            val others = nodes - node
            if ((ancestors.intersect(others)).isEmpty) {
                Some(node)
            } else {
                None
            }
        }
        assert(tops.size > 0)
        tops.toSet
    }

    // Build a top group that has nodes upstream of the rest. Stop
    // once it has one or more calls.
    //
    // Note: the nodes are returned in sorted order, from upstream to downstream.
    private def buildTopGroup(nodes: Set[GraphNode]) : Vector[GraphNode] = {
        //System.err.println(s"buildTopGroup ${nodes.size} nodes")
        var topGroup = Vector.empty[GraphNode]
        var remaining = nodes
        while (remaining.size > 0 &&
                   deepCountCalls(topGroup) == 0) {
            val topNodes = pickTopNodes(remaining, topGroup.toSet)
            remaining --= topNodes
            topGroup ++= topNodes.toVector
        }
        topGroup
    }


    // Check that all the call input expressions are placed
    // in the same block as the call.
    //
    // For example, in a WDL source like this:
    //
    // workflow wf_linear {
    //   input {
    //     Int x = 3
    //     Int y = 5
    //   }
    //   call add {input: a=x, b=y}
    //   call mul { input: a = add.result, b = 5 }
    //   output {
    //     Int r1 = add.result
    //     Int r2 = mul.result
    //   }
    // }
    //
    // For the call to mul, the b=5 clause will have the node:
    //    TaskCallInputExpressionNode(b, 5, WomIntegerType)
    // It can come at the very first block. We want it to be placed in
    // the same block as the mul call.
    private def closeGroup(nodes: Vector[GraphNode],
                           rest: Set[GraphNode]) : Vector[GraphNode] = {
        val callImmediateExpressions = rest.flatMap{
            case call: CallNode => call.upstream.toVector
            case _ => Vector.empty
        }
        val outsideTcInputs: Set[TaskCallInputExpressionNode] = callImmediateExpressions.collect{
            case tc : TaskCallInputExpressionNode => tc
        }.toSet

        // Remove any of these expressions from the group
        nodes.filter{
            case iTc : TaskCallInputExpressionNode if outsideTcInputs contains iTc  => false
            case _ => true
        }
    }

    // Sort the graph into a linear set of blocks, according to dependencies.
    // Each block is itself sorted.
    def splitIntoBlocks(graph: Graph) : (Vector[GraphInputNode],   // inputs
                                         Vector[Block], // blocks
                                         Vector[GraphOutputNode]) // outputs
    = {
        //System.out.println(s"SplitIntoBlocks ${nodes.size} nodes")
        assert(graph.nodes.size > 0)
        var rest = graph.nodes
        var blocks = Vector.empty[Block]

        // The first block has the graph inputs
        val inputBlock = graph.inputNodes.toVector
        rest --= inputBlock.toSet

        // The last block has the graph outputs
        val outputBlock = graph.outputNodes.toVector
        rest --= outputBlock.toSet

        // Create a separate block for each group of statements
        // that include one call or more.
        while (rest.size > 0) {
            val topGroup = buildTopGroup(rest)
            val closedTopGroup = closeGroup(topGroup, rest -- topGroup)
            blocks :+= Block(closedTopGroup)
            rest = rest -- closedTopGroup
        }

        (inputBlock, blocks, outputBlock)
    }

    def dbgPrint(inputNodes: Vector[GraphInputNode],   // inputs
                 subBlocks: Vector[Block], // blocks
                 outputNodes: Vector[GraphOutputNode]) // outputs
            : Unit = {
        System.out.println("Inputs [")
        inputNodes.foreach{ node =>
            val desc = WomPrettyPrint.apply(node)
            System.out.println(s"  ${desc}")
        }
        System.out.println("]")
        subBlocks.foreach{ block =>
            System.out.println("Block [")
            block.nodes.foreach{ node =>
                val desc = WomPrettyPrint.apply(node)
                System.out.println(s"  ${desc}")
            }
            System.out.println("]")
        }
        System.out.println("Output [")
        outputNodes.foreach{ node =>
            val desc = WomPrettyPrint.apply(node)
            System.out.println(s"  ${desc}")
        }
        System.out.println("]")
    }

    // A block of nodes that represents a call with no subexpressions. These
    // can be compiled directly into a dx:workflow stage.
    //
    // For example, the WDL code:
    // call add { input: a=x, b=y }
    //
    // Is represented by the graph:
    // Block [
    //   TaskCallInputExpressionNode(a, x, WomIntegerType, GraphNodeOutputPort(a))
    //   TaskCallInputExpressionNode(b, y, WomIntegerType, GraphNodeOutputPort(b))
    //   CommandCall(add, Set(a, b))
    // ]
    def isSimpleCall(block: Block) : Option[CallNode] = {
        // find the call
        val calls : Seq[CallNode] = block.nodes.collect{
            case cNode : CallNode => cNode
        }
        if (calls.size != 1)
            return None
        val oneCall = calls.head

        // All the other nodes have to the call inputs
        val rest = block.nodes.toSet - oneCall
        val callInputs = oneCall.upstream.toSet
        if (rest != callInputs)
            return None

        // The call inputs have to be simple expressions
        val allSimple = rest.forall{
            case expr: TaskCallInputExpressionNode => isTrivialExpression(expr.womExpression)
            case _ => false
        }

        if (!allSimple)
            return None
        Some(oneCall)
    }

}
