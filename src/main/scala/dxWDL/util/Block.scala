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
import wom.types._

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

    // Check that this block is valid.
    // 1) It can have zero or one top-level calls
    def validate() : Unit = {
        val topLevelCalls: Vector[CallNode] = nodes.collect{
            case x:CallNode => x
        }.toVector
        val numTopLeveleCalls = topLevelCalls.size
        if (numTopLeveleCalls > 1) {
            Utils.error(this.prettyPrint)
            throw new Exception(s"${numTopLeveleCalls} calls in block")
        }
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

    // Is the call [callName] invoked in one of the nodes, or their
    // inner graphs?
    private def graphContainsCall(callName: String,
                                  nodes: Set[GraphNode]) : Boolean = {
        nodes.exists{
            case callNode: CallNode =>
                callNode.identifier.localName.value == callName
            case cNode: ConditionalNode =>
                graphContainsCall(callName, cNode.innerGraph.nodes)
            case scNode: ScatterNode =>
                graphContainsCall(callName, scNode.innerGraph.nodes)
            case _ => false
        }
    }

    // Find the toplevel graph node that contains this call
    private def findCallByName(callName: String,
                               nodes: Set[GraphNode]) : Option[GraphNode] = {
        nodes.find{
            case callNode: CallNode =>
                callNode.identifier.localName.value == callName
            case cNode: ConditionalNode =>
                graphContainsCall(callName, cNode.innerGraph.nodes)
            case scNode: ScatterNode =>
                graphContainsCall(callName, scNode.innerGraph.nodes)
            case _ => false
        }
    }

    private def pickTopNodes(nodes: Set[GraphNode]) = {
        assert(nodes.size > 0)
        nodes.flatMap{ node =>
            val ancestors = node.upstreamAncestry
            val others = nodes - node
            if ((ancestors.intersect(others)).isEmpty) {
                Some(node)
            } else {
                None
            }
        }
    }

    // Sort a group of nodes according to dependencies. Note that this is a partial
    // ordering only.
    private def partialSortByDep(nodes: Set[GraphNode]) : Vector[GraphNode] = {
        var ordered = Vector.empty[GraphNode]
        var rest : Set[GraphNode] = nodes

        while (!rest.isEmpty) {
            val tops = pickTopNodes(rest)
            assert(!tops.isEmpty)
            ordered = ordered ++ tops
            rest = rest -- tops
        }

        assert(ordered.size == nodes.size)
        ordered
    }

    private def deepAncestors(node: GraphNode) : Set[GraphNode] = {
        node match {
            case cndNode: ConditionalNode =>
                cndNode.upstreamAncestry ++
                cndNode.innerGraph.nodes.flatMap(deepAncestors(_))
            case sctNode: ScatterNode =>
                sctNode.upstreamAncestry ++
                sctNode.innerGraph.nodes.flatMap(deepAncestors(_))
            case _ =>
                node.upstreamAncestry
        }
    }

    // Sort the graph into a linear set of blocks, according to
    // dependencies.  Each block is itself sorted. The dependencies
    // impose a partial ordering on the graph. To make it correspond
    // to the original WDL, we attempt to maintain the original line
    // ordering.
    //
    // For example, in the workflow below:
    // workflow foo {
    //    call A
    //    call B
    //    call C
    // }
    // the block splitting algorithm could generate six permutions.
    // For example:
    //
    //    1           2
    //  [ call C ]  [ call A ]
    //  [ call A ]  [ call B ]
    //  [ call B ]  [ call C ]
    //
    // We choose option #2 because it resembles the original.
    def split(graph: Graph, wdlSourceCode: String) :
            (Vector[GraphInputNode],   // inputs
             Vector[Block], // blocks
             Vector[GraphOutputNode]) // outputs
    = {
        //System.out.println(s"SplitIntoBlocks ${nodes.size} nodes")
        assert(graph.nodes.size > 0)
        var rest : Set[GraphNode] = graph.nodes
        var blocks = Vector.empty[Block]
        val callToSrcLine = ParseWomSourceFile.scanForCalls(wdlSourceCode)

        // sort from low to high according to the source lines.
        val callsLoToHi : Vector[(String, Int)] = callToSrcLine.toVector.sortBy(_._2)

        // separate out the inputs
        val inputBlock = graph.inputNodes.toVector
        rest --= inputBlock.toSet

        // separate out the outputs
        val outputBlock = graph.outputNodes.toVector
        rest --= outputBlock.toSet

        // Create a separate block for each call. This maintains
        // the sort order from the origial code.
        //
        for ((callName, _) <- callsLoToHi) {
            assert(!rest.isEmpty)

            findCallByName(callName, rest) match {
                case None =>
                    // we already accounted for this call. Several
                    // calls can be in the same node. For example, a
                    // scatter can has many calls inside its subgraph.
                    ()
                case Some(node) =>
                    rest = rest - node

                    // Build a vector where the callNode comes LAST. Choose
                    // the nodes from the ones that have not been picked yet.
                    val ancestors = deepAncestors(node).intersect(rest)
                    val nodes: Vector[GraphNode] =
                        (partialSortByDep(ancestors) :+ node)
                            .filter{ x => !x.isInstanceOf[GraphInputNode] }
                    val crnt = Block(nodes)
                    blocks :+= crnt
                    rest = rest -- nodes.toSet
            }
        }

        val allBlocks =
            if (rest.size > 0) {
                // Add an additional block for anything not belonging to the calls
                blocks :+ Block(rest.toVector)
            } else {
                blocks
            }
        allBlocks.foreach{ b => b.validate() }
        (inputBlock, allBlocks, outputBlock)
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

        // All the other nodes have to be call inputs
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


    // Check if a block contains exactly one call
    // examples:
    //
    //  if (x > 1) {
    //      call Add { input: a=1, b=2 }
    //  }
    //
    // These are NOT simple blocks:
    // if (x > 1) {
    //    call Multiple { ... }
    //    call Add { ... }
    // }
    //
    // scatter (n in names) {
    //    String full_name = n + " Horowitz"
    //    call Filter { input: prefix = fullName }
    // }
    //
    def isSimpleSubblock(graph: Graph) : Boolean = {
        val nodes = graph.nodes

        // The block can't have conditional/scatter sub-blocks
        val hasSubblocks = nodes.forall{
            case _ : ScatterNode => true
            case _ : ConditionalNode => true
            case _ => false
        }
        if (hasSubblocks)
            return false

        // The block has to have one call
        val calls : Seq[CallNode] = nodes.toSeq.collect{
            case cNode : CallNode => cNode
        }
        if (calls.size > 1)
            return false
        assert(calls.size == 1)

        // The only other kind of nodes could be inputs, outputs, and task input expressions.
        nodes.forall{
            case _: CallNode => true
            case _: TaskCallInputExpressionNode => true
            case _: OuterGraphInputNode => true
            case _: GraphOutputNode => true
            case _ => false
        }
    }

    // These are the kinds of blocks that are run by the workflow-fragment-runner.
    //
    // A block can have expressions, input ports, and output ports in the beginning.
    // The last node can be:
    // 1) Expression
    // 2) Call
    // 3) Conditional block
    // 4) Scatter block
    sealed trait Category
    case object AllExpressions extends Category
    case class Call(call: CallNode) extends Category
    case class Cond(cond: ConditionalNode, simple: Boolean) extends Category
    case class Scatter(scatter: ScatterNode, simple: Boolean) extends Category

    def categorize(block: Block) : (Vector[GraphNode], Category) = {
        assert(!block.nodes.isEmpty)
        val allButLast : Vector[GraphNode] = block.nodes.dropRight(1)

        // make sure there are no calls in the beginning of the block
        val nrCalls = deepFindCalls(allButLast).size
        if (nrCalls > 0)
            throw new Exception(
                s"""|There are ${nrCalls} calls at the beginning of the block.
                    |However, there can be none""".stripMargin.replaceAll("\n", " "))

        val lastNode = block.nodes.last
        lastNode match {
            case callNode: CallNode =>
                (allButLast, Call(callNode))
            case conditionalNode: ConditionalNode =>
                (allButLast, Cond(conditionalNode, isSimpleSubblock(conditionalNode.innerGraph)))
            case sctNode: ScatterNode =>
                (allButLast, Scatter(sctNode, isSimpleSubblock(sctNode.innerGraph)))
            case other =>
                ((allButLast :+ other), AllExpressions)
        }
    }

    // Find all the inputs required for a block. For example,
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
    def closure(block: Block) : Set[String] = {
        // make a deep list of all the nodes inside the block
        val allBlockNodes : Set[GraphNode] = block.nodes.map{
            case sctNode: ScatterNode =>
                sctNode.innerGraph.allNodes + sctNode
            case cndNode: ConditionalNode =>
                cndNode.innerGraph.allNodes + cndNode
            case node : GraphNode =>
                Set(node)
        }.flatten.toSet

        // Examine an input port, and keep it only if it points outside the block.
        def keepOnlyOutsideRefs(inPort : GraphNodePort.InputPort) : Option[String] = {
            if (allBlockNodes contains inPort.upstream.graphNode) None
            else Some(inPort.name)
        }

        // Examine only the outer input nodes, check that they
        // originate in a node outside the block.
        def getInputsToGraph(graph: Graph) : Set[String] = {
            graph.nodes.flatMap {
                case ogin: OuterGraphInputNode =>
                    if (allBlockNodes contains ogin.linkToOuterGraphNode)
                        None
                    else
                        Some(ogin.identifier.localName.value)
                case _ => None
            }.toSet
        }

        val allInputs :Set[String] = block.nodes.flatMap{
            case scNode : ScatterNode =>
                val scNodeInputs = scNode.inputPorts.flatMap(keepOnlyOutsideRefs(_))
                scNodeInputs ++ getInputsToGraph(scNode.innerGraph)
            case cnNode : ConditionalNode =>
                val cnInputs = cnNode.conditionExpression.inputPorts.flatMap(keepOnlyOutsideRefs(_))
                cnInputs ++ getInputsToGraph(cnNode.innerGraph)
            case node : GraphNode =>
                node.inputPorts.flatMap(keepOnlyOutsideRefs(_))
        }.toSet
        allInputs
    }

    // Figure out all the outputs from a sequence of WDL statements.
    //
    // Note: The type outside a scatter/conditional block is *different* than the type in
    // the block.  For example, 'Int x' declared inside a scatter, is
    // 'Array[Int] x' outside the scatter.
    //
    def outputs(block: Block) : Map[String, WomType] = {
        val xtrnPorts : Vector[GraphNodePort.OutputPort] =
            block.nodes.map {
                case node : ExposedExpressionNode =>
                    node.outputPorts
                case _ : ExpressionNode =>
                    // an anonymous expression node, ignore it
                    Set.empty
                case node =>
                    node.outputPorts
            }.flatten

        xtrnPorts.map{ outputPort =>
            // Is this really the fully qualified name?
            val fqn = outputPort.identifier.localName.value
            val womType = outputPort match {
                case gnop : GraphNodePort.GraphNodeOutputPort => gnop.womType
                case ebop : GraphNodePort.ExpressionBasedOutputPort => ebop.womType
                case sctOp : GraphNodePort.ScatterGathererPort => sctOp.womType
                case cnop : GraphNodePort.ConditionalOutputPort => cnop.womType
                case other => throw new Exception(s"unhandled case ${other.getClass}")
            }
            fqn -> womType
        }.toMap
    }
}
