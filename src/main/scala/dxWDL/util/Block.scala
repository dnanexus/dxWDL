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

    def prettyPrintApproxWdl: String = {
        nodes.map{
            WomPrettyPrintApproxWdl.apply(_)
        }.mkString("\n")
    }

    // Check that this block is valid.
    def validate() : Unit = {
        // There can be no calls in the first nodes
        val allButLast : Vector[GraphNode] = this.nodes.dropRight(1)
        val allCalls = Block.deepFindCalls(allButLast)
        assert(allCalls.size == 0)
    }

    // Create a human readable name for a block of statements
    //
    // 1. Ignore all declarations
    // 2. If there is a scatter/if, use that
    // 3. if there is at least one call, use the first one.
    //
    // If the entire block is made up of expressions, return None
    def makeName : Option[String] = {
        val coreStmts = nodes.filter{
            case _: ScatterNode => true
            case _: ConditionalNode => true
            case _: CallNode => true
            case _ => false
        }

        if (coreStmts.isEmpty)
            return None
        val name = coreStmts.head match {
            case ssc : ScatterNode =>
                // WDL allows scatter on one element only
                assert(ssc.scatterVariableNodes.size == 1)
                val svNode: ScatterVariableNode = ssc.scatterVariableNodes.head
                val collection = svNode.scatterExpressionNode.womExpression.sourceString
                val name = svNode.identifier.localName.value
                s"scatter (${name} in ${collection})"
            case cond : ConditionalNode =>
                s"if (${cond.conditionExpression.womExpression.sourceString})"
            case call : CallNode =>
                s"frag ${call.identifier.localName.value}"
            case _ =>
                throw new Exception("sanity")
        }
        return Some(name)
    }

}

object Block {
    // A trivial expression has no operators, it is either a constant WomValue
    // or a single identifier. For example: '5' and 'x' are trivial. 'x + y'
    // is not.
    def isTrivialExpression(womType: WomType,
                            expr: WomExpression) : Boolean = {
        val inputs = expr.inputs
        if (inputs.size > 1)
            return false
        if (WomValueAnalysis.isExpressionConst(womType, expr))
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
                    case (inputDef, expr: WomExpression) =>
                        isTrivialExpression(inputDef.womType, expr)
                    case (_, _) => true
                }
            case _ => false
        }
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
    def findCallByName(callName: String,
                       nodes: Set[GraphNode]) : Option[GraphNode] = {
        val gnode: Option[GraphNode] = nodes.find{
            case callNode: CallNode =>
                callNode.identifier.localName.value == callName
            case cNode: ConditionalNode =>
                graphContainsCall(callName, cNode.innerGraph.nodes)
            case scNode: ScatterNode =>
                graphContainsCall(callName, scNode.innerGraph.nodes)
            case _ => false
        }
/*        gnode match {
            case None =>
                System.out.println(s"findCallByName(${callName}) failed")
            case Some(x) =>
                System.out.println(s"""|findCallByName(${callName})
                                       |${WomPrettyPrintApproxWdl.apply(x)}
                                       |
                                       |""".stripMargin)
        }*/
        gnode
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
            case ogin: OuterGraphInputNode =>
                // follow the indirection, this is not by default
                val sourceNode = ogin.linkToOuterGraphNode
                sourceNode.upstreamAncestry + sourceNode
            case _ =>
                node.upstreamAncestry
        }
    }

    // Split an entire workflow.
    //
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
    def splitGraph(graph: Graph, callsLoToHi: Vector[(String, Int)]):
            (Vector[GraphInputNode], // inputs
             Vector[Block], // blocks
             Vector[GraphOutputNode]) // outputs
    = {
        assert(graph.nodes.size > 0)
        var rest : Set[GraphNode] = graph.nodes
        var blocks = Vector.empty[Block]

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
            findCallByName(callName, rest) match {
                case None =>
                    // we already accounted for this call. Several
                    // calls can be in the same node. For example, a
                    // scatter can have many calls inside its subgraph.
                    ()
                case Some(node) =>
                    assert(!rest.isEmpty)
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
                val lastBlock = partialSortByDep(rest)
                blocks :+ Block(lastBlock)
            } else {
                blocks
            }
        allBlocks.foreach{ b => b.validate() }
        (inputBlock, allBlocks, outputBlock)
    }


    // An easy to use method that takes the workflow source
    def split(graph: Graph, wfSource: String) :  (Vector[GraphInputNode],   // inputs
                                                  Vector[Block], // blocks
                                                  Vector[GraphOutputNode]) // outputs
    = {
        val callToSrcLine = ParseWomSourceFile.scanForCalls(wfSource)

        // sort from low to high according to the source lines.
        val callsLoToHi : Vector[(String, Int)] = callToSrcLine.toVector.sortBy(_._2)

        splitGraph(graph, callsLoToHi)
    }

    // A debugging method, to display a block in a human readable
    // fashion.
    def dbgToString(inputNodes: Vector[GraphInputNode],   // inputs
                    subBlocks: Vector[Block], // blocks
                    outputNodes: Vector[GraphOutputNode]) // outputs
            : String = {
        val inputs = inputNodes.map{ node =>
            WomPrettyPrintApproxWdl.apply(node)
        }.mkString("\n")
        val blocks = subBlocks.map{ block =>
            val body = block.nodes.map{ node =>
                WomPrettyPrintApproxWdl.apply(node)
            }.mkString("\n")
            s"""|Block [
                |${body}
                |]""".stripMargin
        }.mkString("\n")
        val outputs= outputNodes.map{ node =>
            WomPrettyPrintApproxWdl.apply(node)
        }.mkString("\n")

        s"""|Inputs ["
            |${inputs}
            |]
            |${blocks}
            |Output [
            |${outputs}
            |]""".stripMargin
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
    private def isSimpleCall(block: Block) : Boolean = {
        // find the call
        val calls : Seq[CallNode] = block.nodes.collect{
            case cNode : CallNode => cNode
        }
        if (calls.size != 1)
            return false
        val oneCall = calls.head

        // All the other nodes have to be call inputs
        //
        // Some of the inputs may be missing, which is why we
        // have the -subsetOf- call.
        val rest = block.nodes.toSet - oneCall
        val callInputs = oneCall.upstream.toSet
        if (!rest.subsetOf(callInputs))
            return false

        // All the call inputs have to be simple expressions, if the call is
        // to be called "simple"
        val retval = rest.forall{
            case expr: AnonymousExpressionNode =>
                isTrivialExpression(expr.womType, expr.womExpression)
            case other =>
                false
        }
        retval
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

    // These are the kinds of blocks that are run by the workflow-fragment-runner.
    //
    // A block can have expressions, input ports, and output ports in the beginning.
    // The last node can be:
    // 1) Expression
    // 2) Call
    // 3) Conditional block
    // 4) Scatter block
    sealed trait Category {
        val nodes: Vector[GraphNode]
    }
    case class AllExpressions(override val nodes: Vector[GraphNode]) extends Category
    case class CallDirect(override val nodes: Vector[GraphNode], value: CallNode) extends Category
    case class CallCompound(override val nodes: Vector[GraphNode], value: CallNode) extends Category
    case class Cond(override val nodes: Vector[GraphNode], value: ConditionalNode) extends Category
    case class Scatter(override val nodes: Vector[GraphNode], value: ScatterNode) extends Category

    object Category {
        def getInnerGraph(catg: Category) : Graph = {
            catg match {
                case AllExpressions(_) | CallDirect(_, _) | CallCompound(_, _) =>
                    throw new UnsupportedOperationException(
                        s"${catg.getClass.toString} does not implement getInnerGraph")
                case cond: Cond => cond.value.innerGraph
                case sct: Scatter => sct.value.innerGraph
            }
        }
        def toString(catg: Category) : String = {
            // convert Block$Cond to Cond
            val fullClassName = catg.getClass.toString
            val index = fullClassName.lastIndexOf('$')
            if (index == -1)
                fullClassName
            else
                fullClassName.substring(index + 1)
        }
    }

    def categorize(block: Block) : Category = {
        assert(!block.nodes.isEmpty)
        val allButLast : Vector[GraphNode] = block.nodes.dropRight(1)
        val lastNode = block.nodes.last
        lastNode match {
            case _ if deepFindCalls(Seq(lastNode)).isEmpty =>
                // The block comprises of expressions only
                AllExpressions(allButLast :+ lastNode)

            case callNode: CallNode if isSimpleCall(block) =>
                CallDirect(allButLast, callNode)

            case callNode: CallNode =>
                CallCompound(allButLast, callNode)

            case condNode: ConditionalNode =>
                Cond(allButLast, condNode)

            case sctNode: ScatterNode =>
                Scatter(allButLast, sctNode)
        }
    }

    def getSubBlock(path: Vector[Int],
                    graph: Graph,
                    callsLoToHi: Vector[(String, Int)]) : Block = {
        assert(path.size >= 1)

        val (_, blocks, _) = splitGraph(graph, callsLoToHi)
        var subBlock = blocks(path.head)
        for (i <- path.tail) {
            val catg = categorize(subBlock)
            val innerGraph = Category.getInnerGraph(catg)
            val (_, blocks2, _) = splitGraph(innerGraph, callsLoToHi)
            subBlock = blocks2(i)
        }
        return subBlock
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
    def closure(block: Block) : Map[String, WomType] = {
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
        def keepOnlyOutsideRefs(inPort : GraphNodePort.InputPort) : Option[(String, WomType)] = {
            if (allBlockNodes contains inPort.upstream.graphNode) None
            else Some((inPort.name, inPort.womType))
        }

        // Examine only the outer input nodes, check that they
        // originate in a node outside the block.
        def getInputsToGraph(graph: Graph) : Map[String, WomType] = {
            graph.nodes.flatMap {
                case ogin: OuterGraphInputNode =>
                    if (allBlockNodes contains ogin.linkToOuterGraphNode)
                        None
                    else
                        Some((ogin.identifier.localName.value, ogin.womType))
                case _ => None
            }.toMap
        }

        block.nodes.flatMap{
            case scNode : ScatterNode =>
                val scNodeInputs = scNode.inputPorts.flatMap(keepOnlyOutsideRefs(_))
                scNodeInputs ++ getInputsToGraph(scNode.innerGraph)
            case cnNode : ConditionalNode =>
                val cnInputs = cnNode.conditionExpression.inputPorts.flatMap(keepOnlyOutsideRefs(_))
                cnInputs ++ getInputsToGraph(cnNode.innerGraph)
            case node : GraphNode =>
                node.inputPorts.flatMap(keepOnlyOutsideRefs(_))
        }.toMap
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
            val womType = outputPort.womType
            fqn -> womType
        }.toMap
    }

    // Does this output require evaluation? If so, we will need to create
    // another applet for this.
    def isSimpleOutput(outputNode: GraphOutputNode) : Boolean = {
        outputNode match {
            case PortBasedGraphOutputNode(id, womType, sourcePort) =>
                true
            case expr :ExpressionBasedGraphOutputNode if (Block.isTrivialExpression(expr.womType, expr.womExpression)) =>
                true
            case expr :ExpressionBasedGraphOutputNode =>
                // An expression that requires evaluation
                false
            case other =>
                throw new Exception(s"unhandled output class ${other}")
        }
    }

    // Figure out what variables from the environment we need to pass
    // into the applet. In other words, the closure.
    def outputClosure(outputNodes : Vector[GraphOutputNode]) : Set[String] = {
        outputNodes.foldLeft(Set.empty[String]) {
            case (accu, PortBasedGraphOutputNode(id, womType, sourcePort)) =>
                accu + sourcePort.name
            case (accu, expr : ExpressionBasedGraphOutputNode) =>
                accu ++ expr.inputPorts.map(_.name)
            case other =>
                throw new Exception(s"unhandled output ${other}")

        }
    }
}
