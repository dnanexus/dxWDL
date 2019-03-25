package dxWDL.util

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}
import wom.executable.WomBundle
import wom.graph._
import wom.types._

class BlockTest extends FlatSpec with Matchers {
    private def pathFromBasename(dir: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dir}/${basename}").getPath
        Paths.get(p)
    }

    it should "calculate closure correctly" in {
        val path = pathFromBasename("util", "block_closure.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

        /*System.out.println(s"""|block #0 =
                               |${subBlocks(0).prettyPrintApproxWdl}}
                               |""".stripMargin)*/
        Block.closure(subBlocks(0)).keys.toSet should be(Set.empty)
        Block.closure(subBlocks(1)).keys.toSet should be(Set("flag", "rain"))
        Block.closure(subBlocks(2)).keys.toSet should be(Set("flag", "inc1.result"))
        Block.closure(subBlocks(3)).keys.toSet should be(Set("rain"))
        Block.closure(subBlocks(4)).keys.toSet should be(Set("rain", "inc1.result", "flag"))
    }

    it should "calculate outputs correctly" in {
        val path = pathFromBasename("util", "block_closure.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

        Block.outputs(subBlocks(1)) should be(Map("inc2.result" -> WomOptionalType(WomIntegerType)))
        Block.outputs(subBlocks(2)) should be(Map("inc3.result" -> WomOptionalType(WomIntegerType)))
        Block.outputs(subBlocks(3)) should be(Map("inc4.result" -> WomArrayType(WomIntegerType)))
        Block.outputs(subBlocks(4)) should be(
            Map("x" -> WomArrayType(WomIntegerType),
                "inc5.result" -> WomArrayType(WomOptionalType(WomIntegerType)))
        )
    }

    it should "calculate outputs correctly II" in {
        val path = pathFromBasename("compiler", "wf_linear.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

        Block.outputs(subBlocks(1)) should be(Map("z" -> WomIntegerType,
                                                  "mul.result" -> WomIntegerType))
        Block.closure(subBlocks(1)).keys.toSet should be(Set("add.result"))
    }

    it should "handle block zero" in {
        val path = pathFromBasename("util", "block_zero.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (inNodes, subBlocks, outNodes) = Block.split(wf.innerGraph, wfSourceCode)

        Block.outputs(subBlocks(0)) should be(
            Map("rain" -> WomIntegerType,
                "inc.result" -> WomOptionalType(WomIntegerType))
        )
    }

    it should "block with two calls or more" in {
        val path = pathFromBasename("util", "block_with_three_calls.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
        subBlocks.size should be(1)
    }

    it should "split a block with an expression after a call" in {
        val path = pathFromBasename("util", "expression_after_call.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
        subBlocks.size should be(2)
    }

    it should "calculate closure correctly for WDL draft-2" in {
        val path = pathFromBasename("draft2", "block_closure.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

        Block.closure(subBlocks(1)).keys.toSet should be(Set("flag", "rain"))
        Block.closure(subBlocks(2)).keys.toSet should be(Set("flag", "inc1.result"))
        Block.closure(subBlocks(3)).keys.toSet should be(Set("rain"))
        Block.closure(subBlocks(4)).keys.toSet should be(Set("rain", "inc1.result", "flag"))
    }

    it should "calculate closure correctly for WDL draft-2 II" in {
        val path = pathFromBasename("draft2", "shapes.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (inputNodes, subBlocks, outputNodes) = Block.split(wf.innerGraph, wfSourceCode)

        Block.closure(subBlocks(0)).keys.toSet should be(Set("num"))
        Block.closure(subBlocks(1)).keys.toSet should be(Set.empty)
    }

    it should "calculate closure for a workflow with expression outputs" in {
        val path = pathFromBasename("compiler", "wf_with_output_expressions.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)

        val outputNodes = wf.innerGraph.outputNodes
        val exprOutputNodes: Vector[ExpressionBasedGraphOutputNode] =
            outputNodes.flatMap{ node =>
                if (Block.isSimpleOutput(node)) None
                else Some(node.asInstanceOf[ExpressionBasedGraphOutputNode])
            }.toVector
        Block.outputClosure(exprOutputNodes) should be (Set("a", "b"))
    }

    it should "calculate output closure for a workflow" in {
        val path = pathFromBasename("compiler", "cast.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val outputNodes = wf.innerGraph.outputNodes.toVector
        Block.outputClosure(outputNodes) should be (Set("Add.result",
                                                        "SumArray.result",
                                                        "SumArray2.result",
                                                        "JoinMisc.result"))
    }

    it should "identify simple calls even if they have optionals" in {
        val path = pathFromBasename("util", "missing_inputs_to_direct_call.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (inputNodes, subBlocks, outputNodes) = Block.split(wf.innerGraph, wfSourceCode)

        for (i <- 0 to 2) {
            val (_, category) = Block.categorize(subBlocks(i))
            category shouldBe a [Block.CallDirect]
        }
    }

    it should "categorize correctly calls to subworkflows" in {
        val path = pathFromBasename("subworkflows", "trains.wdl")
        val (_, womBundle, sources, _) = ParseWomSourceFile.apply(path)
        val (_, wfSourceCode) = sources.find{ case (key, wdlCode) =>
            key.endsWith("trains.wdl")
        }.get

        val wf : WorkflowDefinition = womBundle.primaryCallable match {
            case Some(wf: WorkflowDefinition) => wf
            case _ => throw new Exception("Could not find the workflow in the source")
        }

        val (inputNodes, subBlocks, outputNodes) = Block.split(wf.innerGraph, wfSourceCode)

        val (_, category) = Block.categorize(subBlocks(0))
        category shouldBe a [Block.Scatter]
    }

    it should "get subblocks" in {
        val path = pathFromBasename("nested", "two_levels.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val (_, womBundle, sources, _) = ParseWomSourceFile.apply(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)

        // sort from low to high according to the source lines.
        val callToSrcLine = ParseWomSourceFile.scanForCalls(wfSourceCode)
        val callsLoToHi : Vector[(String, Int)] = callToSrcLine.toVector.sortBy(_._2)

        val graph = wf.innerGraph

        val b0 = Block.getSubBlock(Vector(0), graph, callsLoToHi)
        val (_, catg0) = Block.categorize(b0)
        catg0 shouldBe a[Block.ScatterSubblock]

        val b1 = Block.getSubBlock(Vector(1), graph, callsLoToHi)
        val (_, catg1) = Block.categorize(b1)
        catg1 shouldBe a[Block.Cond]

        val b2 = Block.getSubBlock(Vector(2), graph, callsLoToHi)
        val (_, catg2) = Block.categorize(b2)
        catg2 shouldBe a[Block.CallDirect]

        val b00 = Block.getSubBlock(Vector(0, 0), graph, callsLoToHi)
        val (_, catg00) = Block.categorize(b00)
        catg00 shouldBe a[Block.CallDirect]

        val b01 = Block.getSubBlock(Vector(0, 1), graph, callsLoToHi)
        val (_, catg01) = Block.categorize(b01)
        catg01 shouldBe a[Block.CallDirect]

        val b02 = Block.getSubBlock(Vector(0, 2), graph, callsLoToHi)
        val (_, catg02) = Block.categorize(b02)
        catg02 shouldBe a[Block.CallCompound]
    }

    it should "handle calls to imported modules" taggedAs(EdgeTag) in {
        val path = pathFromBasename("draft2", "conditionals1.wdl")
        val (language, womBundle: WomBundle, allSources, subBundles) = ParseWomSourceFile.apply(path)

        val (_, wfSource) = allSources.find {
            case (name, _) => name.endsWith("conditionals1.wdl")
        }.get

        val wf: WorkflowDefinition = womBundle.primaryCallable match {
            case Some(wf: WorkflowDefinition) => wf
            case _ => throw new Exception("sanity")
        }
        val graph = wf.innerGraph
        val (inputNodes, subBlocks, outputNodes) = Block.split(graph, wfSource)

        for (i <- 0 to (subBlocks.length - 1)) {
            val b = subBlocks(i)
/*            System.out.println(s"""|BLOCK #${i} = [
                                   |${b.prettyPrintApproxWdl}
                                   |]
                                   |""".stripMargin)*/
            val (_, catg) = Block.categorize(b)
            Utils.ignore(catg)
        }
    }
}
