package dx.core.languages.wdl

import java.nio.file.{Path, Paths}

import dx.Tags.EdgeTest
import dx.core.ir.{Block, BlockKind}
import dx.core.languages.wdl.{WdlUtils => WdlUtils}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.Logger

class WdlBlockTest extends AnyFlatSpec with Matchers {
  private val logger = Logger.Quiet

  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  private def mapFromOutputs(outputs: Vector[TAT.OutputParameter]): Map[String, WdlTypes.T] = {
    outputs.map {
      case TAT.OutputParameter(name, wdlType, _, _) => name -> wdlType
    }.toMap
  }

  private def getDocument(dir: String, basename: String): TAT.Document = {
    val path = pathFromBasename(dir, basename)
    val (doc, _) = WdlUtils.parseAndCheckSourceFile(path)
    doc
  }
  private def getWorkflowBlocks(dir: String, basename: String): Vector[WdlBlock] = {
    WdlBlock.createBlocks(getDocument(dir, basename).workflow.get.body)
  }

  it should "calculate closure correctly" in {
    val blocks = getWorkflowBlocks("util", "block_closure.wdl")
    /*System.out.println(s"""|block #0 =
                               |${subBlocks(0).prettyPrintApproxWdl}}
                               |""".stripMargin)*/
    blocks(0).inputs.map(_.name).toSet should be(Set.empty)
    blocks(1).inputs.map(_.name).toSet should be(Set("flag", "rain"))
    blocks(2).inputs.map(_.name).toSet should be(Set("flag", "inc1.result"))
    blocks(3).inputs.map(_.name).toSet should be(Set("rain"))
    blocks(4).inputs.map(_.name).toSet should be(Set("rain", "inc1.result", "flag"))
  }

  it should "calculate outputs correctly" in {
    val blocks = getWorkflowBlocks("util", "block_closure.wdl")
    mapFromOutputs(blocks(1).outputs) should be(
        Map("inc2.result" -> WdlTypes.T_Optional(WdlTypes.T_Int))
    )
    mapFromOutputs(blocks(2).outputs) should be(
        Map("inc3.result" -> WdlTypes.T_Optional(WdlTypes.T_Int))
    )
    mapFromOutputs(blocks(3).outputs) should be(
        Map("inc4.result" -> WdlTypes.T_Array(WdlTypes.T_Int))
    )
    mapFromOutputs(blocks(4).outputs) should be(
        Map("x" -> WdlTypes.T_Array(WdlTypes.T_Int, nonEmpty = false),
            "inc5.result" -> WdlTypes.T_Array(WdlTypes.T_Optional(WdlTypes.T_Int),
                                              nonEmpty = false))
    )
  }

  it should "calculate outputs correctly II" in {
    val blocks = getWorkflowBlocks("compiler", "wf_linear.wdl")
    mapFromOutputs(blocks(1).outputs) should be(
        Map("z" -> WdlTypes.T_Int, "mul.result" -> WdlTypes.T_Int)
    )
    blocks(1).inputs.map(_.name).toSet should be(Set("add.result"))
  }

  it should "handle block zero" in {
    val blocks = getWorkflowBlocks("util", "block_zero.wdl")
    mapFromOutputs(blocks(0).outputs) should be(
        Map("rain" -> WdlTypes.T_Int, "inc.result" -> WdlTypes.T_Optional(WdlTypes.T_Int))
    )
  }

  it should "block with two calls or more" in {
    val blocks = getWorkflowBlocks("util", "block_with_three_calls.wdl")
    blocks.size should be(1)
  }

  it should "split a block with an expression after a call" in {
    val blocks = getWorkflowBlocks("util", "expression_after_call.wdl")
    blocks.size should be(2)
  }

  it should "calculate closure correctly for WDL draft-2" in {
    val blocks = getWorkflowBlocks("draft2", "block_closure.wdl")
    blocks(1).inputs.map(_.name).toSet should be(Set("flag", "rain"))
    blocks(2).inputs.map(_.name).toSet should be(Set("flag", "inc1.result"))
    blocks(3).inputs.map(_.name).toSet should be(Set("rain"))
    blocks(4).inputs.map(_.name).toSet should be(Set("rain", "inc1.result", "flag"))
  }

  it should "calculate closure correctly for WDL draft-2 II" in {
    val blocks = getWorkflowBlocks("draft2", "shapes.wdl")
    blocks(0).inputs.map(_.name).toSet should be(Set("num"))
    blocks(1).inputs.map(_.name).toSet should be(Set.empty)
  }

  it should "calculate closure for a workflow with expression outputs" in {
    val doc = getDocument("compiler", "wf_with_output_expressions.wdl")
    val outputClosure = WdlUtils.getOutputClosure(doc.workflow.get.outputs)
    outputClosure.keys.toSet should be(Set("a", "b"))
  }

  it should "calculate output closure for a workflow" in {
    val doc = getDocument("compiler", "cast.wdl")
    val outputClosure = WdlUtils.getOutputClosure(doc.workflow.get.outputs)
    outputClosure.keys.toSet should be(
        Set("Add.result", "SumArray.result", "SumArray2.result", "JoinMisc.result")
    )
  }

  it should "identify simple calls even if they have optionals" in {
    val blocks = getWorkflowBlocks("util", "missing_inputs_to_direct_call.wdl")
    blocks.foreach(_.kind shouldBe BlockKind.CallDirect)
  }

  it should "categorize correctly calls to subworkflows" in {
    val blocks = getWorkflowBlocks("subworkflows", "trains.wdl")
    blocks.head.kind shouldBe BlockKind.ScatterOneCall
  }

  ignore should "get subblocks" in {
    val blocks = getWorkflowBlocks("nested", "two_levels.wdl")

    val b0 = Block.getSubBlockAt(blocks, Vector(0))
    b0.kind shouldBe BlockKind.ScatterComplex

    val b1 = Block.getSubBlockAt(blocks, Vector(1))
    b1.kind shouldBe BlockKind.ConditionalOneCall

    val b2 = Block.getSubBlockAt(blocks, Vector(2))
    b2.kind shouldBe BlockKind.CallDirect

    val b00 = Block.getSubBlockAt(blocks, Vector(0, 0))
    b00.kind shouldBe BlockKind.CallDirect

    val b01 = Block.getSubBlockAt(blocks, Vector(0, 1))
    b01.kind shouldBe BlockKind.CallDirect

    val b02 = Block.getSubBlockAt(blocks, Vector(0, 2))
    b02.kind shouldBe BlockKind.CallFragment
  }

  it should "handle calls to imported modules II" in {
    val blocks = getWorkflowBlocks("draft2", "block_category.wdl")
    blocks.head.kind shouldBe BlockKind.ConditionalOneCall
  }

  it should "handle calls to imported modules" in {
    logger.ignore(getWorkflowBlocks("draft2", "conditionals1.wdl"))
  }

  it should "compile a workflow calling a subworkflow as a direct call" in {
    val blocks = getWorkflowBlocks("draft2", "movies.wdl")
    // Find the fragment block to execute
    val block = Block.getSubBlockAt(blocks, Vector(0))

    /*
        val dbgBlock = block.nodes.map{
            WdlPrettyPrintApproxWdl.apply(_)
        }.mkString("\n")
        System.out.println(s"""|Block:
                               |${dbgBlock}
                               |""".stripMargin)
     */

    block.kind shouldBe BlockKind.CallDirect
  }

  it should "sort a block correctly in the presence of conditionals" taggedAs EdgeTest in {
    val blocks = getWorkflowBlocks("draft2", "conditionals3.wdl")
    mapFromOutputs(blocks(0).outputs) should be(
        Map(
            "i1" -> WdlTypes.T_Optional(WdlTypes.T_Int),
            "i2" -> WdlTypes.T_Optional(WdlTypes.T_Int),
            "i3" -> WdlTypes.T_Optional(WdlTypes.T_Int),
            "powers10" -> WdlTypes.T_Array(WdlTypes.T_Optional(WdlTypes.T_Int), nonEmpty = false)
        )
    )
  }

  it should "find the correct number of scatters" in {
    val blocks = getWorkflowBlocks("draft2", "conditionals_base.wdl")
    val scatters = blocks.filter { block =>
      Set(BlockKind.ScatterOneCall, BlockKind.ScatterComplex).contains(block.kind)
    }
    scatters.size should be(1)
  }

  it should "sort a subblock properly" in {
    val blocks = getWorkflowBlocks("draft2", "conditionals4.wdl")
    // Find the fragment block to execute
    val b = Block.getSubBlockAt(blocks, Vector(1))
    /*        System.out.println(s"""|BLOCK #1 = [
                               |${b.prettyPrintApproxWdl}
                               |]
 |""".stripMargin) */
    logger.ignore(b)
  }

  it should "handle an empty workflow" in {
    val blocks = getWorkflowBlocks("util", "empty_workflow.wdl")
    blocks.size shouldBe 0
  }

  it should "detect when inputs are used as outputs" in {
    val doc = getDocument("util", "inputs_used_as_outputs.wdl")
    val wf = doc.workflow.get
    val inputs = wf.inputs.map(WdlBlockInput.translate)
    val inputsUsedAsOutputs =
      inputs.map(_.name).toSet.intersect(WdlUtils.getOutputClosure(wf.outputs).keySet)
    inputsUsedAsOutputs shouldBe Set("lane")
  }

  // TODO: this seems like a useless test, or at least one that doesn't belong here
  it should "create correct inputs for a workflow with an unpassed argument" in {
    val doc = getDocument("bugs", "unpassed_argument_propagation.wdl")
    val names = doc.workflow.get.inputs.map(_.name).toSet
    names shouldBe Set.empty[String]
  }

  it should "figure out when a block has no calls" taggedAs EdgeTest in {
    val blocks = getWorkflowBlocks("block", "b1.wdl")

    val b0 = Block.getSubBlockAt(blocks, Vector(0))
    b0.kind shouldBe BlockKind.ScatterComplex

    val b00 = Block.getSubBlockAt(blocks, Vector(0, 0))

//    val bl33 = WdlPrettyPrintApproxWdl.apply(b00.nodes)
//    System.out.println(bl33)

    b00.kind shouldBe BlockKind.ConditionalOneCall
  }
}
