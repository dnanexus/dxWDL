package dx.core.languages.wdl

import java.nio.file.{Path, Paths}

import dx.core.languages.wdl.{Bundle => WdlBundle}
import dx.core.util.SysUtils
import dx.util.Logger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}

class BlockTest extends AnyFlatSpec with Matchers {
  private val logger = Logger.Quiet
  private val parseWdlSourceFile = ParseSource(logger)

  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  def mapFromOutputs(outputs: Vector[Block.OutputDefinition]): Map[String, WdlTypes.T] = {
    outputs.map {
      case Block.OutputDefinition(name, wdlType, _) => name -> wdlType
    }.toMap
  }

  it should "calculate closure correctly" in {
    val path = pathFromBasename("util", "block_closure.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)

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
    val path = pathFromBasename("util", "block_closure.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)

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
    val path = pathFromBasename("compiler", "wf_linear.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)

    mapFromOutputs(blocks(1).outputs) should be(
        Map("z" -> WdlTypes.T_Int, "mul.result" -> WdlTypes.T_Int)
    )
    blocks(1).inputs.map(_.name).toSet should be(Set("add.result"))
  }

  it should "handle block zero" in {
    val path = pathFromBasename("util", "block_zero.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)

    mapFromOutputs(blocks(0).outputs) should be(
        Map("rain" -> WdlTypes.T_Int, "inc.result" -> WdlTypes.T_Optional(WdlTypes.T_Int))
    )
  }

  it should "block with two calls or more" in {
    val path = pathFromBasename("util", "block_with_three_calls.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)
    blocks.size should be(1)
  }

  it should "split a block with an expression after a call" in {
    val path = pathFromBasename("util", "expression_after_call.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)
    blocks.size should be(2)
  }

  it should "calculate closure correctly for WDL draft-2" in {
    val path = pathFromBasename("draft2", "block_closure.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)

    blocks(1).inputs.map(_.name).toSet should be(Set("flag", "rain"))
    blocks(2).inputs.map(_.name).toSet should be(Set("flag", "inc1.result"))
    blocks(3).inputs.map(_.name).toSet should be(Set("rain"))
    blocks(4).inputs.map(_.name).toSet should be(Set("rain", "inc1.result", "flag"))
  }

  it should "calculate closure correctly for WDL draft-2 II" in {
    val path = pathFromBasename("draft2", "shapes.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)

    blocks(0).inputs.map(_.name).toSet should be(Set("num"))
    blocks(1).inputs.map(_.name).toSet should be(Set.empty)
  }

  it should "calculate closure for a workflow with expression outputs" in {
    val path = pathFromBasename("compiler", "wf_with_output_expressions.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val wfOutputs = wf.outputs.map(Block.translate)
    Block.outputClosure(wfOutputs).keys.toSet should be(Set("a", "b"))
  }

  it should "calculate output closure for a workflow" in {
    val path = pathFromBasename("compiler", "cast.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val wfOutputs = wf.outputs.map(Block.translate)
    Block.outputClosure(wfOutputs).keys.toSet should be(
        Set("Add.result", "SumArray.result", "SumArray2.result", "JoinMisc.result")
    )
  }

  it should "identify simple calls even if they have optionals" in {
    val path = pathFromBasename("util", "missing_inputs_to_direct_call.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)

    for (i <- 0 to 2) {
      Block.categorize(blocks(i)) shouldBe a[Block.CallDirect]
    }
  }

  it should "categorize correctly calls to subworkflows" in {
    val path = pathFromBasename("subworkflows", "trains.wdl")
    val (_, wdlBundle, _, _) = parseWdlSourceFile.apply(path, List.empty)
    val wf = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("Could not find the workflow in the source")
    }

    val blocks = Block.splitWorkflow(wf)

    Block.categorize(blocks(0)) shouldBe a[Block.ScatterOneCall]
  }

  ignore should "get subblocks" in {
    val path = pathFromBasename("nested", "two_levels.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)

    val b0 = Block.getSubBlock(Vector(0), wf.body)
    Block.categorize(b0) shouldBe a[Block.ScatterFullBlock]

    val b1 = Block.getSubBlock(Vector(1), wf.body)
    Block.categorize(b1) shouldBe a[Block.CondOneCall]

    val b2 = Block.getSubBlock(Vector(2), wf.body)
    Block.categorize(b2) shouldBe a[Block.CallDirect]

    val b00 = Block.getSubBlock(Vector(0, 0), wf.body)
    Block.categorize(b00) shouldBe a[Block.CallDirect]

    val b01 = Block.getSubBlock(Vector(0, 1), wf.body)
    Block.categorize(b01) shouldBe a[Block.CallDirect]

    val b02 = Block.getSubBlock(Vector(0, 2), wf.body)
    Block.categorize(b02) shouldBe a[Block.CallFragment]
  }

  it should "handle calls to imported modules II" in {
    val path = pathFromBasename("draft2", "block_category.wdl")
    val (_, wdlBundle: WdlBundle, _, _) =
      parseWdlSourceFile.apply(path, List.empty)

    val wf = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("sanity")
    }
    val blocks = Block.splitWorkflow(wf)
    Block.categorize(blocks(0)) shouldBe a[Block.CondOneCall]
  }

  it should "handle calls to imported modules" in {
    val path = pathFromBasename("draft2", "conditionals1.wdl")
    val (_, wdlBundle: WdlBundle, _, _) =
      parseWdlSourceFile.apply(path, List.empty)

    val wf = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("sanity")
    }
    val blocks = Block.splitWorkflow(wf)

    for (i <- blocks.indices) {
      val b = blocks(i)
      /*            System.out.println(s"""|BLOCK #${i} = [
                                   |${b.prettyPrintApproxWdl}
                                   |]
                                   |""".stripMargin)*/
      val catg = Block.categorize(b)
      logger.ignore(catg)
    }
  }

  it should "compile a workflow calling a subworkflow as a direct call" in {
    val path = pathFromBasename("draft2", "movies.wdl")
    val (_, wdlBundle: WdlBundle, _, _) =
      parseWdlSourceFile.apply(path, List.empty)

    val wf = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("sanity")
    }

    // Find the fragment block to execute
    val block = Block.getSubBlock(Vector(0), wf.body)

    /*
        val dbgBlock = block.nodes.map{
            WdlPrettyPrintApproxWdl.apply(_)
        }.mkString("\n")
        System.out.println(s"""|Block:
                               |${dbgBlock}
                               |""".stripMargin)
     */

    Block.categorize(block) shouldBe a[Block.CallDirect]
  }

  it should "sort a block correctly in the presence of conditionals" taggedAs EdgeTest in {
    val path = pathFromBasename("draft2", "conditionals3.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)

    val blocks = Block.splitWorkflow(wf)

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
    val path = pathFromBasename("draft2", "conditionals_base.wdl")
    val (_, wdlBundle, _, _) = parseWdlSourceFile.apply(path, List.empty)

    val wf = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("sanity")
    }

    val scatters: Seq[TAT.Scatter] = wf.body.collect {
      case n: TAT.Scatter => n
    }
    scatters.size should be(1)
  }

  it should "sort a subblock properly" in {
    val path = pathFromBasename("draft2", "conditionals4.wdl")
    val (_, wdlBundle, _, _) = parseWdlSourceFile.apply(path, List.empty)

    val wf = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("sanity")
    }

    // Find the fragment block to execute
    val b = Block.getSubBlock(Vector(1), wf.body)
    /*        System.out.println(s"""|BLOCK #1 = [
                               |${b.prettyPrintApproxWdl}
                               |]
 |""".stripMargin) */
    logger.ignore(b)
  }

  it should "handle an empty workflow" in {
    val path = pathFromBasename("util", "empty_workflow.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val blocks = Block.splitWorkflow(wf)
    blocks.size shouldBe 0
  }

  it should "detect when inputs are used as outputs" in {
    val path = pathFromBasename("util", "inputs_used_as_outputs.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val wfInputs = wf.inputs.map(Block.translate)
    val wfOutputs = wf.outputs.map(Block.translate)
    Block.inputsUsedAsOutputs(wfInputs, wfOutputs) shouldBe Set("lane")
  }

  it should "create correct inputs for a workflow with an unpassed argument" in {
    val path = pathFromBasename("bugs", "unpassed_argument_propagation.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)
    val names = wf.inputs.map(_.name).toSet
    names shouldBe Set.empty[String]
  }

  it should "figure out when a block has no calls" taggedAs EdgeTest in {
    val path = pathFromBasename("block", "b1.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (wf, _, _, _) = parseWdlSourceFile.parseWdlWorkflow(wfSourceCode)

    val b0 = Block.getSubBlock(Vector(0), wf.body)
    Block.categorize(b0) shouldBe a[Block.ScatterFullBlock]

    val b00 = Block.getSubBlock(Vector(0, 0), wf.body)

//    val bl33 = WdlPrettyPrintApproxWdl.apply(b00.nodes)
//    System.out.println(bl33)

    Block.categorize(b00) shouldBe a[Block.CondOneCall]
  }
}
