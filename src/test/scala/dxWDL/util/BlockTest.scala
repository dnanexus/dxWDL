package dxWDL.util

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}

import wom.callable.{WorkflowDefinition}
import wom.executable.WomBundle
import wom.graph._
import wom.graph.expression._
import wom.types._

import dxWDL.base.Utils

class BlockTest extends FlatSpec with Matchers {
  private val parseWomSourceFile = ParseWomSourceFile(false)

  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  private def closureAll(b: Block): Map[String, WomType] = {
    val cls = Block.closure(b)
    cls.map { case (name, (womType, _)) => name -> womType }.toMap
  }

  it should "calculate closure correctly" in {
    val path = pathFromBasename("util", "block_closure.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

    /*System.out.println(s"""|block #0 =
                               |${subBlocks(0).prettyPrintApproxWdl}}
                               |""".stripMargin)*/
    closureAll(subBlocks(0)).keys.toSet should be(Set.empty)
    closureAll(subBlocks(1)).keys.toSet should be(Set("flag", "rain"))
    closureAll(subBlocks(2)).keys.toSet should be(Set("flag", "inc1.result"))
    closureAll(subBlocks(3)).keys.toSet should be(Set("rain"))
    closureAll(subBlocks(4)).keys.toSet should be(
      Set("rain", "inc1.result", "flag")
    )
  }

  it should "calculate outputs correctly" in {
    val path = pathFromBasename("util", "block_closure.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

    Block.outputs(subBlocks(1)) should be(
      Map("inc2.result" -> WomOptionalType(WomIntegerType))
    )
    Block.outputs(subBlocks(2)) should be(
      Map("inc3.result" -> WomOptionalType(WomIntegerType))
    )
    Block.outputs(subBlocks(3)) should be(
      Map("inc4.result" -> WomArrayType(WomIntegerType))
    )
    Block.outputs(subBlocks(4)) should be(
      Map(
        "x" -> WomArrayType(WomIntegerType),
        "inc5.result" -> WomArrayType(WomOptionalType(WomIntegerType))
      )
    )
  }

  it should "calculate outputs correctly II" in {
    val path = pathFromBasename("compiler", "wf_linear.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

    Block.outputs(subBlocks(1)) should be(
      Map("z" -> WomIntegerType, "mul.result" -> WomIntegerType)
    )
    closureAll(subBlocks(1)).keys.toSet should be(Set("add.result"))
  }

  it should "handle block zero" in {
    val path = pathFromBasename("util", "block_zero.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (inNodes, _, subBlocks, outNodes) =
      Block.split(wf.innerGraph, wfSourceCode)

    Block.outputs(subBlocks(0)) should be(
      Map(
        "rain" -> WomIntegerType,
        "inc.result" -> WomOptionalType(WomIntegerType)
      )
    )
  }

  it should "block with two calls or more" in {
    val path = pathFromBasename("util", "block_with_three_calls.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
    subBlocks.size should be(1)
  }

  it should "split a block with an expression after a call" in {
    val path = pathFromBasename("util", "expression_after_call.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
    subBlocks.size should be(2)
  }

  it should "calculate closure correctly for WDL draft-2" in {
    val path = pathFromBasename("draft2", "block_closure.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

    closureAll(subBlocks(1)).keys.toSet should be(Set("flag", "rain"))
    closureAll(subBlocks(2)).keys.toSet should be(Set("flag", "inc1.result"))
    closureAll(subBlocks(3)).keys.toSet should be(Set("rain"))
    closureAll(subBlocks(4)).keys.toSet should be(
      Set("rain", "inc1.result", "flag")
    )
  }

  it should "calculate closure correctly for WDL draft-2 II" in {
    val path = pathFromBasename("draft2", "shapes.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (inputNodes, _, subBlocks, outputNodes) =
      Block.split(wf.innerGraph, wfSourceCode)

    closureAll(subBlocks(0)).keys.toSet should be(Set("num"))
    closureAll(subBlocks(1)).keys.toSet should be(Set.empty)
  }

  it should "calculate closure for a workflow with expression outputs" in {
    val path = pathFromBasename("compiler", "wf_with_output_expressions.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)

    val outputNodes = wf.innerGraph.outputNodes
    val exprOutputNodes: Vector[ExpressionBasedGraphOutputNode] =
      outputNodes.flatMap { node =>
        if (Block.isSimpleOutput(node)) None
        else Some(node.asInstanceOf[ExpressionBasedGraphOutputNode])
      }.toVector
    Block.outputClosure(exprOutputNodes) should be(Set("a", "b"))
  }

  it should "calculate output closure for a workflow" in {
    val path = pathFromBasename("compiler", "cast.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val outputNodes = wf.innerGraph.outputNodes.toVector
    Block.outputClosure(outputNodes) should be(
      Set(
        "Add.result",
        "SumArray.result",
        "SumArray2.result",
        "JoinMisc.result"
      )
    )
  }

  it should "identify simple calls even if they have optionals" in {
    val path = pathFromBasename("util", "missing_inputs_to_direct_call.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (inputNodes, _, subBlocks, outputNodes) =
      Block.split(wf.innerGraph, wfSourceCode)

    for (i <- 0 to 2) {
      Block.categorize(subBlocks(i)) shouldBe a[Block.CallDirect]
    }
  }

  it should "categorize correctly calls to subworkflows" taggedAs (EdgeTest) in {
    val path = pathFromBasename("subworkflows", "trains.wdl")
    val (_, womBundle, sources, _) = parseWomSourceFile.apply(path, List.empty)
    val (_, wfSourceCode) = sources.find {
      case (key, wdlCode) =>
        key.endsWith("trains.wdl")
    }.get

    val wf: WorkflowDefinition = womBundle.primaryCallable match {
      case Some(wf: WorkflowDefinition) => wf
      case _                            => throw new Exception("Could not find the workflow in the source")
    }

    val (inputNodes, _, subBlocks, outputNodes) =
      Block.split(wf.innerGraph, wfSourceCode)

    Block.categorize(subBlocks(0)) shouldBe a[Block.ScatterOneCall]
  }

  it should "get subblocks" in {
    val path = pathFromBasename("nested", "two_levels.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (_, womBundle, sources, _) = parseWomSourceFile.apply(path, List.empty)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)

    // sort from low to high according to the source lines.
    val callsLoToHi =
      parseWomSourceFile.scanForCalls(wf.innerGraph, wfSourceCode)

    val graph = wf.innerGraph

    val b0 = Block.getSubBlock(Vector(0), graph, callsLoToHi)
    Block.categorize(b0) shouldBe a[Block.ScatterFullBlock]

    val b1 = Block.getSubBlock(Vector(1), graph, callsLoToHi)
    Block.categorize(b1) shouldBe a[Block.CondOneCall]

    val b2 = Block.getSubBlock(Vector(2), graph, callsLoToHi)
    Block.categorize(b2) shouldBe a[Block.CallDirect]

    val b00 = Block.getSubBlock(Vector(0, 0), graph, callsLoToHi)
    Block.categorize(b00) shouldBe a[Block.CallDirect]

    val b01 = Block.getSubBlock(Vector(0, 1), graph, callsLoToHi)
    Block.categorize(b01) shouldBe a[Block.CallDirect]

    val b02 = Block.getSubBlock(Vector(0, 2), graph, callsLoToHi)
    Block.categorize(b02) shouldBe a[Block.CallFragment]
  }

  it should "handle calls to imported modules II" in {
    val path = pathFromBasename("draft2", "block_category.wdl")
    val (language, womBundle: WomBundle, allSources, _) =
      parseWomSourceFile.apply(path, List.empty)

    val (_, wfSource) = allSources.find {
      case (name, _) => name.endsWith("block_category.wdl")
    }.get

    val wf: WorkflowDefinition = womBundle.primaryCallable match {
      case Some(wf: WorkflowDefinition) => wf
      case _                            => throw new Exception("sanity")
    }
    val graph = wf.innerGraph
    val (inputNodes, _, subBlocks, outputNodes) = Block.split(graph, wfSource)

    Block.categorize(subBlocks(0)) shouldBe a[Block.CondOneCall]
  }

  it should "handle calls to imported modules" in {
    val path = pathFromBasename("draft2", "conditionals1.wdl")
    val (language, womBundle: WomBundle, allSources, _) =
      parseWomSourceFile.apply(path, List.empty)

    val (_, wfSource) = allSources.find {
      case (name, _) => name.endsWith("conditionals1.wdl")
    }.get

    val wf: WorkflowDefinition = womBundle.primaryCallable match {
      case Some(wf: WorkflowDefinition) => wf
      case _                            => throw new Exception("sanity")
    }
    val graph = wf.innerGraph
    val (inputNodes, _, subBlocks, outputNodes) = Block.split(graph, wfSource)

    for (i <- 0 to (subBlocks.length - 1)) {
      val b = subBlocks(i)
      /*            System.out.println(s"""|BLOCK #${i} = [
                                   |${b.prettyPrintApproxWdl}
                                   |]
                                   |""".stripMargin)*/
      val catg = Block.categorize(b)
      Utils.ignore(catg)
    }
  }

  it should "compile a workflow calling a subworkflow as a direct call" in {
    val path = pathFromBasename("draft2", "movies.wdl")
    val (language, womBundle: WomBundle, allSources, _) =
      parseWomSourceFile.apply(path, List.empty)

    val (_, wfSource) = allSources.find {
      case (name, _) => name.endsWith("movies.wdl")
    }.get

    val wf: WorkflowDefinition = womBundle.primaryCallable match {
      case Some(wf: WorkflowDefinition) => wf
      case _                            => throw new Exception("sanity")
    }

    // sort from low to high according to the source lines.
    val callsLoToHi = parseWomSourceFile.scanForCalls(wf.innerGraph, wfSource)

    // Find the fragment block to execute
    val block = Block.getSubBlock(Vector(0), wf.innerGraph, callsLoToHi)

    /*
        val dbgBlock = block.nodes.map{
            WomPrettyPrintApproxWdl.apply(_)
        }.mkString("\n")
        System.out.println(s"""|Block:
                               |${dbgBlock}
                               |""".stripMargin)
     */

    Block.categorize(block) shouldBe a[Block.CallDirect]
  }

  it should "sort a block correctly in the presence of conditionals" in {
    val path = pathFromBasename("draft2", "conditionals3.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)

    val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
    val b0 = subBlocks(0)

    val exprVec: Vector[ExposedExpressionNode] = b0.nodes.collect {
      case node: ExposedExpressionNode => node
    }
    exprVec.size should be(1)
    val arrayCalc: ExposedExpressionNode = exprVec.head
    arrayCalc.womExpression.sourceString should be("[i1, i2, i3]")
  }

  it should "find the correct number of scatters" in {
    val path = pathFromBasename("draft2", "conditionals_base.wdl")
    val (_, womBundle: WomBundle, allSources, _) =
      parseWomSourceFile.apply(path, List.empty)

    val (_, wfSource) = allSources.find {
      case (name, _) => name.endsWith("conditionals_base.wdl")
    }.get

    val wf: WorkflowDefinition = womBundle.primaryCallable match {
      case Some(wf: WorkflowDefinition) => wf
      case _                            => throw new Exception("sanity")
    }
    val nodes = wf.innerGraph.allNodes
    val scatters: Set[ScatterNode] = nodes.collect {
      case n: ScatterNode => n
    }
    scatters.size should be(1)
  }

  it should "sort a subblock properly" in {
    val path = pathFromBasename("draft2", "conditionals4.wdl")
    val (language, womBundle: WomBundle, allSources, _) =
      parseWomSourceFile.apply(path, List.empty)
    val (_, wfSource) = allSources.find {
      case (name, _) => name.endsWith("conditionals4.wdl")
    }.get

    val wf: WorkflowDefinition = womBundle.primaryCallable match {
      case Some(wf: WorkflowDefinition) => wf
      case _                            => throw new Exception("sanity")
    }

    // sort from low to high according to the source lines.
    val callsLoToHi = parseWomSourceFile.scanForCalls(wf.innerGraph, wfSource)

    // Find the fragment block to execute
    val b = Block.getSubBlock(Vector(1), wf.innerGraph, callsLoToHi)
    /*        System.out.println(s"""|BLOCK #1 = [
                               |${b.prettyPrintApproxWdl}
                               |]
 |""".stripMargin) */
    Utils.ignore(b)
  }

  it should "handle an empty workflow" in {
    val path = pathFromBasename("util", "empty_workflow.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
    subBlocks.size shouldBe (0)
  }

  it should "detect when inputs are used as outputs" in {
    val path = pathFromBasename("util", "inputs_used_as_outputs.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (inputs, _, _, outputs) = Block.split(wf.innerGraph, wfSourceCode)
    Block.inputsUsedAsOutputs(inputs, outputs) shouldBe (Set("lane"))
  }

  it should "create correct inputs for a workflow with an unpassed argument" in {
    val path = pathFromBasename("bugs", "unpassed_argument_propagation.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (inputs, _, _, _) = Block.split(wf.innerGraph, wfSourceCode)

    val names = inputs.map { inDef =>
      inDef.identifier.localName.value
    }.toSet
    names shouldBe (Set.empty[String])
  }

  it should "account for arguments that have a default, but are not optional" in {
    val path = pathFromBasename("bugs", "unpassed_argument_propagation.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf: WorkflowDefinition, _, _) =
      parseWomSourceFile.parseWdlWorkflow(wfSourceCode)
    val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

    val c0All = Block.closure(subBlocks(0))
    c0All.keys.toSet should be(Set("unpassed_arg_default"))

    val c0Optionals = c0All.collect { case (name, (_, true)) => name }.toSet
    c0Optionals should be(Set("unpassed_arg_default"))

    closureAll(subBlocks(1)).keys.toSet should be(Set.empty)
  }
}
