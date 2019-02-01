package dxWDL.util

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}
import wom.types._

import org.scalatest.Tag
object EdgeTest extends Tag("edge")

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

        Block.closure(subBlocks(1)) should be(Set("flag", "rain"))
        Block.closure(subBlocks(2)) should be(Set("flag", "inc1.result"))
        Block.closure(subBlocks(3)) should be(Set("rain"))
        Block.closure(subBlocks(4)) should be(Set("rain", "inc1.result", "flag"))
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
        Block.closure(subBlocks(1)) should be(Set("add.result"))
    }

    it should "handle block zero" taggedAs(EdgeTest) in {
        val path = pathFromBasename("util", "block_zero.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (inNodes, subBlocks, outNodes) = Block.split(wf.innerGraph, wfSourceCode)

        Block.dbgPrint(inNodes, subBlocks, outNodes)
        Block.outputs(subBlocks(0)) should be(Map("rain" -> WomOptionalType(WomIntegerType)))
    }

    it should "block with two calls or more" in {
        val path = pathFromBasename("util", "block_with_three_calls.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
        Utils.ignore(subBlocks)
    }
}
