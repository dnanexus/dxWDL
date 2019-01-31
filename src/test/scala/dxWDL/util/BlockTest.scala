package dxWDL.util

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}

class BlockTest extends FlatSpec with Matchers {
    private def pathFromBasename(basename: String) : Path = {
        val p = getClass.getResource(s"/util/${basename}").getPath
        Paths.get(p)
    }

    it should "calculate closure correctly" in {
        val path = pathFromBasename("block_closure.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (_, subBlocks, _) = Block.splitIntoBlocks(wf.innerGraph, wfSourceCode)

        Block.closure(subBlocks(1)) should be(Set("flag", "rain"))
        Block.closure(subBlocks(2)) should be(Set("flag", "inc1.result"))
        Block.closure(subBlocks(3)) should be(Set("rain"))
        Block.closure(subBlocks(4)) should be(Set("rain", "inc1.result", "flag"))
    }
}
