package dxWDL.util

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}
import org.scalatest.tags.Slow

@Slow
class WomPrettyPrintApproxWdlTest extends FlatSpec with Matchers {
    private def pathFromBasename(basename: String) : Path = {
        val p = getClass.getResource(s"/util/${basename}").getPath
        Paths.get(p)
    }

    it should "print original WDL from block_closure.wdl" in {
        val path = pathFromBasename("block_closure.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val s = wf.innerGraph.nodes.map{
            WomPrettyPrintApproxWdl.apply(_)
        }.mkString("\n")
        System.out.println(s)
    }
}
