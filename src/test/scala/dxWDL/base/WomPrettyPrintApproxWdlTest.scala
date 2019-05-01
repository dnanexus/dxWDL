package dxWDL.base

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}

class WomPrettyPrintApproxWdlTest extends FlatSpec with Matchers {
    private def pathFromBasename(basename: String) : Path = {
        val p = getClass.getResource(s"/util/${basename}").getPath
        Paths.get(p)
    }


    // Ignore a value. This is useful for avoiding warnings/errors
    // on unused variables.
    def ignore[A](value: A) : Unit = {}

    it should "print original WDL from block_closure.wdl" in {
        val path = pathFromBasename("block_closure.wdl")
        val wfSourceCode = scala.io.Source.fromFile(path.toFile).mkString
        val (wf : WorkflowDefinition, _, typeAliases) = dxWDL.util.ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val s = wf.innerGraph.nodes.map{
            WomPrettyPrintApproxWdl.apply(_)
        }.mkString("\n")
        ignore(s)
    }
}
