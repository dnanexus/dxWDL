package dxWDL.util

import java.io.File
import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}

class WomPrettyPrintApproxWdlTest extends FlatSpec with Matchers {

    private def tryToPrintFile(path : File) : Unit = {
        val wfSourceCode = scala.io.Source.fromFile(path).mkString
        try {
            val (wf : WorkflowDefinition, _, typeAliases) = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
            val (_, _, blocks, outputs) = Block.split(wf.graph, wfSourceCode)

            WomPrettyPrintApproxWdl.graphInputs(wf.inputs.toSeq)
            WomPrettyPrintApproxWdl.graphOutputs(outputs)
            blocks.foreach{ b =>
                WomPrettyPrintApproxWdl.block(b)
            }
        } catch {
            // Some of the source files are invalid, or contain tasks and not workflows
            case e : Throwable => ()
        }
    }

    it should "print original WDL for draft2" in {
        val testDir = new File("src/test/resources/draft2")
        val testCases = testDir.listFiles.filter{ x =>
            x.isFile && x.getName.endsWith(".wdl")
        }.toVector

        for (path <- testCases) {
            tryToPrintFile(path)
        }
    }


    it should "print original WDL for version 1.0" in {
        val testDir = new File("src/test/resources/compiler")
        val testCases = testDir.listFiles.filter{ x =>
            x.isFile && x.getName.endsWith(".wdl")
        }.toVector

        for (path <- testCases) {
            tryToPrintFile(path)
        }
    }
}
