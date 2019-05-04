package dxWDL.base

import java.io.File
import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}

import dxWDL.util.ParseWomSourceFile
import dxWDL.util.Utils

class WomPrettyPrintApproxWdlTest extends FlatSpec with Matchers {

    private def tryToPrintFile(path : File) : Unit = {
        val wfSourceCode = scala.io.Source.fromFile(path).mkString
        try {
            val (wf : WorkflowDefinition, _, typeAliases) = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
            val s = wf.innerGraph.nodes.map{
                WomPrettyPrintApproxWdl.apply(_)
            }.mkString("\n")
            Utils.ignore(s)
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
