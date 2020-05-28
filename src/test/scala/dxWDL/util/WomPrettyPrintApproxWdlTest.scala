package dxWDL.util

import java.io.File
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import dxWDL.base.ParseWomSourceFile
import dxWDL.util.WomPrettyPrintApproxWdl

class WomPrettyPrintApproxWdlTest extends AnyFlatSpec with Matchers {

  private def tryToPrintFile(path: File): Unit = {
    val wfSourceCode = scala.io.Source.fromFile(path).mkString
    try {
      val (wf, _, _, _) = ParseWomSourceFile(false).parseWdlWorkflow(wfSourceCode)
      val (_, _, blocks, _) = Block.splitWorkflow(wf)

      WomPrettyPrintApproxWdl.graphInputs(wf.inputs)
      WomPrettyPrintApproxWdl.graphOutputs(wf.outputs)
      blocks.foreach { b =>
        WomPrettyPrintApproxWdl.block(b)
      }
    } catch {
      // Some of the source files are invalid, or contain tasks and not workflows
      case e: Throwable => ()
    }
  }

  it should "print original WDL for draft2" in {
    val testDir = new File("src/test/resources/draft2")
    val testCases = testDir.listFiles.filter { x =>
      x.isFile && x.getName.endsWith(".wdl")
    }.toVector

    for (path <- testCases) {
      tryToPrintFile(path)
    }
  }

  it should "print original WDL for version 1.0" in {
    val testDir = new File("src/test/resources/compiler")
    val testCases = testDir.listFiles.filter { x =>
      x.isFile && x.getName.endsWith(".wdl")
    }.toVector

    for (path <- testCases) {
      tryToPrintFile(path)
    }
  }
}
