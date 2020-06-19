package dxWDL.util

import java.io.File
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import dxWDL.base.ParseWdlSourceFile

class WdlPrettyPrintApproxTest extends AnyFlatSpec with Matchers {

  private def tryToPrintFile(path: File): Unit = {
    val src = scala.io.Source.fromFile(path)
    val wfSourceCode =
      try {
        src.mkString
      } finally {
        src.close()
      }
    try {
      val (wf, _, _, _) = ParseWdlSourceFile(false).parseWdlWorkflow(wfSourceCode)
      val blocks = Block.splitWorkflow(wf)

      WdlPrettyPrintApprox.graphInputs(wf.inputs)
      WdlPrettyPrintApprox.graphOutputs(wf.outputs)
      blocks.foreach { b =>
        WdlPrettyPrintApprox.block(b)
      }
    } catch {
      // Some of the source files are invalid, or contain tasks and not workflows
      case _: Throwable => ()
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
