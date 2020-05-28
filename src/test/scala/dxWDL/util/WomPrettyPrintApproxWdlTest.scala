package dxWDL.util

import java.io.File
import org.scalatest.{FlatSpec, Matchers}

class WomPrettyPrintApproxWdlTest extends FlatSpec with Matchers {

  private def tryToPrintFile(path: File): Unit = {
    val wfSourceCode = scala.io.Source.fromFile(path).mkString
    try {
      val (wf, _, blocks, _) = ParseWomSourceFile(false).parseWdlWorkflow(wfSourceCode)

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
