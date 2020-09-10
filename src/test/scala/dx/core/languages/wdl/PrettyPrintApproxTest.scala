package dx.core.languages.wdl

import java.io.File

import dx.api.DxApi
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.util.Logger

class PrettyPrintApproxTest extends AnyFlatSpec with Matchers {
  private val dxApi = DxApi(Logger.Quiet)

  private def tryToPrintFile(path: File): Unit = {
    val src = scala.io.Source.fromFile(path)
    val wfSourceCode =
      try {
        src.mkString
      } finally {
        src.close()
      }
    try {
      val (wf, _, _, _) = ParseSource(dxApi).parseWdlWorkflow(wfSourceCode)
      val blocks = WdlBlock.splitWorkflow(wf)

      PrettyPrintApprox.graphInputs(wf.inputs)
      PrettyPrintApprox.graphOutputs(wf.outputs)
      blocks.foreach { b =>
        PrettyPrintApprox.block(b)
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
