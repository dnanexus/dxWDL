package dxWDL.util

import java.io.File
import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}

class WomPrettyPrintApproxWdlTest extends FlatSpec with Matchers {

    private def normalize(s: String) : String = {
        s.replaceAll("[\t ]+", " ")
    }

    val expected = s"""|
                       |scatter(bam in ["1_ACGT_1.bam", "2_TCCT_1.bam"]) {
                       |    String lane = sub(basename(bam), "_[AGTC]+_1.bam", "")
                       |    if (lane == "1") {
                       |        String bam_lane1 = bam
                       |    }
                       |    if (lane == "2") {
                       |        String bam_lane2 = bam
                       |    }
                       |}
                       |Array[String] lane1_bams = select_all(bam_lane1)
                       |""".stripMargin

    ignore should "maintain line ordering" in {
        val path = new File("src/test/resources/bugs/scatter_variable_not_found.wdl")
        val wfSourceCode = scala.io.Source.fromFile(path).mkString

        val (wf : WorkflowDefinition, _, _) = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val (_, _, blocks, _ ) = Block.split(wf.innerGraph, wfSourceCode)
        val b = blocks(0)
        val s = WomPrettyPrintApproxWdl.block(b)
        normalize(s) shouldBe(normalize(expected))
    }

}
