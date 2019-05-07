package dxWDL.util

import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}

class WomPrettyPrintApproxWdlTest extends FlatSpec with Matchers {

    private def normalize(s: String) : String = {
        // all sequences of whitespace with a single whitespace character
//        val s1 = s.replaceAll("[\t ]+", " ")

        // Remove empty lines
        //        s1.replaceAll("^\\s*", "")

        s.replaceAll("\\s+", " ")
    }

    val workflow1 =
        """|
           |workflow foo {
           |    scatter(bam in ["1_ACGT_1.bam", "2_TCCT_1.bam"]) {
           |        String lane = sub(basename(bam), "_[AGTC]+_1.bam", "")
           |        if (lane == "1") {
           |            String bam_lane1 = bam
           |        }
           |    }
           |    Array[String] lane1_bams = select_all(bam_lane1)
           |}
           |""".stripMargin

    val expected1 =
        """|
           |scatter(bam in ["1_ACGT_1.bam", "2_TCCT_1.bam"]) {
           |    String lane = sub(basename(bam), "_[AGTC]+_1.bam", "")
           |    if (lane == "1") {
           |        String bam_lane1 = bam
           |    }
           |}
           |Array[String] lane1_bams = select_all(bam_lane1)
           |""".stripMargin

    ignore should "maintain line ordering" in {
        val (wf : WorkflowDefinition, _, _) = ParseWomSourceFile.parseWdlWorkflow(workflow1)
        val (_, _, blocks, _ ) = Block.split(wf.innerGraph, workflow1)
        val b = blocks(0)
        val s = WomPrettyPrintApproxWdl.block(b)
        normalize(s) shouldBe(normalize(expected1))
    }

}
