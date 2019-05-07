version 1.0

workflow foo {
    scatter(bam in ["1_ACGT_1.bam", "2_TCCT_1.bam"]) {
        String lane = sub(basename(bam), "_[AGTC]+_1.bam", "")
        if (lane == "1") {
            String bam_lane1 = bam
        }
        if (lane == "2") {
            String bam_lane2 = bam
        }
    }
    Array[String] lane1_bams = select_all(bam_lane1)

    output {
        Array[String] result = lane1_bams
    }
}
