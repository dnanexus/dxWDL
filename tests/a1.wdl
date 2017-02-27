task printTask {
    String output_bam_basename

    command {
       echo '${output_bam_basename}'
    }
    output {
        String result = read_string(stdout())
    }
}

workflow a1 {
    Array[File] flowcell_unmapped_bams = ["/tmp/AAA.bam", "/tmp/B.bam", "/tmp/C125.bam"]
    String unmapped_bam_suffix = "bam"

    scatter (unmapped_bam in flowcell_unmapped_bams) {
        String sub_strip_path = "gs://.*/"
        String sub_strip_unmapped = unmapped_bam_suffix + "$"

        call printTask {input:
                          pattern = "AXX",
                          output_bam_basename = sub(sub(unmapped_bam, sub_strip_path, ""), sub_strip_unmapped, "") + ".unmerged"
        }
    }
    output {
        printTask.result
    }
}
