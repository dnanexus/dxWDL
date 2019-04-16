version 1.0

task samtools_count {
    input {
        File bam
    }
    command <<<
        samtools view -c ${bam}
    >>>
    runtime {
        docker: "biocontainers/samtools:v1.7.0_cv3"
    }
    output {
        Int reads = read_int(stdout())
    }
}
