version 1.0

workflow scatter_subworkflow_with_optional {
    Array[Int] reads = [1, 2, 3]
    Array[Int] reads2 = [7, 9, 13]

    scatter ( read_pair in zip(reads, reads2) ) {
        call bwa_for_hba {
            input:
              a = read_pair.left,
              b = read_pair.right,
        }

        call run_gatk1_mask
    }
}

task bwa_for_hba {
    input {
        Int a
        Int b

        String samtools_memory = "3G"
    }
    output {
    }
    command {}
}

task run_gatk1_mask {
    command {}
}
