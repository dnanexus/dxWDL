version 1.0

# check that the docker option is passed correctly from the extras file.

task biocontainers {
    command {
        # check that bwa actually exists in the docker image
        samtools --help
    }
    runtime {
        memory: "16 GB"
    }
    output {
        String o = read_string(stdout())
    }
}
