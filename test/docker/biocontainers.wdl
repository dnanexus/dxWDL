version 1.0

task biocontainers {
    command {
        echo "hello"
    }
    runtime {
        docker: "biocontainers/bwa"
        memory: "16 GB"
    }
    output {
        String o = read_string(stdout())
    }
}
