version 1.0

task bug16_chained_operations {
    input {
        Int i
    }

    Int j = i + i + i

    command <<<
    echo ~{j}
    >>>

    output {
        Int jout = j
    }

    runtime {
        docker: "debian:stretch-slim"
    }
}