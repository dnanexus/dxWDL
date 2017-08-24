task diff {
    File a { stream: true}
    File b { stream: true}

    runtime {
        docker: "ubuntu:16.04"
    }
    command {
        diff ${a} ${b} | wc -l
    }
    output {
        Int result = read_int(stdout())
    }
}

workflow quick_diff {
    File x
    File y

    call diff {
        input: a=x, b=y
    }
    output {
        diff.result
    }
}
