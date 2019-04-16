task diff {
    File a
    File b

    parameter_meta {
        a : "stream"
        b : "stream"
    }
    command {
        diff ${a} ${b} | wc -l
    }
    output {
        Int result = read_int(stdout())
    }
}
