task prepare {
    command <<<
    python -c "print('one\ntwo\nthree\nfour')"
    >>>
    output {
        Array[String] array = read_lines(stdout())
    }
}

workflow output_array {
    call prepare
    output {
        prepare.array
    }
}
