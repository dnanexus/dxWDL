task bb_prepare {
    command <<<
    python -c "print('one\ntwo\nthree\nfour')"
    >>>
    output {
        Array[String] array = read_lines(stdout())
    }
}

workflow output_array {
    call bb_prepare as prepare
    output {
        prepare.array
    }
}
