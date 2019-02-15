task inc {
    Int i
    command {}
    output {
        Int result = i + 1
    }
}

workflow partial_output_definition {
    Int a

    call inc { input: i=a }

    output {
        inc.result
    }
}
