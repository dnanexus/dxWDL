task int_ops {
    Int ai
    Int bi

    command {
    }
    output {
        Int result = ai * bi + 1
    }
}

workflow call_expressions {
    Int i1
    Int i2

    call int_ops {
        input: ai=i1, bi=i2
    }
    call int_ops as int_ops2 {
        input: ai = (int_ops.result * 2), bi = (int_ops.result + 1)
    }

    output {
        int_ops.result
    }
}
