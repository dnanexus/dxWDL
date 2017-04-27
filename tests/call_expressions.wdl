task xx_int_ops {
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

    call xx_int_ops as int_ops1 {
        input: ai=i1, bi=i2
    }
    call xx_int_ops as int_ops2 {
        input: ai = (int_ops1.result * 2), bi = (int_ops1.result + 1)
    }

    output {
        int_ops2.result
    }
}
