task fff_int_ops {
    Int a
    Int b

    command {
    }
    output {
        Int mul = a * b
        Int sum = a + b
        Int sub = a - b
        Int div = a / b
        Int ai = a
        Int bi = b
    }
}

workflow math_expr {
    Int ai
    Int bi

    call fff_int_ops as int_ops1 {
        input: a=ai, b=bi
    }
    call fff_int_ops as int_ops2 {
        input: a = (3 * 2), b = (5 + 1)
    }
    call fff_int_ops as int_ops3 {
        input: a = (3 + 2 - 1), b = 5 * 2
    }

    output {
        int_ops1.sum
        int_ops1.sub
        int_ops2.mul
        int_ops2.div
        int_ops3.sum
        int_ops3.mul
        int_ops3.ai
        int_ops3.bi
    }
}
