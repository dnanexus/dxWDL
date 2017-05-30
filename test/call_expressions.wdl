import "library_math.wdl" as lib

workflow call_expressions {
    Int i1
    Int i2

    call lib.IntOps as int_ops1 {
        input: a=i1, b=i2
    }
    call lib.IntOps as int_ops2 {
        input: a = (int_ops1.result * 2), b = (int_ops1.result + 1)
    }

    output {
        int_ops2.result
    }
}
