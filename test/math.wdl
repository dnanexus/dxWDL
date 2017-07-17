import "library_math.wdl" as lib

workflow math {
    Int ai
    Int bi

    call lib.IntOps as int_ops1 {
        input: a=ai, b=bi
    }
    call lib.IntOps as int_ops2 {
        input: a = (3 * 2), b = (5 + 1)
    }
    call lib.IntOps as int_ops3 {
        input: a = (3 + 2 - 1), b = 5 * 2
    }

    output {
        Int a_final = int_ops3.ai
        Int b_final = int_ops3.bi

        # Check references between output variables
        Int c = b_final
        Int d = a_final
    }
}
