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
    call lib.IntOps as int_ops4 {
        input: a = (ai * 2), b = (bi+3)
    }
    call lib.IntOps as int_ops5 {
        input: a = (int_ops4.result * 5), b = (int_ops4.result + 1)
    }

    output {
        Int x = int_ops3.mul
        Int y = int_ops3.sub
        Int sum = int_ops5.sum
        Int div = int_ops5.div
    }
}
