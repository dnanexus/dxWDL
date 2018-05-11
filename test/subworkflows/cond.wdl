# A sub-block with a conditional
import "library_math.wdl" as lib

workflow cond {
    Boolean flag
    Int n = 7

    if (flag) {
        call lib.add { input: a=5, b=1, n=n }
    }
    Int i = select_first([add.result])

    if (flag) {
        call lib.mul {input: a=i, b=i, n=n}
    }

    output {
        Int? add_r = add.result
        Int? mul_r = mul.result
    }
}
