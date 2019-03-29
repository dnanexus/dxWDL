# A sub-block that is compiled into a subworkflow
import "library_math.wdl" as lib

workflow subblocks {
    Array[Int] numbers
    Int n = 7

    scatter (i in numbers) {
        call lib.z_add as add { input: a=i, b=1, n=n }

        Int k = add.result + 2

        call lib.z_sub as sub { input: a=k, b=3, n=n }

        Int j = sub.result * 2

        call lib.z_mul as mul { input: a=j, b=1, n=n }
    }

    output {
        Array[Int] add_a = add.result
        Array[Int] sub_a = sub.result
        Array[Int] mul_a = mul.result
    }
}
