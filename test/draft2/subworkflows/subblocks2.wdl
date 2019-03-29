# A sub-block that is compiled into a subworkflow
import "library_math.wdl" as lib

workflow subblocks2 {
    Array[Int] numbers
    Int n = 7

    scatter (i in numbers) {
        call lib.z_add as add { input: a=i, b=1, n=n }
        Int m = add.result + 2
    }

    output {
        Array[Int] ms = m
    }
}
