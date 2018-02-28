import "array_mod_add.wdl" as add_lib
import "array_mod_mul.wdl" as mul_lib

workflow modulo {
    Int n

    call add_lib.array_mod_add as add1 {
        input: n=n, a=3
    }
    call mul_lib.array_mod_mul as mul1 {
        input: n=n, a=2
    }

    output {
        Array[Int] addr = add1.result
        Array[Int] mulr = mul1.result
    }
}
