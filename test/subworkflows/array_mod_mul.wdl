import "library_modulo_math.wdl" as mod

workflow array_mod_mul {
    Int n
    Int a

    scatter (i in range(n)) {
        call mod.mul_modulo as mul {
            input: n=n, a=a, b=i
        }
    }
    output {
        Array[Int] result = mul.result
    }
}
