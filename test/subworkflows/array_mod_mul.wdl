import "library_modulo_math.wdl" as mod

# With arithmetic in the [mod n] multiplicative group, calculate:
# [a, a^2, a^3, ..., a^n]
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
