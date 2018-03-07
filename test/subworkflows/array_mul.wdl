import "library_math.wdl" as lib

# With arithmetic in the [mod n] multiplicative group, calculate:
# [a, a^2, a^3, ..., a^n]
workflow array_mul {
    Int n
    Int a

    scatter (i in range(n)) {
        call lib.mul as mul {
            input: n=n, a=a, b=i
        }
    }
    output {
        Array[Int] result = mul.result
    }
}
