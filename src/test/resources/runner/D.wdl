# Nested conditionals, should not create Optional[Optional] types

workflow D {
    Array[Int] primes = [1, 2, 3, 5, 7]

    scatter (p in primes) {
        Boolean cond1 = p > 1
        Boolean cond2 = p < 5

        if (cond1) {
            if (cond2) {
                Int chosen = p
            }
        }
    }
    output {
        chosen
    }
}
