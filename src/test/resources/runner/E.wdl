# scatter inside an if

workflow E {
    Array[Int] primes = [1, 2, 3, 5]
    Boolean alwaysTrue = true
    Boolean alwaysFalse = false

    if (alwaysTrue) {
        scatter (p in primes) {
            Int cube = p * p * p
        }
    }
    if (alwaysFalse) {
        scatter (p in primes) {
            Int square = p * p
        }
    }
    output {
        cube
        square
    }
}
