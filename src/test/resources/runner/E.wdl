# scatter inside an if

workflow E {
    Array[Int] primes = [1, 2, 3, 5]

    if (true) {
        scatter (p in primes) {
            Int cube = p * p * p
        }
    }
    if (false) {
        scatter (p in primes) {
            Int square = p * p
        }
    }
    output {
        cube
        square
    }
}
