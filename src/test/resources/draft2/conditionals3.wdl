workflow conditionals_base {
    # Artifically construct an array of optional integers
    if (true) {
        Int i1 = 1
    }
    if (false) {
        Int i2 = 10
    }
    if (true) {
        Int i3 = 100
    }

    Array[Int?] powers10 = [i1, i2, i3]

    output {
        Array[Int?] r = powers10
    }
}
