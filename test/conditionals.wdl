import "library_math.wdl" as lib

workflow conditionals {
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

    # scatter that returns an optional type
    scatter (i in powers10) {
        call lib.MaybeInt { input: a=i }
    }
    output {
        MaybeInt.result
    }
}
