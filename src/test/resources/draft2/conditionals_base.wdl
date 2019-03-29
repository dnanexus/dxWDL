import "library_math.wdl" as lib

workflow conditionals_base {
    # Return null from an optional
    if (true) {
        call lib.MaybeInt as mi1
    }
    if (true) {
        call nonEmptyArray
    }

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
        call lib.MaybeInt as mi2 { input: a=i }
    }
    Array[Int] r =select_all(mi2.result)

    output {
        Int? e = mi1.result
        Array[Int] results = r
        Array[Int]+? nea = nonEmptyArray.results
    }
}

# Return a non-empty array type. We want
# to test that it works when we put an
# optional modifier on it.
task nonEmptyArray {
    command {}
    output {
        Array[Int]+ results = [5,7]
    }
}
