import "library_math.wdl" as lib

workflow conditionals1 {
    Array[Int] powers10 = [1, 10]

    # scatter that returns an optional type
    scatter (i in powers10) {
        call lib.MaybeInt { input: a=i }
    }
    Array[Int] r =select_all(MaybeInt.result)

    output {
        Array[Int] results = r
    }
}
