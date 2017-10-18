import "library_math.wdl" as lib

workflow math2 {
    # Conversion from Array[Int] to Array[Array[Int]]
    scatter (k in [2,3,5]) {
        call lib.RangeFromInt as rfi { input: len=k }
    }
    output {
        Array[Array[Int]] rra = rfi.result
    }
}
