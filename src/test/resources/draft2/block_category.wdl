import "library_math.wdl" as lib

workflow block_category {
    # Return null from an optional
    if (true) {
        call lib.MaybeInt as empty
    }
    if (true) {
        call nonEmptyArray
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
