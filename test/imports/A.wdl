version 1.0

# check that we can specify import directories on the command line.
import "library.wdl" as lib

workflow A {
    input {
        Int x
        Int y
    }
    call lib.B { input: a = x, b = y }

    output {
        Int result = B.result
    }
}
