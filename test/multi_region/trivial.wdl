version 1.0

# A trivial script, to test the basic sanity
# of a dxWDL release.

workflow trivial {
    input {
        Int x = 3
        Int y = 5
    }

    call xxAdd as Add {
        input: a=x, b=y
    }
    output {
        Int sum = Add.result
    }
}

# library of math module N
task xxAdd {
    input {
        Int a
        Int b
    }
    command {}
    output {
        Int result = a + b
    }
}
