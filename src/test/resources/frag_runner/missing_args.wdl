version 1.0

# A WDL workflow with expressions, but without branches and loops.

workflow missing_args {
    input {
        Int? x
        Int y
    }

    Int retval = select_first([x, y])

    output {
        Int result = retval
    }
}
