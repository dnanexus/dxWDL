# A simple workflow with two stages wired together.
# It is supposed to sum three integers.
# This one is a variant that is has a forward reference
# First and very simple test of topological sorting
task add3_Add {
    Int a
    Int b

    command {
        echo $((a + b))
    }
    output {
        Int sum = a + b
    }
}

workflow add3 {
    Int ai
    Int bi
    Int ci

    call add3_Add as Add3 {
         input: a = add3_Add.sum, b = ci
    }
    call add3_Add {
         input: a = ai, b = bi
    }
    output {
        Add3.sum
    }
}
