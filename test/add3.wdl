# A simple workflow with two stages wired together.
# It is supposed to sum three integers.
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

    call add3_Add {
         input: a = ai, b = bi
    }
    call add3_Add as Add3 {
         input: a = add3_Add.sum, b = ci
    }
    output {
        Add3.sum
    }
}
