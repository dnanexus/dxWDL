# A simple workflow with two stages wired together.
# It is supposed to sum three integers.
task Add {
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

    call Add {
         input: a = ai, b = bi
    }
    call Add as Add3 {
         input: a = Add.sum, b = ci
    }
    output {
        Add3.sum
    }
}
