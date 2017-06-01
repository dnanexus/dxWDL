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

workflow four_step {
    Int ai
    Int bi

    call Add {
         input: a = ai, b = bi
    }
    call Add as Add2 {
         input: a = Add.sum, b = Add.sum
    }
    call Add as Add3 {
         input: a = Add2.sum, b = Add2.sum
    }
    call Add as Add4 {
         input: a = Add3.sum, b = Add3.sum
    }
    output {
        Add4.sum
    }
}
