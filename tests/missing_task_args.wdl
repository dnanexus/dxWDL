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
    call Add {
         input: a = 1
    }
    output {
        Add.sum
    }
}
