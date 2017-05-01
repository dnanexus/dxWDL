# A simple workflow with two stages wired together.
# It is supposed to sum three integers.
task Add {
    Int a
    Int b

    command {
        echo $((a + b))
    }
    output {
        Int result = a + b
        Int result2 = a - b
    }
}

task Multiply {
    Int a
    Int b

    command {
        echo $((a * b))
    }
    output {
        Int result = a * b
    }
}

workflow math {
    Int ai

    call Add {
         input: a = ai, b = 3
    }
    call Multiply {
         input: a = Add.result + 10, b = 2
    }
    output {
        Multiply.result
    }
}
