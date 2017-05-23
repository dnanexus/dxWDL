# A simple workflow with two stages wired together.
# It is supposed to sum three integers.
import "library.wdl" as lib

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
