# A simple workflow with two stages wired together.
# It calculates simple math operations.
import "library_math.wdl" as lib

workflow math {
    Int ai

    call lib.Add  as Add {
         input: a = ai, b = 3
    }
    call lib.Multiply as Multiply {
         input: a = Add.result + 10, b = 2
    }
    output {
        Multiply.result
    }
}
