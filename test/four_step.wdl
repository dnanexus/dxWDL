# A simple workflow with two stages wired together.
# It is supposed to sum three integers.
import "library_math.wdl" as lib

workflow four_step {
    Int ai
    Int bi

    call lib.Add as Add {
         input: a = ai, b = bi
    }
    call lib.Add as Add2 {
         input: a = Add.sum, b = Add.sum
    }
    call lib.Add as Add3 {
         input: a = Add2.sum, b = Add2.sum
    }
    call lib.Add as Add4 {
         input: a = Add3.sum, b = Add3.sum
    }
    output {
        Add4.sum
    }
}
