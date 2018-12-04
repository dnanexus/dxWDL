version 1.0

# A trivial workflow, to test the basic sanity
# of a dxWDL release.
import "add.wdl"

workflow wf_two_inputs {
    input {
        Int x = 3
        Int y = 5
    }

    call add.add {
        input: a=x, b=y
    }
    output {
        Int sum = add.result
    }
}
