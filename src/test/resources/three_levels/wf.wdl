import "math.wdl" as sub

workflow wf {
    Int arg1

    call sub.addition {
        input: a=arg1, b=arg1
    }
    output {
        Int result = addition.result
    }
}
