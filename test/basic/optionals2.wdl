version 1.0

workflow optionals2 {
    Boolean use_cas = false
    Int h1 = 13

    if (use_cas) {
        call y_inc { input : a = 3 }
    }
    Int retval = select_first([y_inc.result, h1])

    output {
        Int result = retval
    }
}

task y_inc {
    input {
        Int a
    }
    command {}
    output {
        Int result= a + 1
    }
}
