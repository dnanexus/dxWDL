version 1.0

workflow inner_wf {
    input {}

    call foo as f1

    call foo as f2 { input: unpassed_arg_default = false }

    output {}
}

task foo {
    input {
        Boolean unpassed_arg_default = true
    }
    command {}
    output {}
}
