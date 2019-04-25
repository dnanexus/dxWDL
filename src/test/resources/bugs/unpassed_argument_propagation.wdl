version 1.0

workflow inner_wf {
    input {}
    call foo
    output {}
}

task foo {
    input {
        Boolean unpassed_arg_default = true
    }
    command {}
    output {}
}
