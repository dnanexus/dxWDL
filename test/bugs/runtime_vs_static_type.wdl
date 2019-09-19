version 1.0

workflow runtime_vs_static_type {
    call opt_int { input: x = 14 }
    output {
        Int result = opt_int.result
    }
}


task opt_int {
    input {
        Int? x
    }
    command {
        echo $(( ~{x} + 10 ))
    }
    output {
        Int result = read_int(stdout())
    }
}
