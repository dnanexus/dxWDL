task mul2 {
    Int i

    command {
        python -c "print(${i} + ${i})"
    }
    output {
        Int result = read_int(stdout())
    }
}

workflow unbound_arg {
    Int arg1

    # A call missing a compulsory argument
    call mul2
    output {
        Int result = mul2.result
    }
}
