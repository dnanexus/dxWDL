task mul2 {
    Int i

    command {
        python -c "print(${i} + ${i})"
    }
    output {
        Int result = read_int(stdout())
    }
}

workflow ngs {
    Int A
    Int B
    call mul2 { input: i=C }
    output {
        Int result = mul2.result
    }
}
