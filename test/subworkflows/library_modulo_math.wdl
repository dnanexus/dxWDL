task add_modulo {
    Int n
    Int a
    Int b

    command <<<
        python -c "print((${a} + ${b}) % ${n})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task mul_modulo {
    Int n
    Int a
    Int b

    command <<<
        python -c "print((${a} * ${b}) % ${n})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}
