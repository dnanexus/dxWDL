# library of math module N
task add {
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

task sub {
    Int n
    Int a
    Int b

    command <<<
        python -c "print((${a} - ${b}) % ${n})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task mul {
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
