# library of math module N
task z_add {
    Int n
    Int a
    Int b

    command <<<
        python3 -c "print((${a} + ${b}) % ${n})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task z_sub {
    Int n
    Int a
    Int b

    command <<<
        python3 -c "print((${a} - ${b}) % ${n})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task z_mul {
    Int n
    Int a
    Int b

    command <<<
        python3 -c "print((${a} * ${b}) % ${n})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}


task z_MaybeInt {
    Int? a
    command {
    }
    output {
        Int? result = a
    }
}
