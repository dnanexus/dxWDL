workflow math {
    Int x

    call add {input: a=3, b=x}

    output {
        Int result = add.result
    }
}

task add {
    Int a
    Int b

    command {
        echo $((${a} + ${b}))
    }
    output {
        Int result = read_int(stdout())
    }
}
