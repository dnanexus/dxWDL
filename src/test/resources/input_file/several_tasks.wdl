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

task mul {
    Int a
    Int b

    command {
        echo $((${a} * ${b}))
    }
    output {
        Int result = read_int(stdout())
    }
}
