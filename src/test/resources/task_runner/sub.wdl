version 1.0

task sub {
    input {
        Int a
        Int b
    }
    command {
        echo $((${a} - ${b}))
    }
    output {
        Int result = read_int(stdout())
    }
}
