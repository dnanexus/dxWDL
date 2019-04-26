version 1.0

# The ___ symbol is reserved, an exception should be thrown
task add {
    input {
        Int a___x
        Int b___y
    }
    command {
        echo $((${a___x} + ${b___y}))
    }
    output {
        Int result = read_int(stdout())
    }
}
