version 1.0

task add_timeout_override {
    input {
        Int a
        Int b
    }
    command {
        echo $((${a} + ${b}))
    }
    meta {
        summary: "Adds two int together"
    }

    output {
        Int result = read_int(stdout())
    }
}
