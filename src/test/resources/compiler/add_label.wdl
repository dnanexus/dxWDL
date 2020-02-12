version 1.0

task add_label {
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

    parameter_meta {
        a: {
            label: "A positive integer"
        }
        b: {
            label: "A negative integer"
        }
    }

    output {
        Int result = read_int(stdout())
    }
}
