version 1.0

task add_group {
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
            group: "common"
        }
        b: {
            group: "obscure"
        }
    }

    output {
        Int result = read_int(stdout())
    }
}
