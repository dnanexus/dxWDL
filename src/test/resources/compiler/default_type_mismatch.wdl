version 1.0

# This should fail to compile due to a type mismatch between 'b' and its default value
# in parameter_meta

task add_default {
    input {
        Int a = 1
        Int? b
    }

    Int b_actual = select_first([b, 2])

    command {
        echo $((${a} + ${b_actual}))
    }

    meta {
        summary: "Adds two int together"
    }

    parameter_meta {
        b: {
            default: "two"
        }
    }

    output {
        Int result = read_int(stdout())
    }
}
