version 1.0

# Incorrect, can't mark an integer as streaming
task add {
    input {
        Int a
        Int b
    }

    parameter_meta {
        a : "stream"
    }
    command {}
    output {
        Int result = a + b
    }
}
