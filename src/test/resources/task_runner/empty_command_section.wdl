version 1.0

task empty_command_section {
    input {
        Int a
        Int b
    }
    command {
    }
    output {
        Int result = a + b
    }
}
