version 1.0

# Marking files as streaming should succeed. Marking
# non files should fail.

# Correct
task cgrep {
    input {
        String pattern
        File in_file
    }
    parameter_meta {
        in_file : "stream"
    }
    command {
        grep '${pattern}' ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}
