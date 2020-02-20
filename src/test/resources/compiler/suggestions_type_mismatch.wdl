version 1.0

# Incorrect - choice value types do not match parameter types
task suggestion_values_cgrep {
    input {
        String pattern
        File in_file
    }
    parameter_meta {
        in_file: {
          suggestions: [0, 1]
        }
        pattern: {
          suggestions: [true, false]
        }
    }
    command {
        grep '${pattern}' ${in_file} | wc -l
        cp ${in_file} out_file
    }
    output {
        Int count = read_int(stdout())
        File out_file = "out_file"
    }
}

