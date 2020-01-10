version 1.0

# Marking files as streaming should succeed. Marking
# non files should fail.

# `help` should ve a valid key and the message passed through

# Correct
task help_output_params_cgrep {
    input {
        String pattern
        File in_file
    }
    parameter_meta {
        in_file : {
          help: "The input file to be searched"
        }
        pattern: {
          help: "The pattern to use to search in_file"
        }
        count: {
          help: "The number of lines in the input file containing the pattern"
        }
    }
    command {
        grep '${pattern}' ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}

task help_output_params_diff {
    input {
        File a
        File b
    }
    parameter_meta {
        a : {
          help: "lefthand file"
        }
        b : {
          help: "righthand file"
        }
        result: {
          help: "The number of different lines"
        }
    }
    runtime {
        docker: "ubuntu:16.04"
    }
    command {
        diff ${a} ${b} | wc -l
    }
    output {
        Int result = read_int(stdout())
    }
}
