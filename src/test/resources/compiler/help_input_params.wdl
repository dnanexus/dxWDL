version 1.0

# Marking files as streaming should succeed. Marking
# non files should fail.

# `help` should ve a valid key and the message passed through

# Correct
task help_input_params_cgrep {
    input {
        String pattern
        File in_file
        String s
    }
    parameter_meta {
      in_file : {
        help: "The input file to be searched",
        group: "Common",
        label: "Input file"
      }
      pattern: {
        description: "The pattern to use to search in_file",
        group: "Common",
        label: "Search pattern"
      }
      s: "This is help for s"
    }
    command {
        grep '${pattern}' ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}

task help_input_params_diff {
    input {
        File a
        File b
    }
    parameter_meta {
        a : {
          help: "lefthand file",
          group: "Files",
          label: "File A"
        }
        b : {
          help: "righthand file",
          group: "Files",
          label: "File B"
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
