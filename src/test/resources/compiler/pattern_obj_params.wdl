version 1.0

# adding a pattern attr to the parameter_meta section for an input variable
# should propogate that var to the interface

# Correct
task pattern_params_obj_cgrep {
    input {
        String pattern
        File in_file
    }
    parameter_meta {
        in_file: {
          help: "The input file to be searched",
          patterns: {
              name: ["*.txt", "*.tsv"],
              class: "file",
              tag: ["foo", "bar"]
          }
        }
        pattern: {
          help: "The pattern to use to search in_file"
        }
        out_file: {
          patterns: ["*.txt", "*.tsv"]
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
