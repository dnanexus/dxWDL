version 1.0

# adding a choices attr to the parameter_meta section for an input variable
# should propogate that var to the interface

# Correct
task suggestion_values_cgrep {
    input {
        String pattern
        File in_file
    }
    parameter_meta {
        in_file: {
          suggestions: [
              {
                  name: "file2",
                  project: "project-FGpfqjQ0ffPF1Q106JYP2j3v",
              }
          ]
        }
        pattern: {
          suggestions: ["A", "B"]
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
