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

task diff {
    input {
        File a
        File b
    }
    parameter_meta {
        a : "stream"
        b : "stream"
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
