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

# Incorrect, can't mark a file-array as streaming
task sundry {
    input {
        Array[File] aF
        File? oF
    }

    parameter_meta {
        aF: "stream"
        oF: "stream"
    }
    command {}
}
