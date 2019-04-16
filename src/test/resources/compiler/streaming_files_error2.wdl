version 1.0

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
