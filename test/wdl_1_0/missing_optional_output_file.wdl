version 1.0

task missing_optional_output_file {
    command <<<
        touch something.txt
    >>>
    output {
        File? non_existent_file = "nofile"
    }
}
