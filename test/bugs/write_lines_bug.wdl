task write_lines_bug {
    Array[File] files

    command <<<
    filenames=${write_lines(files)}
    cat $filenames
    >>>
    output {
        String result = read_string(stdout())
    }
    runtime {
        docker: "ubuntu:16.04"
    }
}
