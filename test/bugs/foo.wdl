task write_lines_bug {
    Array[File] files

    command <<<
    filenames=${write_lines(files)}
    echo "wrote to file $filenames"

    cat $filenames
    >>>
    output {
        String result = read_string(stdout())
    }
    runtime {
        docker: "ubuntu:16.04"
    }
}

workflow foo {
    Array[File] files
    call write_lines_bug {input : files = files}
    output {
        String result = write_lines_bug.result
    }
}
