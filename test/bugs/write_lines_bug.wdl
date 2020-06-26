# this used to fail due to a bug in WOM where file arguments to
# a function were not coerced to strings - should be fixed with wdlTools
task write_lines_bug {
    Array[File] files
    command <<<
    filenames=${write_lines(files)}
    cat $filenames
    >>>
    output {
        Array[String] result = read_lines(stdout())
    }
    runtime {
        docker: "ubuntu:16.04"
    }
}
