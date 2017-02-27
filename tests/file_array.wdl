task wc {
    Array[File] files

    command {
        wc ${sep=' ' files}
    }
    output {
        String result = read_string(stdout())
        Array[File] result_files = files
    }
}

workflow file_array {
    Array[File] fs

    call wc {
        input : files=fs
    }
    output {
        wc.result
        wc.result_files
    }
}
