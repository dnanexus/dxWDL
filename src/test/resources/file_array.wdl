task diff {
    File A
    File B
    command {
        diff ${A} ${B} | wc -l
    }
    output {
        Int result = read_int(stdout())
    }
}

workflow file_array {
    Array[File] fs
    call diff {
        input : A=fs[0], B=fs[1]
    }
    output {
        Int result = diff.result
    }
}
