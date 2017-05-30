import "library_sys_call.wdl" as lib

task aa_wc {
    Array[File] files

    command {
        wc ${sep=' ' files}
    }
    output {
        String result = read_string(stdout())
        Array[File] result_files = files
    }
}

task aa_diff {
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

    call aa_wc as wc {
        input : files=fs
    }
    call aa_diff as diff {
        input : A=wc.result_files[0],
                B=wc.result_files[1]
    }
    call lib.Colocation as colocation {
        input : A=fs[0], B=fs[1]
    }
    output {
        wc.result
        diff.result
        colocation.result
    }
}
