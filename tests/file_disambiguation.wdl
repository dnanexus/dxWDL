import "library_sys_call.wdl" as lib

workflow file_disambiguation {
    File f1
    File f2

    call lib.Colocation as colocation {
        input : A=f1, B=f2
    }
    output {
        colocation.result
    }
}
