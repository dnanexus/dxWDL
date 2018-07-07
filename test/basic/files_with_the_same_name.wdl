import "library_sys_call.wdl" as lib

workflow files_with_the_same_name {
    Array[File] in_files
    call lib.FileArraySize {
        input: files = in_files
    }
    output {
        Int result = FileArraySize.result
    }
}
