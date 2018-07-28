import "foo_toplevel.wdl" as top

workflow foo_if_flag {
    Boolean flag
    File data

    if (flag) {
        call top.file_size {input:  data = data }
    }
    output {
        file_size.result
    }
}
