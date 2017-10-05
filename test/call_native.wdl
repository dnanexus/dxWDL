import "dx_extern.wdl" as lib

workflow call_native {
    call lib.mk_int_list as mk_list {
        input: a=3, b=5
    }

    output {
        mk_list.all
    }
}
