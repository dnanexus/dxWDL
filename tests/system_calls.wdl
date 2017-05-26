import "sys_call_lib.wdl" as lib

workflow system_calls {
    File data
    String pattern

    call lib.cgrep as cgrep {
        input: in_file = data, pattern = pattern
    }
    call lib.wc as wc {
        input: in_file = data
    }
    output {
        cgrep.count
        wc.count
    }
}
