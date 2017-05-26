import "library_sys_call.wdl" as lib

workflow system_calls2 {
    String pattern

    call ps
    call lib.cgrep as cgrep {
        input: in_file = ps.procs, pattern=pattern
    }
    call lib.wc as wc {
        input: in_file = ps.procs
    }
    output {
        cgrep.count
        wc.count
    }
}
