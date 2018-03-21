import "wf.wdl" as sub

workflow toplevel_unbound_arg {
    call sub.wf { input: arg1 = 3 }
    output {
        Int result = wf.result
    }
}
