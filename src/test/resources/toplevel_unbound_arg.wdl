import "unbound_arg.wdl" as unbound_arg

workflow toplevel_unbound_arg {
    call unbound_arg.unbound_arg { input: arg1 = 3 }
}
