import "unbound_arg.wdl" as sub

workflow toplevel_unbound_arg {
    call sub.unbound_arg { input: arg1 = 3 }
    output {
        Array[Int] result = unbound_arg.result
    }
}
