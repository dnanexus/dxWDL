version 1.0

import "unpassed_default_arg.wdl" as lib

workflow top {
    input {
        Array[String] arr
    }

    call lib.unpassed_default_arg {
        input:
            initial_arr=arr,
    }
}
