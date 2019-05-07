version 1.0

import "dx_app_extern.wdl" as lib

workflow call_native_app {
    input {
        String my_name
    }

    call lib.native_hello as nh {
        input: who= my_name
    }
    output {
        String result = nh.greeting
    }
}
