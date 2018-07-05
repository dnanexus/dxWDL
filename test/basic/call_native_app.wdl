import "dx_app_extern.wdl" as lib

workflow call_native_app {
    String my_name

    call lib.native_hello as nh {
        input: who= my_name
    }
    output {
        String result = nh.greeting
    }
}
