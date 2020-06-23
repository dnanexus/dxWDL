version 2.0

import "dnanexus_applet.wdl" as lib

workflow native_call_sum {
    input {
        Int a
        Int b
    }

    call lib.native_sum_012

    call lib.native_sum_wf as native_sum_wf {
        input: a=a, b=b
    }

    output {
        Int result = native_sum_wf.result
    }
}
