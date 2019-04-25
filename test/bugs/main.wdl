version 1.0

import "imported.wdl"

# here's an overview of what we're doing
workflow outer {
    input {
        Array[String] arr
    }

    call imported.inner_wf {
        input:
            initial_arr=arr,
    }
}
