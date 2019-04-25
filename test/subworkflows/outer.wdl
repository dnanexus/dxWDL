version 1.0

import "inner.wdl" as lib

workflow outer {
    input {
        String lane
    }

    call lib.inner as inner {
        input: lane=lane
    }

    output {
        String o = inner.blah
    }
}
