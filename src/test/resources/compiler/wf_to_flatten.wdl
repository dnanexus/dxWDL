version 1.0

import "library.wdl" as lib

workflow wf_to_flatten {
    call lib.NoArguments
    call lib.Concat {input: a = "a", b = "c" }
}
