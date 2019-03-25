# Workflow with declarations in the middle that can be lifted to the top level.
import "library_string.wdl" as lib
import "library_math.wdl" as math_lib

workflow decl_mid_wf {
    String s
    Int i

    call math_lib.Add as add {
        input: a = (i * 2), b = (i+3)
    }

    String q = sub(s, "frogs", "RIP")
    Int j = i + i

    call lib.Concat as concat {
        input:
                x = s + ".aligned",
                y = q + ".wgs"
    }

    Int k = 2 * j
    # This declaration should not be moved by the reorg
    # step
    String c_result = concat.result

    call math_lib.Add as add2 {
        input: a = j, b = k
    }

    output {
        Int add_result = add.result
        Int add2_result = add2.result
        String concat_result = concat.result
    }
}
