import "library_math.wdl" as lib
import "library_string.wdl" as lib_string

workflow call_expressions2 {
    String s
    Int i

    # The following to calls are equivalent to
    # concatenating three variables
    call lib_string.Concat as concat {
        input:
            x = s + ".aligned",
            y = s + ".duplicate_metrics"
    }
    call lib_string.Concat as string_ops {
        input:
            x = concat.result,
            y = sub(s, "frogs", "xRIPx")
    }
    call lib.IntOps as int_ops {
        input: a = (i * 2), b = (i+3)
    }
    call lib.IntOps as int_ops2 {
        input: a = (int_ops.result * 5), b = (int_ops.result + 1)
    }

    # A missing output section is interpreted as a global wildcard
}
