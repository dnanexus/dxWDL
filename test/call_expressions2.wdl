import "library_math.wdl" as lib

task eee_ident {
    String s1
    Int i1

    command {
    }
    output {
        String s = s1
        Int i = i1
    }
}

task eee_string_ops {
    String p1
    String p2
    String p3

    command {
    }
    output {
        String result = p1 + "__" + p2 + "__" + p3
    }
}

workflow call_expressions2 {
    String s
    Int i

    call eee_string_ops as string_ops {
        input:
            p1 = s + ".aligned",
            p2 = s + ".duplicate_metrics",
            p3 = sub(s, "frogs", "xRIPx")
    }
    call lib.IntOps as int_ops {
        input: a = (i * 2), b = (i+3)
    }
    call lib.IntOps as int_ops2 {
        input: a = (int_ops.result * 5), b = (int_ops.result + 1)
    }

    # A missing output section is interpreted as a global wildcard
}
