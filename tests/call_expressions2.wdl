task ident {
    String s1
    Int i1

    command {
    }
    output {
        String s = s1
        Int i = i1
    }
}

task string_ops {
    String p1
    String p2
    String p3

    command {
    }
    output {
        String result = p1 + "__" + p2 + "__" + p3
    }
}

task int_ops {
    Int ai
    Int bi

    command {
    }
    output {
        Int result = ai * bi + 1
    }
}

workflow call_expressions2 {
    String s
    Int i

    call string_ops {
        input:
            p1 = s + ".aligned",
            p2 = s + ".duplicate_metrics",
            p3 = sub(s, "frogs", "xRIPx")
    }
    call int_ops {
        input: ai = (i * 2), bi = (i+3)
    }
    call int_ops as int_ops2 {
        input: ai = (int_ops.result * 5), bi = (int_ops.result + 1)
    }

    output {
        string_ops.result
        int_ops.result
    }
}
