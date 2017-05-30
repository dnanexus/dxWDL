task string_ops {
    String p1
    String p2
    String p3
    String p4

    command {
    }
    output {
        String result = p1 + "__" + p2 + "__" + p3
    }
}

workflow string_expr {
    call string_ops {
        input:
            p1 = "delicate" + ".aligned",
            p2 = "number" + ".duplicate_metrics",
            p3 = sub("frogs_toads_salamander", "frogs", "xRIPx"),
            p4 = "ignored variable"
    }

    output {
        string_ops.result
    }
}
