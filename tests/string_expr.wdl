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

workflow string_expr {
    call string_ops {
        input:
            p1 = "delicate" + ".aligned",
            p2 = "number" + ".duplicate_metrics",
            p3 = sub("frogs_toads_salamander", "frogs", "xRIPx")
    }

    output {
        string_ops.result
    }
}
