import "library_string.wdl" as lib

workflow string_expr {
    Array[String] sa

    call lib.ConcatArray as string_ops {
        input: words = [
            "delicate" + ".aligned",
            "number" + ".duplicate_metrics",
            sub("frogs_toads_salamander", "frogs", "xRIPx")
        ]
    }
    call lib.ConcatArray as concat {
        input : words=sa
    }

    output {
        string_ops.result
        concat.result
    }
}
