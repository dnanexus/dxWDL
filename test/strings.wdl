import "library_string.wdl" as lib

workflow strings {
    String s
    Array[String] sa

    # The following two calls are equivalent to
    # concatenating three variables
    call lib.Concat as concat1 {
        input:
            x = s + ".aligned",
            y = s + ".duplicate_metrics"
    }
    call lib.Concat as concat2 {
        input:
            x = concat1.result,
            y = sub(s, "frogs", "xRIPx")
    }

    call lib.ConcatArray as concat3 {
        input: words = [
            "delicate" + ".aligned",
            "number" + ".duplicate_metrics",
            sub("frogs_toads_salamander", "frogs", "xRIPx")
        ]
    }
    call lib.ConcatArray as concat4 {
        input : words=sa
    }

    output {
        concat1.result
        concat2.result
        concat3.result
        concat4.result
    }
}
