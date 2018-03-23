import "library_string.wdl" as lib

# Generate sets of intervals for scatter-gathering over chromosomes
task createTsv {
    # Use python to create a string parsed into a wdl Array[Array[String]]
    command<<<
    python <<CODE
    tsv_list = []
    ll = [["1"], ["2"], ["3", "4"], ["5"], ["6", "7", "8"]]
    for l in ll:
      tsv_list.append('\t'.join(l))
    tsv_string = '\n'.join(tsv_list)
    print tsv_string
    CODE
    >>>

    output {
      Array[Array[String]] result = read_tsv(stdout())
    }
}

task processTsv {
    Array[Array[String]] words
    command {
        cat ${write_tsv(words)}
    }
    output {
        String result = read_string(stdout())
    }
}

task processLine {
    Array[String] line
    command {
        echo ${sep=' INPUT=' line}
    }
    output {
        String result = read_string(stdout())
    }
}

task collect {
    Array[String] line
    command {
    }
    output {
        Array[String] result = line
    }
}

workflow strings {
    String s
    Array[String] sa

    # conditional blocks
    if (length(sa) == 3 ) {
        call lib.Concat as cond_c1 {
            input: x="hello", y="world"
        }
        call lib.Concat as cond_c2 {
            input: x=cond_c1.result, y="2"
        }
    }

    # The following two calls are equivalent to
    # concatenating three variables.
    # Test string interpolation.
    call lib.Concat as concat1 {
        input:
            x = "${s}.aligned",
            y = "${s}.duplicate_metrics"
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

    String fruit = "orange"
    String ice = "popsicle"
    call lib.ConcatArray as concat5 {
        input: words = [
            "Tamara likes ${fruit}s and ${ice}s",
            "Would you like some too?"
        ]
    }

    # Ragged array of strings
    call createTsv
    call processTsv {
        input : words=createTsv.result
    }
    scatter (x in createTsv.result) {
        call processLine as processLine {input : line=x}
    }
    call collect {
        input : line=processLine.result
    }

    output {
        String? cond_c2_result = cond_c2.result
        String concat1_result = concat1.result
        String concat2_result = concat2.result
        String concat3_result = concat3.result
        String concat4_result = concat4.result
        String concat5_result = concat5.result
        Array[String] collect_result = collect.result
    }
}
