version 1.0

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
    input {
        Array[Array[String]] words
    }
    command {
        cat ${write_tsv(words)}
    }
    output {
        String result = read_string(stdout())
    }
}

workflow strings {
    input {
        String s
        Array[String] sa
    }

    # Ragged array of strings
    call createTsv
    call processTsv { input: words = createTsv.result }

    output {
        String tsv_result = processTsv.result
    }
}
