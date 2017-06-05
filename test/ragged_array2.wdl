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

workflow ragged_array2 {
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
        collect.result
    }
}
