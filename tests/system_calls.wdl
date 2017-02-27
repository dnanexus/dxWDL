task cgrep {
    File in_file
    String pattern

    command {
        grep '${pattern}' ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}

task wc {
    File in_file
    command {
        cat ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}

workflow system_calls {
    File data
    String pattern

    call cgrep {
        input: in_file = data, pattern = pattern
    }
    call wc {
        input: in_file = data
    }
    output {
        cgrep.count
        wc.count
    }
}
