task ps {
    command {
        ps aux
    }
    output {
        File procs = stdout()
    }
}

task cgrep {
    String pattern
    File in_file

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

workflow system_calls2 {
    String pattern

    call ps
    call cgrep {
        input: in_file = ps.procs, pattern=pattern
    }
    call wc {
        input: in_file = ps.procs
    }
    output {
        cgrep.count
        wc.count
    }
}
