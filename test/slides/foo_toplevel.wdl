import "foo_if_flag.wdl" as bottom

workflow foo {
    Boolean flag
    File genome_man
    File genome_mouse

    call file_size {input: data = genome_man}

    call bottom.foo_if_flag { input:
        flag = flag,
        data = genome_mouse
    }
    output {
        file_size.result
        if_flag.result
    }
}

task file_size {
    File data
    command {
        ls -lh ${data}
    }
    output {
        String result = read_string(stdout())
    }
}
