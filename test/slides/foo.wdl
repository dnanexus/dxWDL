workflow foo {
    Boolean flag
    File genome_man
    File genome_mouse

    call file_size {input: data = genome_man}

    if (flag) {
        call file_size as f2 {input:  data = genome_mouse }
    }
    output {
        file_size.result
        f2.result
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
