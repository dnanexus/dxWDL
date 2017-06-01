task Prepare {
    command <<<
       python -c "print('one\ntwo\nthree\nfour')"
    >>>
    output {
        Array[String] array = read_lines(stdout())
    }
}

task Gather {
    Array[File] files
    command <<<
        wc ${sep=' ' files}
    >>>
    output {
        String str = read_string(stdout())
    }
}
