# A very simple WDL file, that includes a single task
task join2 {
    String x
    String y

    command {
        echo ${x}_${y}
    }
    output {
        String result = read_string(stdout())
    }
}

workflow concat {
    String s1
    String s2

    call join2 {
        input: x = s1, y = s2
    }
    output {
        join2.result
    }
}
