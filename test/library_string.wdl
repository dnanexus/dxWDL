# Concatenate two string
task Concat {
    String x
    String y

    command {
        echo ${x}_${y}
    }
    output {
        String result = read_string(stdout())
    }
}
