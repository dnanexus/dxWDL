workflow check_route {
    String prefix
    Array[String] ranges

    scatter (range in ranges) {
        call Concat as concat1 {
            input:
              x=prefix,
              y=range
        }
    }

    output {
        Array[String] result = concat1.result
    }
}

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
