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

# Concatenate array of strings
task ConcatArray {
    Array[String] words

    command {
        echo ${sep='_' words}
    }
    output {
      String result = read_string(stdout())
    }
}
