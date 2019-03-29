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

# A task that sets a default for an optional input
task MaybeString {
    String? instrument = "french horn"
    command {
    }
    output {
        String? result = instrument
    }
}
