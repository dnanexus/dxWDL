workflow strings {
    String s = "Anteater"

    # The following two calls are equivalent to
    # concatenating three variables.
    # Test string interpolation.
    call concat {
        input:
            x = "${s}.aligned",
            y = "${s}.duplicate_metrics"
    }

    output {
        String result = concat.result
    }
}

# Concatenate two string
task concat {
    String x
    String y

    command {
        echo ${x}_${y}
    }
    output {
        String result = read_string(stdout())
    }
}
