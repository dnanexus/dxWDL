# Try using an array of strings in a task
task ConcatArray {
    Array[String] words

    command {
        echo ${sep=' INPUT=' words}
    }
    output {
      String result = read_string(stdout())
    }
}

workflow string_array {
    Array[String] sa

    call ConcatArray as concat {
        input : words=sa
    }
    output {
        concat.result
    }
}
