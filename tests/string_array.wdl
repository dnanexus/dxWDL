# Try using an array of strings in a task
task y_Concat {
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

    call y_Concat as Concat {
        input : words=sa
    }
    output {
        Concat.result
    }
}
