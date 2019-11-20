version 1.0

task some_other_task {
    input {
        String file_name
    }
    output {
      File some_file = "~{file_name}.txt"
    }
    command <<<
    echo "Hello World!" > ~{file_name}.txt;
    >>>
}
