
version 1.0

task secondary {
  input {
    String output_file_name
  }

  command <<<
    touch "~{output_file_name}"

  >>>
  output {
    File output_file = "${output_file_name}"
  }
}

task secondary_two {
    input {
        File input_file
    }

    command <<<
      echo "hi" >> ~{input_file}
      mv ~{input_file} out_2.txt
    >>>

    output {
        File output_file = "out_2.txt"
    }
}


workflow secondary_wf  {
  input {
    String output_file_name
  }

  call secondary {
    input: output_file_name = output_file_name
  }

  call secondary_two {
    input: input_file = secondary.output_file

  }

  output {
    File output_file = secondary.output_file
    File output_file_secondary = secondary_two.output_file
  }
}
