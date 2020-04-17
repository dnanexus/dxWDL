
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


workflow secondary_wf  {
  input {
    String output_file_name
  }

  call secondary {
    input: output_file_name = output_file_name
  }

  output {
    File output_file = secondary.output_file
  }
}
