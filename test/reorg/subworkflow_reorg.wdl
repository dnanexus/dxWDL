
version 1.0

import "secondary.wdl" as lib


task main {
  input {
    File input_file
  }
  command <<<
    echo "hello world" > out.txt
  >>>

  output {
    File output_file = "out.txt"
  }


}

workflow main_wf {
  input {
    String input_string
  }

  call lib.secondary_wf as sub_wf {
    input: output_file_name = input_string
  }

  call main {
    input: input_file=sub_wf.output_file
  }

  output {
    File output_file = main.output_file
  }
}
