
version 1.0

import "secondary.wdl" as lib


task main {
  input {
    Array[File] input_file_array
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

  scatter (letter in ["a", "b", "c"]){
    # to generate a frag
    String with_prefix = "${input_string}_${letter}"

    call lib.secondary_wf as sub_wf {
        input: output_file_name = with_prefix
      }
  }

  call main {
    input: input_file_array=sub_wf.output_file
  }

  output {
    File output_file = main.output_file
  }
}
