version 1.0


workflow no_file_key {
  input {
     Map[File, String] file_key
  }

  call no_file_key_support_task {
    input:
      file_key=file_key
  }

  output {
    File file_key_map_output = no_file_key_support_task.file_key_map_output
  }

}

task no_file_key_support_task {
  input {
     Map[File, String] file_key
  }

  command <<<
  set -euxo pipefail
  x=~{write_map(file_key)}
  mv $x output.tsv
  >>>

  runtime {
    docker: "debian:stretch-slim"
  }

  output {
    File file_key_map_output = "output.tsv"
  }

}
