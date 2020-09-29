# dx_string_no_file.wdl
version 1.0

task dynamic_docker_image {
  input {
    String docker_dxfile
  }

  command <<<
    echo "hello world" > cat.txt
  >>>

  runtime {
    docker: "${docker_dxfile}"
  }

  output {
    String cat = read_string("cat.txt")
  }
}