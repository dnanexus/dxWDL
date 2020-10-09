version 1.0

task two_files {
  input {
    File file1
    File file2
  }

  command <<<
    echo "~{file1} ~{file2}"
  >>>

  output {
    String fileNames = read_string(stdout())
  }
}
