version 1.0

task bar {
  input {}

  command <<<
  echo "hello"
  >>>

  output {
    String s = "hello"
  }
}