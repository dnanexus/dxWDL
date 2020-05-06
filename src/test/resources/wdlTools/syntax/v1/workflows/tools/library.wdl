version 1.0

task wc {
  input {
    File f
  }
  command {
    wc ~{f}
  }
  output {
    String result = read_string(stdout())
  }
}
