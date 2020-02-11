version 1.0

task add2 {
  input {
    Int a
    Int b
  }
  command {
    echo $((${a} + ${b}))
  }
  output {
    Int c = read_int(stdout())
  }
}
