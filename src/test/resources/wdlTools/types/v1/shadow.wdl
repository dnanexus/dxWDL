version 1.0

task A {
  input {
    Int x
  }
  command {
    echo "hello $x"
  }
  output {
    String s = read_string(stdout())
  }
}

workflow linear {
  call A { input: x = 3 }
  call A { input : x = 4 }
}
