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

task B {
  command {
    echo "never say never"
  }
  output {
    File o = stdout()
  }
}

workflow linear {
  scatter (i in [1, 2, 3]) {
    call B
  }

  Array[Int] files = B.o
}
