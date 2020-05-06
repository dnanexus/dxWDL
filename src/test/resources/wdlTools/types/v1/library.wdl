version 1.0

task add {
  input {
    Int a
    Int b
  }
  command {}
  output {
    Int result = a + b
  }
}

task concat {
  input {
    String a
    String b
  }
  command {}
  output {
    String result = a + b
  }
}

task gen_array {
  input {
    Int len
  }
  command {}
  output {
    Array[Int] result = range(len)
  }
}
