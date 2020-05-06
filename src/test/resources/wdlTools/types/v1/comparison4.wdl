version 1.0

task comparison {
  input {
    Int i
    String s
  }

  Boolean ct_1 = i < s

  command {}
}
