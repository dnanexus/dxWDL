version 1.0

task bar {
  input {
    Int a
    Int b
  }
  command {}
}

workflow foo {

  call bar { input: a = 3, b = 5, }

}
