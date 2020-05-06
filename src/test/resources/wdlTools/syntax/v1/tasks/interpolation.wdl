# check that string interpolation works
version 1.0

task foo {
  input {
    Int min_std_max_min
    String prefix
  }
  command {
    echo ${sep=',' min_std_max_min}
  }
}
