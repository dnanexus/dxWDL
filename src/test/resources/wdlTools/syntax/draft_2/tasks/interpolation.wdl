# check that string interpolation works
task foo {
  Int min_std_max_min
  String prefix

  command {
    echo ${sep=',' min_std_max_min}
  }
}
