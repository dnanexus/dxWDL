version 1.0

workflow foo {
  Int a = select_first([3, round(100)])
}
