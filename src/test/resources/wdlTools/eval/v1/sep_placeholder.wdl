version 1.0

task foo {
#  Array[Int] numbers = [1, 10, 100]
  Array[String] numbers = ["1", "10", "100"]

  command <<<
    We have lots of numbers here ~{sep=", " numbers}
  >>>
}
