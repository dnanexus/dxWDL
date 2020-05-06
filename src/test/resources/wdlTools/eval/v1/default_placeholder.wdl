version 1.0

task foo {
  String s = "hello"

  command <<<
    ~{default="nice" s}
  >>>
}
